// Package store holds the matchmaking state: sessions (rooms and lobbies) and their members in
// one Postgres database (`matchmaking`). The schema migrates at start. Every change to a session
// takes a row lock on it, so joins cannot overfill and revisions are strictly increasing.
// Nothing console-shaped is stored except the opaque blobs an adapter asks the core to keep.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	pb "github.com/openpak/matchmaking/proto/openpak/matchmaking/v1"
)

var (
	ErrNotFound  = errors.New("matchmaking: not found")
	ErrFull      = errors.New("matchmaking: session full")
	ErrClosed    = errors.New("matchmaking: session closed")
	ErrSecret    = errors.New("matchmaking: wrong join secret")
	ErrNotOwner  = errors.New("matchmaking: not the owner")
	ErrNotMember = errors.New("matchmaking: not a member")
	ErrExists    = errors.New("matchmaking: unique key in use")
	ErrRevision  = errors.New("matchmaking: revision mismatch")
	ErrInvalid   = errors.New("matchmaking: invalid argument")
	ErrWrongNet  = errors.New("matchmaking: session is limited to another network")
	ErrNotEnough = errors.New("matchmaking: max_members below current members")
)

const (
	DefaultMax     = 8
	MaxMembers     = 1024
	DefaultTTL     = 300
	MaxTTL         = 86400
	MaxAttrs       = 64
	MaxString      = 1024
	MaxSessionBlob = 8192
	MaxMemberBlob  = 4096
	MaxEndpoints   = 8
)

var nameRe = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)
var titleRe = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,64}$`)
var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type Store struct {
	DB *pgxpool.Pool
}

const schema = `
CREATE TABLE IF NOT EXISTS sessions (
    id             UUID PRIMARY KEY,
    title_key      TEXT NOT NULL,
    network        TEXT NOT NULL,
    kind           TEXT NOT NULL DEFAULT 'room' CHECK (kind IN ('room','lobby')),
    parent_id      UUID,
    unique_key     TEXT NOT NULL DEFAULT '',
    owner_id       UUID NOT NULL,
    max_members    INTEGER NOT NULL,
    is_open        BOOLEAN NOT NULL DEFAULT TRUE,
    hidden         BOOLEAN NOT NULL DEFAULT FALSE,
    network_only   BOOLEAN NOT NULL DEFAULT FALSE,
    migrate_owner  BOOLEAN NOT NULL DEFAULT FALSE,
    secret_hash    TEXT NOT NULL DEFAULT '',
    attrs          JSONB NOT NULL DEFAULT '{}',
    data           BYTEA NOT NULL DEFAULT ''::bytea,
    next_member_no INTEGER NOT NULL DEFAULT 1,
    ttl_seconds    INTEGER NOT NULL,
    revision       INTEGER NOT NULL DEFAULT 1,
    created        TIMESTAMPTZ NOT NULL,
    updated        TIMESTAMPTZ NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS sessions_unique_key ON sessions (title_key, kind, unique_key) WHERE unique_key <> '';
CREATE INDEX IF NOT EXISTS sessions_title ON sessions (title_key, kind, created DESC);
CREATE INDEX IF NOT EXISTS sessions_parent ON sessions (parent_id) WHERE parent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS sessions_owner ON sessions (owner_id);
CREATE TABLE IF NOT EXISTS members (
    session_id   UUID NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    account_id   UUID NOT NULL,
    member_no    INTEGER NOT NULL,
    display_name TEXT NOT NULL DEFAULT '',
    network      TEXT NOT NULL DEFAULT '',
    attrs        JSONB NOT NULL DEFAULT '{}',
    data         BYTEA NOT NULL DEFAULT ''::bytea,
    endpoints    JSONB NOT NULL DEFAULT '[]',
    joined       TIMESTAMPTZ NOT NULL,
    last_seen    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, account_id),
    UNIQUE (session_id, member_no)
);
CREATE INDEX IF NOT EXISTS members_account ON members (account_id);
CREATE INDEX IF NOT EXISTS members_seen ON members (last_seen);
`

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("matchmaking: schema: %w", err)
	}
	return &Store{DB: pool}, nil
}

func (s *Store) Close() { s.DB.Close() }

// Stamp is the stored time resolution: milliseconds, what the wire reports.
func Now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

// ---- attributes

type attrJSON struct {
	T int32  `json:"t"`
	I int64  `json:"i,omitempty"`
	S string `json:"s,omitempty"`
	B []byte `json:"b,omitempty"`
	X bool   `json:"x,omitempty"`
}

// ValidateAttrs checks names, types and sizes.
func ValidateAttrs(as []*pb.Attr) error {
	if len(as) > MaxAttrs {
		return fmt.Errorf("%w: more than %d attributes", ErrInvalid, MaxAttrs)
	}
	for _, a := range as {
		if !nameRe.MatchString(a.GetName()) {
			return fmt.Errorf("%w: attribute name", ErrInvalid)
		}
		switch a.GetType() {
		case pb.AttrType_ATTR_TYPE_INT:
		case pb.AttrType_ATTR_TYPE_STRING:
			if len(a.GetStringValue()) > MaxString {
				return fmt.Errorf("%w: string attribute too long", ErrInvalid)
			}
		case pb.AttrType_ATTR_TYPE_BYTES:
			if len(a.GetBytesValue()) > MaxString {
				return fmt.Errorf("%w: bytes attribute too long", ErrInvalid)
			}
		default:
			return fmt.Errorf("%w: attribute type", ErrInvalid)
		}
	}
	return nil
}

func attrsToMap(as []*pb.Attr) map[string]attrJSON {
	m := make(map[string]attrJSON, len(as))
	for _, a := range as {
		m[a.GetName()] = attrJSON{T: int32(a.GetType()), I: a.GetIntValue(), S: a.GetStringValue(), B: a.GetBytesValue(), X: a.GetSearchable()}
	}
	return m
}

func mergeAttrs(cur map[string]attrJSON, set []*pb.Attr, del []string) (map[string]attrJSON, error) {
	if cur == nil {
		cur = map[string]attrJSON{}
	}
	for k, v := range attrsToMap(set) {
		cur[k] = v
	}
	for _, d := range del {
		delete(cur, d)
	}
	if len(cur) > MaxAttrs {
		return nil, fmt.Errorf("%w: more than %d attributes", ErrInvalid, MaxAttrs)
	}
	return cur, nil
}

func mapToAttrs(m map[string]attrJSON, onlySearchable bool) []*pb.Attr {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]*pb.Attr, 0, len(names))
	for _, k := range names {
		a := m[k]
		if onlySearchable && !a.X {
			continue
		}
		out = append(out, &pb.Attr{Name: k, Type: pb.AttrType(a.T), IntValue: a.I, StringValue: a.S, BytesValue: a.B, Searchable: a.X})
	}
	return out
}

// ---- helpers

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

func secretHash(id, secret string) string {
	if secret == "" {
		return ""
	}
	h := sha256.Sum256([]byte(id + "\x00" + secret))
	return hex.EncodeToString(h[:])
}

func ValidAccount(a string) bool { return uuidRe.MatchString(a) }

func lower(s string) string { return strings.ToLower(s) }

type srow struct {
	id, title, network, kind, unique, owner, secret string
	parent                                          *string
	max, nextNo, ttl, rev                           int
	open, hidden, netOnly, migrate                  bool
	attrs                                           map[string]attrJSON
	data                                            []byte
	created, updated                                time.Time
}

const sessionCols = `id::text, title_key, network, kind, parent_id::text, unique_key, owner_id::text, max_members, is_open, hidden, network_only, migrate_owner, secret_hash, attrs, data, next_member_no, ttl_seconds, revision, created, updated`

func scanSession(r pgx.Row) (*srow, error) {
	var x srow
	err := r.Scan(&x.id, &x.title, &x.network, &x.kind, &x.parent, &x.unique, &x.owner, &x.max, &x.open, &x.hidden, &x.netOnly, &x.migrate, &x.secret, &x.attrs, &x.data, &x.nextNo, &x.ttl, &x.rev, &x.created, &x.updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return &x, err
}

type tx = pgx.Tx

func lockSession(ctx context.Context, t tx, id string) (*srow, error) {
	if !uuidRe.MatchString(id) {
		return nil, ErrNotFound
	}
	return scanSession(t.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions WHERE id=$1 FOR UPDATE`, id))
}

func loadMembers(ctx context.Context, q interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, id string) ([]*pb.Member, error) {
	rows, err := q.Query(ctx, `SELECT account_id::text, member_no, display_name, network, attrs, data, endpoints, joined FROM members WHERE session_id=$1 ORDER BY member_no`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.Member
	for rows.Next() {
		var m pb.Member
		var attrs map[string]attrJSON
		var eps []*endpointJSON
		var joined time.Time
		if err := rows.Scan(&m.AccountId, &m.MemberNo, &m.DisplayName, &m.Network, &attrs, &m.Data, &eps, &joined); err != nil {
			return nil, err
		}
		m.Attrs = mapToAttrs(attrs, false)
		for _, e := range eps {
			m.Endpoints = append(m.Endpoints, &pb.Endpoint{Kind: e.K, Host: e.H, Port: e.P, NatType: e.N})
		}
		m.JoinedUnixMs = joined.UnixMilli()
		out = append(out, &m)
	}
	return out, rows.Err()
}

type endpointJSON struct {
	K string `json:"k"`
	H string `json:"h"`
	P uint32 `json:"p"`
	N uint32 `json:"n"`
}

func endpointsJSON(es []*pb.Endpoint) ([]endpointJSON, error) {
	if len(es) > MaxEndpoints {
		return nil, fmt.Errorf("%w: more than %d endpoints", ErrInvalid, MaxEndpoints)
	}
	out := make([]endpointJSON, 0, len(es))
	for _, e := range es {
		if len(e.GetHost()) > 255 || len(e.GetKind()) > 16 || e.GetPort() > 65535 {
			return nil, fmt.Errorf("%w: endpoint", ErrInvalid)
		}
		out = append(out, endpointJSON{e.GetKind(), e.GetHost(), e.GetPort(), e.GetNatType()})
	}
	return out, nil
}

func (x *srow) pb(members []*pb.Member, viewer string) *pb.Session {
	out := &pb.Session{
		Id: x.id, Title: &pb.TitleRef{Network: x.network, Title: x.title}, Kind: x.kind, UniqueKey: x.unique,
		OwnerId: x.owner, MaxMembers: uint32(x.max), Open: x.open, Hidden: x.hidden, NetworkOnly: x.netOnly,
		MigrateOwner: x.migrate, HasSecret: x.secret != "", Data: x.data,
		CreatedUnixMs: x.created.UnixMilli(), UpdatedUnixMs: x.updated.UnixMilli(), TtlSeconds: uint32(x.ttl), Revision: uint32(x.rev),
	}
	if x.parent != nil {
		out.ParentId = *x.parent
	}
	isMember := false
	for _, m := range members {
		if m.AccountId == viewer {
			isMember = true
		}
		m.IsOwner = m.AccountId == x.owner
	}
	// A viewer of "*" (internal fan-out) and members see everything; others only searchable attributes.
	if viewer == "*" || isMember {
		out.Attrs = mapToAttrs(x.attrs, false)
		out.Members = members
	} else {
		out.Attrs = mapToAttrs(x.attrs, true)
		out.Data = nil
		for _, m := range members {
			out.Members = append(out.Members, &pb.Member{MemberNo: m.MemberNo, AccountId: m.AccountId, DisplayName: m.DisplayName, IsOwner: m.IsOwner, JoinedUnixMs: m.JoinedUnixMs, Network: m.Network})
		}
	}
	return out
}

func (s *Store) view(ctx context.Context, t tx, x *srow, viewer string) (*pb.Session, error) {
	ms, err := loadMembers(ctx, t, x.id)
	if err != nil {
		return nil, err
	}
	return x.pb(ms, viewer), nil
}

func isUnique(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && pe.Code == "23505"
}

func (s *Store) inTx(ctx context.Context, f func(t tx) error) error {
	t, err := s.DB.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer t.Rollback(ctx)
	if err := f(t); err != nil {
		return err
	}
	return t.Commit(ctx)
}

// ---- operations

// Create makes a session and its first member. A unique_key already in use is ErrExists.
func (s *Store) Create(ctx context.Context, r *pb.CreateSessionRequest) (*pb.Session, error) {
	title, kind := r.GetTitle().GetTitle(), r.GetKind()
	if kind == "" {
		kind = "room"
	}
	if !titleRe.MatchString(title) || (kind != "room" && kind != "lobby") || !ValidAccount(r.GetCreator()) {
		return nil, ErrInvalid
	}
	if len(r.GetUniqueKey()) > 128 || len(r.GetData()) > MaxSessionBlob || len(r.GetMemberData()) > MaxMemberBlob || len(r.GetDisplayName()) > 64 {
		return nil, fmt.Errorf("%w: field too large", ErrInvalid)
	}
	if err := ValidateAttrs(r.GetAttrs()); err != nil {
		return nil, err
	}
	if err := ValidateAttrs(r.GetMemberAttrs()); err != nil {
		return nil, err
	}
	max := int(r.GetMaxMembers())
	if max == 0 {
		max = DefaultMax
	}
	if max < 1 || max > MaxMembers {
		return nil, fmt.Errorf("%w: max_members", ErrInvalid)
	}
	ttl := int(r.GetTtlSeconds())
	if ttl == 0 {
		ttl = DefaultTTL
	}
	if ttl < 10 || ttl > MaxTTL {
		return nil, fmt.Errorf("%w: ttl_seconds", ErrInvalid)
	}
	eps, err := endpointsJSON(r.GetEndpoints())
	if err != nil {
		return nil, err
	}
	network := r.GetNetwork()
	if network == "" {
		network = r.GetTitle().GetNetwork()
	}
	id := newID()
	creator := lower(r.GetCreator())
	var parent *string
	if p := r.GetParentId(); p != "" {
		if !uuidRe.MatchString(p) {
			return nil, ErrInvalid
		}
		parent = &p
	}
	now := Now()
	var out *pb.Session
	err = s.inTx(ctx, func(t tx) error {
		_, err := t.Exec(ctx, `INSERT INTO sessions (id,title_key,network,kind,parent_id,unique_key,owner_id,max_members,is_open,hidden,network_only,migrate_owner,secret_hash,attrs,data,next_member_no,ttl_seconds,revision,created,updated)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,2,$16,1,$17,$17)`,
			id, title, r.GetTitle().GetNetwork(), kind, parent, r.GetUniqueKey(), creator, max, !r.GetClosed(), r.GetHidden(), r.GetNetworkOnly(), r.GetMigrateOwner(),
			secretHash(id, r.GetSecret()), attrsToMap(r.GetAttrs()), nb(r.GetData()), ttl, now)
		if isUnique(err) {
			return ErrExists
		}
		if err != nil {
			return err
		}
		if _, err := t.Exec(ctx, `INSERT INTO members (session_id,account_id,member_no,display_name,network,attrs,data,endpoints,joined,last_seen) VALUES ($1,$2,1,$3,$4,$5,$6,$7,$8,$8)`,
			id, creator, r.GetDisplayName(), network, attrsToMap(r.GetMemberAttrs()), nb(r.GetMemberData()), eps, now); err != nil {
			return err
		}
		x, err := scanSession(t.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions WHERE id=$1`, id))
		if err != nil {
			return err
		}
		out, err = s.view(ctx, t, x, "*")
		return err
	})
	return out, err
}

// Join adds a member. A member joining twice gets its existing record back (client retries are safe).
func (s *Store) Join(ctx context.Context, r *pb.JoinSessionRequest) (*pb.Session, uint32, error) {
	if !ValidAccount(r.GetAccountId()) || len(r.GetMemberData()) > MaxMemberBlob || len(r.GetDisplayName()) > 64 {
		return nil, 0, ErrInvalid
	}
	if err := ValidateAttrs(r.GetMemberAttrs()); err != nil {
		return nil, 0, err
	}
	eps, err := endpointsJSON(r.GetEndpoints())
	if err != nil {
		return nil, 0, err
	}
	acct := lower(r.GetAccountId())
	var out *pb.Session
	var no uint32
	err = s.inTx(ctx, func(t tx) error {
		x, err := lockSession(ctx, t, r.GetSessionId())
		if err != nil {
			return err
		}
		var cnt int
		var existing *int
		if err := t.QueryRow(ctx, `SELECT count(*), max(member_no) FILTER (WHERE account_id=$2) FROM members WHERE session_id=$1`, x.id, acct).Scan(&cnt, &existing); err != nil {
			return err
		}
		if existing != nil {
			no = uint32(*existing)
			out, err = s.view(ctx, t, x, "*")
			return err
		}
		if x.netOnly && r.GetNetwork() != "" && r.GetNetwork() != x.network {
			return ErrWrongNet
		}
		if !x.open && !r.GetAllowClosed() {
			return ErrClosed
		}
		if x.secret != "" && secretHash(x.id, r.GetSecret()) != x.secret {
			return ErrSecret
		}
		if cnt >= x.max {
			return ErrFull
		}
		now := Now()
		no = uint32(x.nextNo)
		net := r.GetNetwork()
		if net == "" {
			net = x.network
		}
		if _, err := t.Exec(ctx, `INSERT INTO members (session_id,account_id,member_no,display_name,network,attrs,data,endpoints,joined,last_seen) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$9)`,
			x.id, acct, x.nextNo, r.GetDisplayName(), net, attrsToMap(r.GetMemberAttrs()), nb(r.GetMemberData()), eps, now); err != nil {
			return err
		}
		if _, err := t.Exec(ctx, `UPDATE sessions SET next_member_no=next_member_no+1, revision=revision+1, updated=$2 WHERE id=$1`, x.id, now); err != nil {
			return err
		}
		x.nextNo++
		x.rev++
		x.updated = now
		out, err = s.view(ctx, t, x, "*")
		return err
	})
	return out, no, err
}

// FindByKey returns the id of the open session with this unique key, or "".
func (s *Store) FindByKey(ctx context.Context, title, kind, key string) (string, error) {
	if kind == "" {
		kind = "room"
	}
	var id string
	err := s.DB.QueryRow(ctx, `SELECT id::text FROM sessions WHERE title_key=$1 AND kind=$2 AND unique_key=$3`, title, kind, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// LeaveResult says what a Leave did, and the session as it stands afterwards (nil when it ended).
type LeaveResult struct {
	Account  string
	Left     bool
	Ended    bool
	NewOwner string
	Session  *pb.Session // remaining members: the fan-out set
	MemberNo uint32
	// Final holds the members that were in the session before it ended, for the ended event.
	Final []string
}

func (s *Store) Leave(ctx context.Context, sid, account string) (*LeaveResult, error) {
	res := &LeaveResult{}
	acct := lower(account)
	err := s.inTx(ctx, func(t tx) error {
		return s.leaveTx(ctx, t, sid, acct, res)
	})
	return res, err
}

func (s *Store) leaveTx(ctx context.Context, t tx, sid, acct string, res *LeaveResult) error {
	x, err := lockSession(ctx, t, sid)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	ms, err := loadMembers(ctx, t, x.id)
	if err != nil {
		return err
	}
	var gone *pb.Member
	var rest []*pb.Member
	for _, m := range ms {
		if m.AccountId == acct {
			gone = m
		} else {
			rest = append(rest, m)
		}
	}
	if gone == nil {
		return nil
	}
	res.Left, res.MemberNo, res.Account = true, gone.MemberNo, acct
	for _, m := range ms {
		res.Final = append(res.Final, m.AccountId)
	}
	now := Now()
	if _, err := t.Exec(ctx, `DELETE FROM members WHERE session_id=$1 AND account_id=$2`, x.id, acct); err != nil {
		return err
	}
	if len(rest) == 0 || (acct == x.owner && !x.migrate) {
		// A room ends with its owner unless it migrates; a lobby ends only when empty.
		if len(rest) == 0 || x.kind == "room" {
			if _, err := t.Exec(ctx, `DELETE FROM sessions WHERE id=$1`, x.id); err != nil {
				return err
			}
			res.Ended = true
			return nil
		}
	}
	if acct == x.owner {
		x.owner = rest[0].AccountId
		res.NewOwner = x.owner
	}
	x.rev++
	x.updated = now
	if _, err := t.Exec(ctx, `UPDATE sessions SET owner_id=$2, revision=$3, updated=$4 WHERE id=$1`, x.id, x.owner, x.rev, now); err != nil {
		return err
	}
	res.Session = x.pb(rest, "*")
	return nil
}

func (s *Store) Get(ctx context.Context, sid, viewer string) (*pb.Session, error) {
	if !uuidRe.MatchString(sid) {
		return nil, ErrNotFound
	}
	x, err := scanSession(s.DB.QueryRow(ctx, `SELECT `+sessionCols+` FROM sessions WHERE id=$1`, sid))
	if err != nil {
		return nil, err
	}
	ms, err := loadMembers(ctx, s.DB, x.id)
	if err != nil {
		return nil, err
	}
	return x.pb(ms, lower(viewer)), nil
}

// UpdateSession applies owner changes under the row lock.
func (s *Store) UpdateSession(ctx context.Context, r *pb.UpdateSessionRequest) (*pb.Session, error) {
	if err := ValidateAttrs(r.GetSetAttrs()); err != nil {
		return nil, err
	}
	if len(r.GetData()) > MaxSessionBlob {
		return nil, fmt.Errorf("%w: data too large", ErrInvalid)
	}
	var out *pb.Session
	err := s.inTx(ctx, func(t tx) error {
		x, err := lockSession(ctx, t, r.GetSessionId())
		if err != nil {
			return err
		}
		if x.owner != lower(r.GetActor()) {
			return ErrNotOwner
		}
		if r.GetExpectRevision() != 0 && int(r.GetExpectRevision()) != x.rev {
			return ErrRevision
		}
		if x.attrs, err = mergeAttrs(x.attrs, r.GetSetAttrs(), r.GetDeleteAttrs()); err != nil {
			return err
		}
		if r.GetSetData() {
			x.data = r.GetData()
		}
		if r.GetSetOpen() {
			x.open = r.GetOpen()
		}
		if r.GetSetHidden() {
			x.hidden = r.GetHidden()
		}
		if r.GetSetMaxMembers() {
			m := int(r.GetMaxMembers())
			if m < 1 || m > MaxMembers {
				return fmt.Errorf("%w: max_members", ErrInvalid)
			}
			var cnt int
			if err := t.QueryRow(ctx, `SELECT count(*) FROM members WHERE session_id=$1`, x.id).Scan(&cnt); err != nil {
				return err
			}
			if m < cnt {
				return ErrNotEnough
			}
			x.max = m
		}
		x.rev++
		x.updated = Now()
		if _, err := t.Exec(ctx, `UPDATE sessions SET attrs=$2,data=$3,is_open=$4,hidden=$5,max_members=$6,revision=$7,updated=$8 WHERE id=$1`,
			x.id, x.attrs, nb(x.data), x.open, x.hidden, x.max, x.rev, x.updated); err != nil {
			return err
		}
		out, err = s.view(ctx, t, x, "*")
		return err
	})
	return out, err
}

// UpdateMember changes a member record; the member itself or the owner may.
func (s *Store) UpdateMember(ctx context.Context, r *pb.UpdateMemberRequest) (*pb.Session, error) {
	if err := ValidateAttrs(r.GetSetAttrs()); err != nil {
		return nil, err
	}
	if len(r.GetData()) > MaxMemberBlob || len(r.GetDisplayName()) > 64 {
		return nil, fmt.Errorf("%w: field too large", ErrInvalid)
	}
	acct, actor := lower(r.GetAccountId()), lower(r.GetActor())
	var out *pb.Session
	err := s.inTx(ctx, func(t tx) error {
		x, err := lockSession(ctx, t, r.GetSessionId())
		if err != nil {
			return err
		}
		if actor != acct && actor != x.owner {
			return ErrNotOwner
		}
		var attrs map[string]attrJSON
		var data []byte
		var name string
		err = t.QueryRow(ctx, `SELECT attrs, data, display_name FROM members WHERE session_id=$1 AND account_id=$2`, x.id, acct).Scan(&attrs, &data, &name)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotMember
		}
		if err != nil {
			return err
		}
		if attrs, err = mergeAttrs(attrs, r.GetSetAttrs(), r.GetDeleteAttrs()); err != nil {
			return err
		}
		if r.GetSetData() {
			data = r.GetData()
		}
		if r.GetDisplayName() != "" {
			name = r.GetDisplayName()
		}
		now := Now()
		if _, err := t.Exec(ctx, `UPDATE members SET attrs=$3,data=$4,display_name=$5,last_seen=$6 WHERE session_id=$1 AND account_id=$2`, x.id, acct, attrs, nb(data), name, now); err != nil {
			return err
		}
		x.rev++
		x.updated = now
		if _, err := t.Exec(ctx, `UPDATE sessions SET revision=$2, updated=$3 WHERE id=$1`, x.id, x.rev, now); err != nil {
			return err
		}
		out, err = s.view(ctx, t, x, "*")
		return err
	})
	return out, err
}

func (s *Store) SetEndpoints(ctx context.Context, sid, account string, es []*pb.Endpoint) (*pb.Session, error) {
	eps, err := endpointsJSON(es)
	if err != nil {
		return nil, err
	}
	acct := lower(account)
	var out *pb.Session
	err = s.inTx(ctx, func(t tx) error {
		x, err := lockSession(ctx, t, sid)
		if err != nil {
			return err
		}
		now := Now()
		tag, err := t.Exec(ctx, `UPDATE members SET endpoints=$3, last_seen=$4 WHERE session_id=$1 AND account_id=$2`, x.id, acct, eps, now)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrNotMember
		}
		x.rev++
		x.updated = now
		if _, err := t.Exec(ctx, `UPDATE sessions SET revision=$2, updated=$3 WHERE id=$1`, x.id, x.rev, now); err != nil {
			return err
		}
		out, err = s.view(ctx, t, x, "*")
		return err
	})
	return out, err
}

func (s *Store) SetOwner(ctx context.Context, sid, actor, newOwner string) (*pb.Session, error) {
	actor, newOwner = lower(actor), lower(newOwner)
	var out *pb.Session
	err := s.inTx(ctx, func(t tx) error {
		x, err := lockSession(ctx, t, sid)
		if err != nil {
			return err
		}
		if x.owner != actor {
			return ErrNotOwner
		}
		var one int
		if err := t.QueryRow(ctx, `SELECT 1 FROM members WHERE session_id=$1 AND account_id=$2`, x.id, newOwner).Scan(&one); errors.Is(err, pgx.ErrNoRows) {
			return ErrNotMember
		} else if err != nil {
			return err
		}
		x.owner = newOwner
		x.rev++
		x.updated = Now()
		if _, err := t.Exec(ctx, `UPDATE sessions SET owner_id=$2, revision=$3, updated=$4 WHERE id=$1`, x.id, x.owner, x.rev, x.updated); err != nil {
			return err
		}
		out, err = s.view(ctx, t, x, "*")
		return err
	})
	return out, err
}

// Heartbeat marks every membership of the account alive and returns how many it has.
func (s *Store) Heartbeat(ctx context.Context, account string) (int, error) {
	tag, err := s.DB.Exec(ctx, `UPDATE members SET last_seen=$2 WHERE account_id=$1`, lower(account), Now())
	return int(tag.RowsAffected()), err
}

// Reap removes members not seen for their session's ttl (the owner migrates or the room ends)
// and returns the leave results so the caller can announce them.
func (s *Store) Reap(ctx context.Context) ([]*LeaveResult, []string, error) {
	rows, err := s.DB.Query(ctx, `SELECT m.session_id::text, m.account_id::text FROM members m JOIN sessions s ON s.id=m.session_id WHERE m.last_seen < now() - make_interval(secs => s.ttl_seconds)`)
	if err != nil {
		return nil, nil, err
	}
	type pair struct{ sid, acct string }
	var stale []pair
	for rows.Next() {
		var p pair
		if err := rows.Scan(&p.sid, &p.acct); err != nil {
			rows.Close()
			return nil, nil, err
		}
		stale = append(stale, p)
	}
	rows.Close()
	var out []*LeaveResult
	var sids []string
	for _, p := range stale {
		res, err := s.Leave(ctx, p.sid, p.acct)
		if err != nil {
			return out, sids, err
		}
		if res.Left {
			out = append(out, res)
			sids = append(sids, p.sid)
		}
	}
	return out, sids, nil
}

// MemberSessions lists the session ids the account is in.
func (s *Store) MemberSessions(ctx context.Context, account string) ([]string, error) {
	rows, err := s.DB.Query(ctx, `SELECT session_id::text FROM members WHERE account_id=$1`, lower(account))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// Erase removes the account from everything: it leaves every session it is in (so owners
// migrate or rooms end). It returns the leave results for announcing and the count.
func (s *Store) Erase(ctx context.Context, account string) ([]*LeaveResult, []string, error) {
	sids, err := s.MemberSessions(ctx, account)
	if err != nil {
		return nil, nil, err
	}
	var out []*LeaveResult
	var done []string
	for _, sid := range sids {
		res, err := s.Leave(ctx, sid, account)
		if err != nil {
			return out, done, err
		}
		if res.Left {
			out = append(out, res)
			done = append(done, sid)
		}
	}
	// Sessions that survive with the erased account as owner of a lobby were migrated by Leave;
	// nothing keyed by the account remains.
	return out, done, nil
}

// Search lists sessions matching the request. Members are omitted; $members is the count.
func (s *Store) Search(ctx context.Context, r *pb.SearchSessionsRequest) ([]*pb.Session, int, error) {
	title := r.GetTitle().GetTitle()
	if !titleRe.MatchString(title) {
		return nil, 0, ErrInvalid
	}
	kind := r.GetKind()
	if kind == "" {
		kind = "room"
	}
	args := []any{title, kind}
	arg := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	where := []string{"s.title_key=$1", "s.kind=$2", "NOT s.hidden"}
	if !r.GetIncludeClosed() {
		where = append(where, "s.is_open")
	}
	// Network visibility: a network_only session is seen only from its own network.
	net := r.GetNetwork()
	if net == "" {
		net = r.GetTitle().GetNetwork()
	}
	where = append(where, "(NOT s.network_only OR s.network="+arg(net)+")")
	if ns := r.GetNetworks(); len(ns) > 0 {
		where = append(where, "s.network = ANY("+arg(ns)+")")
	}
	if p := r.GetParentId(); p != "" {
		if !uuidRe.MatchString(p) {
			return nil, 0, ErrInvalid
		}
		where = append(where, "s.parent_id="+arg(p)+"::uuid")
	}
	if os := r.GetOwners(); len(os) > 0 {
		for i := range os {
			if !uuidRe.MatchString(os[i]) {
				return nil, 0, ErrInvalid
			}
			os[i] = lower(os[i])
		}
		where = append(where, "s.owner_id::text = ANY("+arg(os)+")")
	}
	if r.GetOnlyWithFreeSlots() {
		where = append(where, "(SELECT count(*) FROM members m WHERE m.session_id=s.id) < s.max_members")
	}
	ops := map[string]string{"eq": "=", "ne": "<>", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}
	if len(r.GetFilters()) > 16 {
		return nil, 0, fmt.Errorf("%w: too many filters", ErrInvalid)
	}
	for _, f := range r.GetFilters() {
		op := f.GetOp()
		name := f.GetAttr()
		var lhs string
		isStr := f.GetIsString()
		switch name {
		case "$members":
			lhs, isStr = "(SELECT count(*) FROM members m WHERE m.session_id=s.id)", false
		case "$max_members":
			lhs, isStr = "s.max_members", false
		case "$created":
			lhs, isStr = "(extract(epoch from s.created)*1000)::bigint", false
		case "$kind":
			lhs, isStr = "s.kind", true
		case "$unique_key":
			lhs, isStr = "s.unique_key", true
		case "$owner":
			lhs, isStr = "s.owner_id::text", true
		default:
			if !nameRe.MatchString(name) {
				return nil, 0, fmt.Errorf("%w: filter attribute", ErrInvalid)
			}
			where = append(where, "(s.attrs->"+arg(name)+"->>'x')='true'")
			if isStr {
				lhs = "(s.attrs->" + arg(name) + "->>'s')"
			} else {
				lhs = "((s.attrs->" + arg(name) + "->>'i')::bigint)"
			}
		}
		if op == "prefix" {
			if !isStr {
				return nil, 0, fmt.Errorf("%w: prefix needs a string", ErrInvalid)
			}
			where = append(where, lhs+" LIKE "+arg(escapeLike(f.GetStringValue())+"%"))
			continue
		}
		sop, ok := ops[op]
		if !ok {
			return nil, 0, fmt.Errorf("%w: filter op", ErrInvalid)
		}
		if isStr {
			where = append(where, lhs+" "+sop+" "+arg(f.GetStringValue()))
		} else {
			where = append(where, lhs+" "+sop+" "+arg(f.GetIntValue()))
		}
	}
	order := "s.created"
	if sa := r.GetSortAttr(); sa != "" && sa != "$created" {
		switch sa {
		case "$members":
			order = "(SELECT count(*) FROM members m WHERE m.session_id=s.id)"
		case "$max_members":
			order = "s.max_members"
		default:
			if !nameRe.MatchString(sa) {
				return nil, 0, fmt.Errorf("%w: sort attribute", ErrInvalid)
			}
			order = "((s.attrs->" + arg(sa) + "->>'i')::bigint)"
		}
	}
	dir := "DESC"
	if r.GetAscending() {
		dir = "ASC"
	}
	limit := int(r.GetLimit())
	if limit == 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	cond := strings.Join(where, " AND ")
	var hits int
	if err := s.DB.QueryRow(ctx, `SELECT count(*) FROM sessions s WHERE `+cond, args...).Scan(&hits); err != nil {
		return nil, 0, err
	}
	q := `SELECT ` + prefixCols("s.") + `, (SELECT count(*) FROM members m WHERE m.session_id=s.id) FROM sessions s WHERE ` + cond +
		` ORDER BY ` + order + ` ` + dir + `, s.id LIMIT ` + arg(limit) + ` OFFSET ` + arg(int(r.GetOffset()))
	rows, err := s.DB.Query(ctx, q, args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*pb.Session
	for rows.Next() {
		var x srow
		var cnt int
		if err := rows.Scan(&x.id, &x.title, &x.network, &x.kind, &x.parent, &x.unique, &x.owner, &x.max, &x.open, &x.hidden, &x.netOnly, &x.migrate, &x.secret, &x.attrs, &x.data, &x.nextNo, &x.ttl, &x.rev, &x.created, &x.updated, &cnt); err != nil {
			return nil, 0, err
		}
		ss := x.pb(nil, lower(r.GetViewer()))
		ss.Data = nil
		ss.Attrs = mapToAttrs(x.attrs, true)
		ss.Attrs = append(ss.Attrs, &pb.Attr{Name: "$members", Type: pb.AttrType_ATTR_TYPE_INT, IntValue: int64(cnt), Searchable: true})
		out = append(out, ss)
	}
	return out, hits, rows.Err()
}

func prefixCols(p string) string {
	cols := strings.Split(sessionCols, ", ")
	for i, c := range cols {
		switch c {
		case "id::text":
			cols[i] = p + "id::text"
		case "parent_id::text":
			cols[i] = p + "parent_id::text"
		case "owner_id::text":
			cols[i] = p + "owner_id::text"
		default:
			cols[i] = p + c
		}
	}
	return strings.Join(cols, ", ")
}

func escapeLike(s string) string {
	return strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(s)
}

// JSON is exported for tests that need to read raw attrs.
func JSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func nb(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}
