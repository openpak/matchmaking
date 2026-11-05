// Package service turns store results into events: every successful change is announced to the
// affected members through the hub after the transaction committed.
package service

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/openpak/matchmaking/internal/hub"
	"github.com/openpak/matchmaking/internal/store"
	pb "github.com/openpak/matchmaking/proto/openpak/matchmaking/v1"
)

type Service struct {
	Store *store.Store
	Hub   *hub.Hub
}

func now() int64 { return time.Now().UnixMilli() }

func members(s *pb.Session) []string {
	out := make([]string, 0, len(s.GetMembers()))
	for _, m := range s.GetMembers() {
		out = append(out, m.GetAccountId())
	}
	return out
}

func (s *Service) emit(t pb.EventType, sess *pb.Session, subject string, no uint32, text string, to []string) {
	s.Hub.Publish(&pb.Event{Type: t, SessionId: sess.GetId(), AccountId: subject, MemberNo: no, Text: text, Revision: sess.GetRevision(), AtUnixMs: now()}, to)
}

func (s *Service) Create(ctx context.Context, r *pb.CreateSessionRequest) (*pb.SessionResponse, error) {
	sess, err := s.Store.Create(ctx, r)
	if err != nil {
		return nil, err
	}
	s.emit(pb.EventType_EVENT_TYPE_SESSION_CREATED, sess, r.GetCreator(), 1, "", members(sess))
	return &pb.SessionResponse{Session: sess, MemberNo: 1, Created: true}, nil
}

// CreateOrJoin finds an existing session by unique_key and joins it, else creates it; a lost
// race on the unique key becomes a join.
func (s *Service) CreateOrJoin(ctx context.Context, r *pb.CreateSessionRequest) (*pb.SessionResponse, error) {
	if r.GetUniqueKey() == "" {
		return s.Create(ctx, r)
	}
	for i := 0; i < 3; i++ {
		id, err := s.Store.FindByKey(ctx, r.GetTitle().GetTitle(), r.GetKind(), r.GetUniqueKey())
		if err != nil {
			return nil, err
		}
		if id == "" {
			res, err := s.Create(ctx, r)
			if err == store.ErrExists {
				continue
			}
			return res, err
		}
		return s.Join(ctx, &pb.JoinSessionRequest{SessionId: id, AccountId: r.GetCreator(), DisplayName: r.GetDisplayName(), Network: r.GetNetwork(),
			Secret: r.GetSecret(), MemberData: r.GetMemberData(), MemberAttrs: r.GetMemberAttrs(), Endpoints: r.GetEndpoints()})
	}
	return nil, store.ErrExists
}

func (s *Service) Join(ctx context.Context, r *pb.JoinSessionRequest) (*pb.SessionResponse, error) {
	sess, no, err := s.Store.Join(ctx, r)
	if err != nil {
		return nil, err
	}
	// A repeat join returns the old record; announce only a real change (revision moved).
	s.emit(pb.EventType_EVENT_TYPE_MEMBER_JOINED, sess, strings.ToLower(r.GetAccountId()), no, "", members(sess))
	return &pb.SessionResponse{Session: sess, MemberNo: no}, nil
}

func (s *Service) announceLeave(res *store.LeaveResult, sid, account, reason string) {
	if !res.Left {
		return
	}
	if res.Ended {
		ev := &pb.Event{Type: pb.EventType_EVENT_TYPE_SESSION_ENDED, SessionId: sid, AccountId: account, MemberNo: res.MemberNo, Text: reason, AtUnixMs: now()}
		s.Hub.Publish(ev, res.Final)
		return
	}
	sess := res.Session
	to := append(members(sess), account)
	s.emit(pb.EventType_EVENT_TYPE_MEMBER_LEFT, sess, account, res.MemberNo, reason, to)
	if res.NewOwner != "" {
		s.emit(pb.EventType_EVENT_TYPE_OWNER_CHANGED, sess, res.NewOwner, 0, "", members(sess))
	}
}

func (s *Service) Leave(ctx context.Context, sid, account, reason string) (*pb.LeaveResponse, error) {
	if reason == "" {
		reason = "leave"
	}
	if len(reason) > 64 {
		reason = reason[:64]
	}
	res, err := s.Store.Leave(ctx, sid, account)
	if err != nil {
		return nil, err
	}
	s.announceLeave(res, sid, strings.ToLower(account), reason)
	return &pb.LeaveResponse{Left: res.Left, SessionEnded: res.Ended, NewOwnerId: res.NewOwner}, nil
}

func (s *Service) Kick(ctx context.Context, sid, account, actor string) (*pb.SessionResponse, error) {
	sess, err := s.Store.Get(ctx, sid, "*")
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(sess.GetOwnerId(), actor) {
		return nil, store.ErrNotOwner
	}
	if strings.EqualFold(account, actor) {
		return nil, store.ErrInvalid
	}
	if _, err := s.Leave(ctx, sid, account, "kicked"); err != nil {
		return nil, err
	}
	sess, err = s.Store.Get(ctx, sid, "*")
	return &pb.SessionResponse{Session: sess}, err
}

func (s *Service) UpdateSession(ctx context.Context, r *pb.UpdateSessionRequest) (*pb.SessionResponse, error) {
	sess, err := s.Store.UpdateSession(ctx, r)
	if err != nil {
		return nil, err
	}
	s.emit(pb.EventType_EVENT_TYPE_SESSION_UPDATED, sess, strings.ToLower(r.GetActor()), 0, "", members(sess))
	return &pb.SessionResponse{Session: sess}, nil
}

func (s *Service) UpdateMember(ctx context.Context, r *pb.UpdateMemberRequest) (*pb.SessionResponse, error) {
	sess, err := s.Store.UpdateMember(ctx, r)
	if err != nil {
		return nil, err
	}
	s.emit(pb.EventType_EVENT_TYPE_MEMBER_UPDATED, sess, strings.ToLower(r.GetAccountId()), 0, "", members(sess))
	return &pb.SessionResponse{Session: sess}, nil
}

func (s *Service) SetEndpoints(ctx context.Context, r *pb.SetEndpointsRequest) (*pb.SessionResponse, error) {
	sess, err := s.Store.SetEndpoints(ctx, r.GetSessionId(), r.GetAccountId(), r.GetEndpoints())
	if err != nil {
		return nil, err
	}
	s.emit(pb.EventType_EVENT_TYPE_MEMBER_UPDATED, sess, strings.ToLower(r.GetAccountId()), 0, "endpoints", members(sess))
	return &pb.SessionResponse{Session: sess}, nil
}

func (s *Service) SetOwner(ctx context.Context, r *pb.SetOwnerRequest) (*pb.SessionResponse, error) {
	sess, err := s.Store.SetOwner(ctx, r.GetSessionId(), r.GetActor(), r.GetAccountId())
	if err != nil {
		return nil, err
	}
	s.emit(pb.EventType_EVENT_TYPE_OWNER_CHANGED, sess, strings.ToLower(r.GetAccountId()), 0, "", members(sess))
	return &pb.SessionResponse{Session: sess}, nil
}

// Signal relays a small message between members. Both ends must be members; the sender is
// verified from the session, so an adapter cannot signal into a session it is not in.
func (s *Service) Signal(ctx context.Context, r *pb.SignalRequest) (*pb.SignalResponse, error) {
	if len(r.GetPayload()) > 1024 || len(r.GetKind()) > 32 {
		return nil, store.ErrInvalid
	}
	sess, err := s.Store.Get(ctx, r.GetSessionId(), "*")
	if err != nil {
		return nil, err
	}
	from, to := strings.ToLower(r.GetFrom()), strings.ToLower(r.GetTo())
	var fromNo uint32
	var targets []string
	for _, m := range sess.GetMembers() {
		if m.GetAccountId() == from {
			fromNo = m.GetMemberNo()
		}
	}
	if fromNo == 0 {
		return nil, store.ErrNotMember
	}
	for _, m := range sess.GetMembers() {
		if m.GetAccountId() == from {
			continue
		}
		if to == "" || m.GetAccountId() == to {
			targets = append(targets, m.GetAccountId())
		}
	}
	if to != "" && len(targets) == 0 {
		return nil, store.ErrNotMember
	}
	ev := &pb.Event{Type: pb.EventType_EVENT_TYPE_SIGNAL, SessionId: sess.GetId(), AccountId: from, MemberNo: fromNo, Text: r.GetKind(), Payload: r.GetPayload(), AtUnixMs: now()}
	return &pb.SignalResponse{Delivered: uint32(s.Hub.Publish(ev, targets))}, nil
}

// Reap removes members whose heartbeat lapsed and announces it.
func (s *Service) Reap(ctx context.Context) (int, error) {
	res, sids, err := s.Store.Reap(ctx)
	for i, r := range res {
		s.announceLeave(r, sids[i], r.Account, "timeout")
	}
	return len(res), err
}

// Erase removes the account from every session (account erasure) and returns the count.
func (s *Service) Erase(ctx context.Context, account string) (int, error) {
	res, sids, err := s.Store.Erase(ctx, account)
	for i, r := range res {
		s.announceLeave(r, sids[i], strings.ToLower(account), "erased")
	}
	slog.Info("erase", "sessions", len(res))
	return len(res), err
}
