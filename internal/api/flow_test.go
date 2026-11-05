// The matchmaking flow against a real Postgres (MATCHMAKING_TEST_DATABASE_URL) over bufconn gRPC:
// create, join, capacity, secrets, search filters, owner migration, signaling events, heartbeat
// reaping, network scoping and account erasure. Skipped without a database.
package api_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/openpak/matchmaking/internal/api"
	"github.com/openpak/matchmaking/internal/grpcapi"
	"github.com/openpak/matchmaking/internal/hub"
	"github.com/openpak/matchmaking/internal/service"
	"github.com/openpak/matchmaking/internal/store"
	pb "github.com/openpak/matchmaking/proto/openpak/matchmaking/v1"
)

const (
	alice = "11111111-1111-1111-1111-111111111111"
	bob   = "22222222-2222-2222-2222-222222222222"
	carol = "33333333-3333-3333-3333-333333333333"
	key   = "test-key"
)

type env struct {
	c    pb.MatchmakingClient
	svc  *service.Service
	http *httptest.Server
}

func setup(t *testing.T) *env {
	url := os.Getenv("MATCHMAKING_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("MATCHMAKING_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	st, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.DB.Exec(ctx, `TRUNCATE sessions CASCADE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	svc := &service.Service{Store: st, Hub: hub.New()}
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterMatchmakingServer(gs, grpcapi.New(svc, key))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///x", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	(&api.Server{Svc: svc, Key: key, EraseKey: "erase-key"}).Register(mux)
	h := httptest.NewServer(mux)
	t.Cleanup(h.Close)
	return &env{c: pb.NewMatchmakingClient(conn), svc: svc, http: h}
}

func kctx() context.Context {
	return metadata.AppendToOutgoingContext(context.Background(), "x-internal-key", key)
}

func code(err error) codes.Code { return status.Code(err) }

var terraria = &pb.TitleRef{Network: "np", Title: "terraria"}

func ia(name string, v int64) *pb.Attr {
	return &pb.Attr{Name: name, Type: pb.AttrType_ATTR_TYPE_INT, IntValue: v, Searchable: true}
}

func sa(name, v string) *pb.Attr {
	return &pb.Attr{Name: name, Type: pb.AttrType_ATTR_TYPE_STRING, StringValue: v, Searchable: true}
}

func TestFlow(t *testing.T) {
	e := setup(t)
	ctx := kctx()

	if _, err := e.c.GetSession(context.Background(), &pb.GetSessionRequest{SessionId: alice}); code(err) != codes.Unauthenticated {
		t.Fatalf("no key: %v", err)
	}

	// create + join + capacity
	res, err := e.c.CreateSession(ctx, &pb.CreateSessionRequest{Title: terraria, Creator: alice, DisplayName: "Alice", MaxMembers: 2, UniqueKey: "world-1",
		Attrs: []*pb.Attr{ia("difficulty", 2), sa("name", "Forest"), {Name: "secret", Type: pb.AttrType_ATTR_TYPE_STRING, StringValue: "x"}}, Data: []byte("blob")})
	if err != nil {
		t.Fatal(err)
	}
	sid := res.Session.Id
	if res.MemberNo != 1 || res.Session.OwnerId != alice || len(res.Session.Members) != 1 {
		t.Fatalf("create: %+v", res)
	}
	if _, err := e.c.CreateSession(ctx, &pb.CreateSessionRequest{Title: terraria, Creator: bob, UniqueKey: "world-1"}); code(err) != codes.AlreadyExists {
		t.Fatalf("dup key: %v", err)
	}
	watch, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := e.c.Watch(watch, &pb.WatchRequest{Accounts: []string{alice}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)

	jr, err := e.c.JoinSession(ctx, &pb.JoinSessionRequest{SessionId: sid, AccountId: bob, DisplayName: "Bob", Endpoints: []*pb.Endpoint{{Kind: "udp", Host: "1.2.3.4", Port: 3658, NatType: 2}}})
	if err != nil || jr.MemberNo != 2 || len(jr.Session.Members) != 2 {
		t.Fatalf("join: %v %+v", err, jr)
	}
	ev, err := stream.Recv()
	if err != nil || ev.Type != pb.EventType_EVENT_TYPE_MEMBER_JOINED || ev.AccountId != bob || ev.To != alice {
		t.Fatalf("event: %v %+v", err, ev)
	}
	again, err := e.c.JoinSession(ctx, &pb.JoinSessionRequest{SessionId: sid, AccountId: bob})
	if err != nil || again.MemberNo != 2 || len(again.Session.Members) != 2 {
		t.Fatalf("rejoin must be idempotent: %v", err)
	}
	if _, err := e.c.JoinSession(ctx, &pb.JoinSessionRequest{SessionId: sid, AccountId: carol}); code(err) != codes.ResourceExhausted {
		t.Fatalf("full: %v", err)
	}

	// views: a non-member sees searchable attrs only and no endpoints or blob
	g, err := e.c.GetSession(ctx, &pb.GetSessionRequest{SessionId: sid, Viewer: carol})
	if err != nil || len(g.Session.Attrs) != 2 || g.Session.Data != nil || len(g.Session.Members[1].Endpoints) != 0 {
		t.Fatalf("outsider view: %v %+v", err, g.Session)
	}
	g, _ = e.c.GetSession(ctx, &pb.GetSessionRequest{SessionId: sid, Viewer: bob})
	if len(g.Session.Attrs) != 3 || string(g.Session.Data) != "blob" || len(g.Session.Members[1].Endpoints) != 1 {
		t.Fatalf("member view: %+v", g.Session)
	}

	// search
	s2, _ := e.c.CreateSession(ctx, &pb.CreateSessionRequest{Title: terraria, Creator: carol, Attrs: []*pb.Attr{ia("difficulty", 1), sa("name", "Desert")}, NetworkOnly: true})
	_ = s2
	sr, err := e.c.SearchSessions(ctx, &pb.SearchSessionsRequest{Title: terraria, Viewer: carol})
	if err != nil || sr.Hits != 2 {
		t.Fatalf("search all: %v %+v", err, sr)
	}
	sr, _ = e.c.SearchSessions(ctx, &pb.SearchSessionsRequest{Title: &pb.TitleRef{Network: "x360", Title: "terraria"}, Viewer: carol})
	if sr.Hits != 1 || sr.Sessions[0].Id != sid {
		t.Fatalf("network_only hidden from other networks: %+v", sr)
	}
	sr, _ = e.c.SearchSessions(ctx, &pb.SearchSessionsRequest{Title: terraria, Filters: []*pb.Filter{{Attr: "difficulty", Op: "ge", IntValue: 2}}})
	if sr.Hits != 1 || sr.Sessions[0].Id != sid {
		t.Fatalf("int filter: %+v", sr)
	}
	sr, _ = e.c.SearchSessions(ctx, &pb.SearchSessionsRequest{Title: terraria, Filters: []*pb.Filter{{Attr: "name", Op: "prefix", IsString: true, StringValue: "Des"}}})
	if sr.Hits != 1 || sr.Sessions[0].OwnerId != carol {
		t.Fatalf("prefix: %+v", sr)
	}
	sr, _ = e.c.SearchSessions(ctx, &pb.SearchSessionsRequest{Title: terraria, Filters: []*pb.Filter{{Attr: "secret", Op: "eq", IsString: true, StringValue: "x"}}})
	if sr.Hits != 0 {
		t.Fatalf("non-searchable attributes must not be filterable: %+v", sr)
	}
	sr, _ = e.c.SearchSessions(ctx, &pb.SearchSessionsRequest{Title: terraria, OnlyWithFreeSlots: true})
	if sr.Hits != 1 || sr.Sessions[0].OwnerId != carol {
		t.Fatalf("free slots: %+v", sr)
	}
	sr, _ = e.c.SearchSessions(ctx, &pb.SearchSessionsRequest{Title: terraria, Filters: []*pb.Filter{{Attr: "x'; drop table sessions;--", Op: "eq"}}})
	if sr != nil {
		t.Fatal("bad attribute name must be rejected")
	}

	// owner-only updates, revisions
	if _, err := e.c.UpdateSession(ctx, &pb.UpdateSessionRequest{SessionId: sid, Actor: bob, SetOpen: true}); code(err) != codes.PermissionDenied {
		t.Fatalf("non-owner update: %v", err)
	}
	us, err := e.c.UpdateSession(ctx, &pb.UpdateSessionRequest{SessionId: sid, Actor: alice, SetAttrs: []*pb.Attr{ia("difficulty", 3)}, ExpectRevision: g.Session.Revision})
	if err != nil || us.Session.Revision != g.Session.Revision+1 {
		t.Fatalf("update: %v", err)
	}
	if _, err := e.c.UpdateSession(ctx, &pb.UpdateSessionRequest{SessionId: sid, Actor: alice, ExpectRevision: g.Session.Revision}); code(err) != codes.AlreadyExists {
		t.Fatalf("stale revision: %v", err)
	}
	if _, err := e.c.UpdateMember(ctx, &pb.UpdateMemberRequest{SessionId: sid, AccountId: bob, Actor: carol, DisplayName: "x"}); code(err) != codes.PermissionDenied {
		t.Fatalf("foreign member update: %v", err)
	}

	// signaling: bob signals alice, who is watching
	sg, err := e.c.Signal(ctx, &pb.SignalRequest{SessionId: sid, From: bob, To: alice, Kind: "punch", Payload: []byte{1, 2, 3}})
	if err != nil || sg.Delivered != 1 {
		t.Fatalf("signal: %v %+v", err, sg)
	}
	for {
		ev, err = stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if ev.Type == pb.EventType_EVENT_TYPE_SIGNAL {
			break
		}
	}
	if ev.AccountId != bob || ev.Text != "punch" || len(ev.Payload) != 3 {
		t.Fatalf("signal event: %+v", ev)
	}
	if _, err := e.c.Signal(ctx, &pb.SignalRequest{SessionId: sid, From: carol, To: alice}); code(err) != codes.PermissionDenied {
		t.Fatalf("outsider signal: %v", err)
	}

	// owner leaves: no migration -> room ends
	lv, err := e.c.LeaveSession(ctx, &pb.LeaveSessionRequest{SessionId: sid, AccountId: alice})
	if err != nil || !lv.Left || !lv.SessionEnded {
		t.Fatalf("owner leave: %v %+v", err, lv)
	}
	if _, err := e.c.GetSession(ctx, &pb.GetSessionRequest{SessionId: sid}); code(err) != codes.NotFound {
		t.Fatalf("ended session: %v", err)
	}
	if lv, _ = e.c.LeaveSession(ctx, &pb.LeaveSessionRequest{SessionId: sid, AccountId: alice}); lv.Left {
		t.Fatal("second leave must be a no-op")
	}

	// migration + secret + closed + CreateOrJoin
	m, err := e.c.CreateSession(ctx, &pb.CreateSessionRequest{Title: terraria, Creator: alice, MigrateOwner: true, Secret: "pw", UniqueKey: "w2"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.c.JoinSession(ctx, &pb.JoinSessionRequest{SessionId: m.Session.Id, AccountId: bob}); code(err) != codes.PermissionDenied {
		t.Fatalf("secret: %v", err)
	}
	if _, err := e.c.JoinSession(ctx, &pb.JoinSessionRequest{SessionId: m.Session.Id, AccountId: bob, Secret: "pw"}); err != nil {
		t.Fatal(err)
	}
	lv, _ = e.c.LeaveSession(ctx, &pb.LeaveSessionRequest{SessionId: m.Session.Id, AccountId: alice})
	if lv.SessionEnded || lv.NewOwnerId != bob {
		t.Fatalf("migration: %+v", lv)
	}
	if _, err := e.c.UpdateSession(ctx, &pb.UpdateSessionRequest{SessionId: m.Session.Id, Actor: bob, SetOpen: true, Open: false}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.c.JoinSession(ctx, &pb.JoinSessionRequest{SessionId: m.Session.Id, AccountId: carol, Secret: "pw"}); code(err) != codes.FailedPrecondition {
		t.Fatalf("closed: %v", err)
	}
	coj, err := e.c.CreateOrJoinSession(ctx, &pb.CreateSessionRequest{Title: terraria, Creator: carol, UniqueKey: "w2", Secret: "pw"})
	if code(err) != codes.FailedPrecondition {
		t.Fatalf("create-or-join into closed: %v %+v", err, coj)
	}
	coj, err = e.c.CreateOrJoinSession(ctx, &pb.CreateSessionRequest{Title: terraria, Creator: carol, UniqueKey: "fresh"})
	if err != nil || !coj.Created {
		t.Fatalf("create-or-join new: %v", err)
	}

	// reaping: nobody heartbeats past ttl
	if _, err := e.svc.Store.DB.Exec(ctx, `UPDATE members SET last_seen = now() - interval '1 hour' WHERE account_id=$1`, carol); err != nil {
		t.Fatal(err)
	}
	if n, err := e.svc.Reap(ctx); err != nil || n < 1 {
		t.Fatalf("reap: %d %v", n, err)
	}
	if _, err := e.c.GetSession(ctx, &pb.GetSessionRequest{SessionId: coj.Session.Id}); code(err) != codes.NotFound {
		t.Fatalf("reaped empty room must be gone: %v", err)
	}
	hb, _ := e.c.Heartbeat(ctx, &pb.HeartbeatRequest{AccountId: bob})
	if hb.Sessions != 1 {
		t.Fatalf("heartbeat: %+v", hb)
	}

	// erasure
	req, _ := http.NewRequest("DELETE", e.http.URL+"/internal/accounts/"+bob+"/data", nil)
	r, _ := http.DefaultClient.Do(req)
	if r.StatusCode != 401 {
		t.Fatalf("erase needs key: %d", r.StatusCode)
	}
	req.Header.Set("Authorization", "Bearer erase-key")
	r, _ = http.DefaultClient.Do(req)
	if r.StatusCode != 200 {
		t.Fatalf("erase: %d", r.StatusCode)
	}
	var n int
	if err := e.svc.Store.DB.QueryRow(ctx, `SELECT (SELECT count(*) FROM members WHERE account_id=$1)+(SELECT count(*) FROM sessions WHERE owner_id=$1)`, bob).Scan(&n); err != nil || n != 0 {
		t.Fatalf("erased account must leave nothing: %d %v", n, err)
	}
}
