// Package grpcapi is the gRPC surface. It authenticates the caller by x-internal-key, maps store
// errors to gRPC codes and does nothing else.
package grpcapi

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/openpak/matchmaking/internal/service"
	"github.com/openpak/matchmaking/internal/store"
	pb "github.com/openpak/matchmaking/proto/openpak/matchmaking/v1"
)

type Server struct {
	pb.UnimplementedMatchmakingServer
	svc *service.Service
	key string
}

func New(svc *service.Service, key string) *Server { return &Server{svc: svc, key: key} }

func (s *Server) auth(ctx context.Context) error {
	md, _ := metadata.FromIncomingContext(ctx)
	v := md.Get("x-internal-key")
	if s.key == "" || len(v) == 0 || subtle.ConstantTimeCompare([]byte(v[0]), []byte(s.key)) != 1 {
		return status.Error(codes.Unauthenticated, "internal key")
	}
	return nil
}

func conv(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "not found")
	case errors.Is(err, store.ErrFull):
		return status.Error(codes.ResourceExhausted, "session full")
	case errors.Is(err, store.ErrClosed), errors.Is(err, store.ErrWrongNet):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrSecret), errors.Is(err, store.ErrNotOwner), errors.Is(err, store.ErrNotMember):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, store.ErrExists), errors.Is(err, store.ErrRevision), errors.Is(err, store.ErrNotEnough):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, store.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	}
	slog.Error("internal", "err", err.Error())
	return status.Error(codes.Internal, "matchmaking error")
}

func (s *Server) CreateSession(ctx context.Context, r *pb.CreateSessionRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.Create(ctx, r)
	return res, conv(err)
}

func (s *Server) CreateOrJoinSession(ctx context.Context, r *pb.CreateSessionRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.CreateOrJoin(ctx, r)
	return res, conv(err)
}

func (s *Server) JoinSession(ctx context.Context, r *pb.JoinSessionRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.Join(ctx, r)
	return res, conv(err)
}

func (s *Server) LeaveSession(ctx context.Context, r *pb.LeaveSessionRequest) (*pb.LeaveResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.Leave(ctx, r.GetSessionId(), r.GetAccountId(), r.GetReason())
	return res, conv(err)
}

func (s *Server) GetSession(ctx context.Context, r *pb.GetSessionRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	sess, err := s.svc.Store.Get(ctx, r.GetSessionId(), r.GetViewer())
	if err != nil {
		return nil, conv(err)
	}
	return &pb.SessionResponse{Session: sess}, nil
}

func (s *Server) SearchSessions(ctx context.Context, r *pb.SearchSessionsRequest) (*pb.SearchSessionsResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	out, hits, err := s.svc.Store.Search(ctx, r)
	if err != nil {
		return nil, conv(err)
	}
	return &pb.SearchSessionsResponse{Sessions: out, Hits: uint32(hits)}, nil
}

func (s *Server) UpdateSession(ctx context.Context, r *pb.UpdateSessionRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.UpdateSession(ctx, r)
	return res, conv(err)
}

func (s *Server) UpdateMember(ctx context.Context, r *pb.UpdateMemberRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.UpdateMember(ctx, r)
	return res, conv(err)
}

func (s *Server) KickMember(ctx context.Context, r *pb.KickMemberRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.Kick(ctx, r.GetSessionId(), r.GetAccountId(), r.GetActor())
	return res, conv(err)
}

func (s *Server) SetOwner(ctx context.Context, r *pb.SetOwnerRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.SetOwner(ctx, r)
	return res, conv(err)
}

func (s *Server) Heartbeat(ctx context.Context, r *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	n, err := s.svc.Store.Heartbeat(ctx, r.GetAccountId())
	return &pb.HeartbeatResponse{Sessions: uint32(n)}, conv(err)
}

func (s *Server) SetEndpoints(ctx context.Context, r *pb.SetEndpointsRequest) (*pb.SessionResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.SetEndpoints(ctx, r)
	return res, conv(err)
}

func (s *Server) Signal(ctx context.Context, r *pb.SignalRequest) (*pb.SignalResponse, error) {
	if err := s.auth(ctx); err != nil {
		return nil, err
	}
	res, err := s.svc.Signal(ctx, r)
	return res, conv(err)
}

func (s *Server) Watch(r *pb.WatchRequest, stream pb.Matchmaking_WatchServer) error {
	if err := s.auth(stream.Context()); err != nil {
		return err
	}
	if len(r.GetAccounts()) == 0 || len(r.GetAccounts()) > 4096 {
		return status.Error(codes.InvalidArgument, "accounts")
	}
	sub := s.svc.Hub.Subscribe(r.GetAccounts())
	defer s.svc.Hub.Unsubscribe(sub)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case ev := <-sub.C:
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}
