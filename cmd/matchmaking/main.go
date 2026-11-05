// Matchmaking: the neutral session core. Rooms, lobbies, members, attributes, search and
// peer-to-peer signaling for every network's adapter (np-matching2 first; Demonware, Blaze and
// NEX later). Ports: 20240 HTTP, 20241 gRPC, 20242 metrics (block 20240).
package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/openpak/matchmaking/internal/api"
	"github.com/openpak/matchmaking/internal/grpcapi"
	"github.com/openpak/matchmaking/internal/grpclog"
	"github.com/openpak/matchmaking/internal/httplog"
	"github.com/openpak/matchmaking/internal/hub"
	"github.com/openpak/matchmaking/internal/service"
	"github.com/openpak/matchmaking/internal/store"
	pb "github.com/openpak/matchmaking/proto/openpak/matchmaking/v1"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	ctx := context.Background()
	st, err := store.Open(ctx, env("MATCHMAKING_DATABASE_URL", "postgres://postgres:test@127.0.0.1:54330/matchmaking?sslmode=disable"))
	if err != nil {
		log.Fatalf("matchmaking: database: %v", err)
	}
	defer st.Close()
	svc := &service.Service{Store: st, Hub: hub.New()}
	key := os.Getenv("MATCHMAKING_INTERNAL_KEY")

	// Members whose adapter stopped heartbeating are removed (owners migrate, empty rooms end).
	go func() {
		for range time.Tick(30 * time.Second) {
			if n, err := svc.Reap(ctx); err != nil {
				slog.Error("reap", "err", err)
			} else if n > 0 {
				slog.Info("reap", "members", n)
			}
		}
	}()

	mux := http.NewServeMux()
	(&api.Server{Svc: svc, Key: key}).Register(mux)
	httpAddr := env("MATCHMAKING_HTTP_ADDR", "127.0.0.1:20240")
	go func() {
		log.Printf("matchmaking: http on %s", httpAddr)
		srv := &http.Server{Addr: httpAddr, Handler: httplog.Middleware(mux), ReadHeaderTimeout: 5 * time.Second}
		log.Fatal(srv.ListenAndServe())
	}()

	grpcAddr := env("MATCHMAKING_GRPC_ADDR", "127.0.0.1:20241")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("matchmaking: grpc listen: %v", err)
	}
	gs := grpc.NewServer(grpc.ChainUnaryInterceptor(grpclog.Unary))
	pb.RegisterMatchmakingServer(gs, grpcapi.New(svc, key))
	go func() {
		log.Printf("matchmaking: grpc on %s", grpcAddr)
		log.Fatal(gs.Serve(lis))
	}()

	metricsAddr := env("MATCHMAKING_METRICS_ADDR", "127.0.0.1:20242")
	go func() {
		m := http.NewServeMux()
		m.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
		log.Fatal(http.ListenAndServe(metricsAddr, m))
	}()

	select {}
}
