// Package api is the HTTP surface; the data path is gRPC.
//
//	GET    /healthz                                  200 when the database answers
//	DELETE /internal/accounts/{account}/data         account erasure: the account leaves every session
//	                                                 (X-Internal-Key, or Bearer ACCOUNT_ERASE_KEY) -> {"deleted":n}
package api

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/openpak/matchmaking/internal/service"
	"github.com/openpak/matchmaking/internal/store"
)

type Server struct {
	Svc *service.Service
	Key string
	// EraseKey is the shared ACCOUNT_ERASE_KEY the website's erasure finaliser sends as a bearer token.
	EraseKey string
}

func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Svc.Store.DB.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("DELETE /internal/accounts/{account}/data", s.auth(s.erase))
}

func (s *Server) auth(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ok := s.Key != "" && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Internal-Key")), []byte(s.Key)) == 1
		if b, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer "); found && s.EraseKey != "" && subtle.ConstantTimeCompare([]byte(b), []byte(s.EraseKey)) == 1 {
			ok = true
		}
		if !ok {
			jsonOut(w, http.StatusUnauthorized, map[string]any{"error": "internal key"})
			return
		}
		h(w, r)
	}
}

func jsonOut(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// erase is the account-erasure finaliser's call once the deletion grace period is over; a repeat is a 200.
func (s *Server) erase(w http.ResponseWriter, r *http.Request) {
	acct := r.PathValue("account")
	if !store.ValidAccount(acct) {
		jsonOut(w, http.StatusBadRequest, map[string]any{"error": "account ids are UUIDs"})
		return
	}
	n, err := s.Svc.Erase(r.Context(), acct)
	if err != nil {
		jsonOut(w, http.StatusBadGateway, map[string]any{"error": "matchmaking error"})
		return
	}
	jsonOut(w, 200, map[string]any{"deleted": n})
}
