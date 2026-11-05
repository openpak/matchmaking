// Package hub is the in-memory fan-out for Watch streams. It holds no state worth keeping:
// session truth is Postgres, and an adapter whose stream dropped calls GetSession to resync.
package hub

import (
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	pb "github.com/openpak/matchmaking/proto/openpak/matchmaking/v1"
)

type Sub struct {
	C        chan *pb.Event
	accounts map[string]bool
}

type Hub struct {
	mu   sync.RWMutex
	subs map[*Sub]struct{}
}

func New() *Hub { return &Hub{subs: map[*Sub]struct{}{}} }

// Subscribe registers a stream for events addressed to any of the accounts.
func (h *Hub) Subscribe(accounts []string) *Sub {
	s := &Sub{C: make(chan *pb.Event, 512), accounts: map[string]bool{}}
	for _, a := range accounts {
		s.accounts[strings.ToLower(a)] = true
	}
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
	return s
}

func (h *Hub) Unsubscribe(s *Sub) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

// Live reports whether any stream is subscribed for the account.
func (h *Hub) Live(account string) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.subs {
		if s.accounts[strings.ToLower(account)] {
			return true
		}
	}
	return false
}

// Publish sends a copy of ev to every stream for each recipient (To is set per copy).
// A full stream drops the event: the adapter resyncs from GetSession.
func (h *Hub) Publish(ev *pb.Event, recipients []string) (delivered int) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, to := range recipients {
		to = strings.ToLower(to)
		for s := range h.subs {
			if !s.accounts[to] {
				continue
			}
			c := proto.Clone(ev).(*pb.Event)
			c.To = to
			select {
			case s.C <- c:
				delivered++
			default:
			}
		}
	}
	return delivered
}
