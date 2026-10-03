package command

import (
	"sync"
	"sync/atomic"
	"time"

	"ember/internal/store"
)

// Version is reported by INFO and HELLO.
const Version = "0.1.0"

// Env is the state shared by all connections.
type Env struct {
	Store      *store.Store
	Start      time.Time
	Port       int
	MaxClients int
	Table      *Table
	// AOF is nil when persistence is disabled.
	AOF Persister

	TotalConns atomic.Int64
	Rejected   atomic.Int64

	mu      sync.Mutex
	clients map[int64]*Client
	// Counters folded in from clients that have disconnected.
	doneCmds, doneHits, doneMisses int64
	nextID                         atomic.Int64
}

// NewEnv creates the shared state around a store.
func NewEnv(st *store.Store) *Env {
	return &Env{Store: st, Start: time.Now(), Table: NewTable(), clients: make(map[int64]*Client)}
}

// NewClient registers a new client for a connection from addr.
func (e *Env) NewClient(addr string) *Client {
	c := &Client{ID: e.nextID.Add(1), Addr: addr}
	e.mu.Lock()
	e.clients[c.ID] = c
	e.mu.Unlock()
	e.TotalConns.Add(1)
	return c
}

// RemoveClient unregisters c and folds its counters into the totals.
func (e *Env) RemoveClient(c *Client) {
	e.mu.Lock()
	delete(e.clients, c.ID)
	e.doneCmds += c.Commands.Load()
	e.doneHits += c.Hits.Load()
	e.doneMisses += c.Misses.Load()
	e.mu.Unlock()
}

// Stats is a snapshot of server-wide counters.
type Stats struct {
	Clients                int
	Commands, Hits, Misses int64
}

// Stats sums counters over live and finished clients.
func (e *Env) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := Stats{Clients: len(e.clients), Commands: e.doneCmds, Hits: e.doneHits, Misses: e.doneMisses}
	for _, c := range e.clients {
		s.Commands += c.Commands.Load()
		s.Hits += c.Hits.Load()
		s.Misses += c.Misses.Load()
	}
	return s
}
