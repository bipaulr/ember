// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Ongaro and Ousterhout).
// It knows nothing about Ember's commands: it talks to peers through a
// Transport and hands committed entries to a StateMachine.
//
// The code is split by concern:
//
//	node.go         the Node type, its lifecycle and timers
//	election.go     becoming leader: RequestVote, both sides
//	replication.go  being leader or follower: AppendEntries, both sides, and commit
//	apply.go        feeding committed entries to the state machine
//
// Locking: every field of Node below mu is guarded by mu. The lock is never
// held while waiting for a network call or while the state machine runs.
// Functions whose names end in "Locked" expect the caller to hold mu.
package raft

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// None means "no node": no vote cast, or no known leader. Node IDs start at 1.
const None = 0

// tickInterval is how often a node checks its timers. It only needs to be
// small compared with the heartbeat interval.
const tickInterval = 5 * time.Millisecond

// Role is what a node currently believes it is.
type Role int

// A node is always in exactly one of these roles.
const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "follower"
}

// Config describes one node of a fixed-size cluster.
type Config struct {
	ID        int   // this node, 1 or greater
	Peers     []int // every other node in the cluster
	Transport Transport
	// StateMachine receives committed entries. Nil discards them.
	StateMachine StateMachine

	// ElectionTimeout is the shortest time a follower waits without hearing
	// from a leader before it starts an election. Each wait is drawn at random
	// from [ElectionTimeout, 2*ElectionTimeout) so that nodes rarely time out
	// together and split the vote.
	ElectionTimeout time.Duration
	// HeartbeatInterval is how often a leader contacts each follower. It must
	// be well below ElectionTimeout.
	HeartbeatInterval time.Duration
	// MaxEntriesPerAppend caps the entries in one AppendEntries, so a follower
	// that is far behind is caught up in bounded messages. Zero selects
	// DefaultMaxEntriesPerAppend.
	MaxEntriesPerAppend int

	Logger *slog.Logger // nil discards logs
}

// Status is a snapshot of a node's view of the cluster.
type Status struct {
	ID       int
	Term     uint64
	Role     Role
	LeaderID int // None if this node does not know of a leader for Term

	LastLogIndex uint64 // index of the last entry in this node's log
	CommitIndex  uint64 // highest index known to be committed
	LastApplied  uint64 // highest index handed to the state machine
}

// Node is one member of a Raft cluster.
type Node struct {
	cfg Config
	log *slog.Logger

	mu sync.Mutex

	// State that Figure 2 of the paper calls persistent. It will be written to
	// disk before any reply that depends on it, starting in the persistence phase.
	currentTerm uint64
	votedFor    int // candidate that received this node's vote in currentTerm, or None
	// entries is the log. entries[0] is a placeholder with index 0 and term 0,
	// so that entries[i].Index == i and "the entry before the first" exists.
	entries []Entry

	role     Role
	leaderID int

	commitIndex uint64 // highest index known to be stored on a majority
	lastApplied uint64 // highest index handed to the state machine
	// applyCond wakes the apply loop when commitIndex moves or the node stops.
	applyCond *sync.Cond

	// Leader-only state, rebuilt at every election win (Figure 2, "volatile
	// state on leaders").
	nextIndex  map[int]uint64 // next entry to send to each peer
	matchIndex map[int]uint64 // highest entry known to be stored on each peer
	// inflight counts AppendEntries calls in progress to each peer. A new write
	// is not sent to a peer that is busy; it rides on the next call, which
	// batches writes naturally.
	inflight map[int]int

	electionDeadline time.Time // follower/candidate: start an election at this time
	lastHeartbeat    time.Time // leader: when the periodic AppendEntries last went out

	stopped bool
	ctx     context.Context // cancelled by Stop; ends in-flight RPCs
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// New creates a node as a follower in term 0. It does nothing until Start.
func New(cfg Config) *Node {
	if cfg.MaxEntriesPerAppend <= 0 {
		cfg.MaxEntriesPerAppend = DefaultMaxEntriesPerAppend
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{
		cfg:      cfg,
		log:      cfg.Logger.With("node", cfg.ID),
		role:     Follower,
		entries:  []Entry{{}},
		inflight: make(map[int]int),
		ctx:      ctx,
		cancel:   cancel,
	}
	n.applyCond = sync.NewCond(&n.mu)
	return n
}

// Start begins running timers and applying committed entries.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetElectionTimerLocked()
	n.goLocked(n.run)
	n.goLocked(n.applyLoop)
}

// Stop halts the node and waits for its goroutines. It is safe to call twice.
func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.cancel()
	n.wg.Wait()
}

// Status reports the node's current view.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:           n.cfg.ID,
		Term:         n.currentTerm,
		Role:         n.role,
		LeaderID:     n.leaderID,
		LastLogIndex: n.lastLogIndexLocked(),
		CommitIndex:  n.commitIndex,
		LastApplied:  n.lastApplied,
	}
}

// goLocked runs f on a goroutine that Stop waits for. Checking stopped under
// the lock guarantees no goroutine is added after Stop has begun waiting.
func (n *Node) goLocked(f func()) {
	if n.stopped {
		return
	}
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		f()
	}()
}

// run drives the node's timers until Stop.
func (n *Node) run() {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

// tick fires whichever timer is due: a leader's heartbeat or everyone else's
// election timeout.
func (n *Node) tick() {
	n.mu.Lock()
	defer n.mu.Unlock()
	now := time.Now()
	if n.role == Leader {
		if now.Sub(n.lastHeartbeat) >= n.cfg.HeartbeatInterval {
			n.lastHeartbeat = now
			for _, peer := range n.cfg.Peers {
				n.replicateLocked(peer)
			}
		}
		return
	}
	if !now.Before(n.electionDeadline) {
		n.startElectionLocked()
	}
}

func (n *Node) resetElectionTimerLocked() {
	jitter := rand.N(n.cfg.ElectionTimeout)
	n.electionDeadline = time.Now().Add(n.cfg.ElectionTimeout + jitter)
}

// majority is the number of nodes, counting this one, that must agree.
func (n *Node) majority() int {
	return (len(n.cfg.Peers)+1)/2 + 1
}

func (n *Node) lastLogIndexLocked() uint64 {
	return n.entries[len(n.entries)-1].Index
}

func (n *Node) lastLogTermLocked() uint64 {
	return n.entries[len(n.entries)-1].Term
}

// becomeFollowerLocked moves the node into a newer term that it learned about
// from a peer. Its vote is cleared because votedFor only has meaning within
// one term.
func (n *Node) becomeFollowerLocked(term uint64) {
	if n.role != Follower {
		n.log.Debug("stepping down", "from", n.role.String(), "old_term", n.currentTerm, "new_term", term)
	}
	n.currentTerm = term
	n.votedFor = None
	n.role = Follower
	n.leaderID = None
}
