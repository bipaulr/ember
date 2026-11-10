// Package raft implements the Raft consensus algorithm as described in
// "In Search of an Understandable Consensus Algorithm" (Ongaro and Ousterhout).
// It knows nothing about Ember's commands: it talks to peers through a
// Transport and, in later phases, to disk and to a state machine through
// small interfaces.
//
// Locking: every field of Node below mu is guarded by mu. The lock is never
// held while waiting for a network call. Functions whose names end in "Locked"
// expect the caller to hold mu.
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

	// ElectionTimeout is the shortest time a follower waits without hearing
	// from a leader before it starts an election. Each wait is drawn at random
	// from [ElectionTimeout, 2*ElectionTimeout) so that nodes rarely time out
	// together and split the vote.
	ElectionTimeout time.Duration
	// HeartbeatInterval is how often a leader announces itself. It must be
	// well below ElectionTimeout.
	HeartbeatInterval time.Duration

	Logger *slog.Logger // nil discards logs
}

// Status is a snapshot of a node's view of the cluster.
type Status struct {
	ID       int
	Term     uint64
	Role     Role
	LeaderID int // None if this node does not know of a leader for Term
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

	role     Role
	leaderID int

	electionDeadline time.Time // follower/candidate: start an election at this time
	lastHeartbeat    time.Time // leader: when heartbeats were last sent

	stopped bool
	ctx     context.Context // cancelled by Stop; ends in-flight RPCs
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// New creates a node as a follower in term 0. It does nothing until Start.
func New(cfg Config) *Node {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Node{
		cfg:    cfg,
		log:    cfg.Logger.With("node", cfg.ID),
		role:   Follower,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins running timers: the node will hold elections and, if it wins,
// send heartbeats.
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.resetElectionTimerLocked()
	n.goLocked(n.run)
}

// Stop halts the node and waits for its goroutines. It is safe to call twice.
func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	n.mu.Unlock()
	n.cancel()
	n.wg.Wait()
}

// Status reports the node's current term, role and known leader.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{ID: n.cfg.ID, Term: n.currentTerm, Role: n.role, LeaderID: n.leaderID}
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
			n.broadcastHeartbeatLocked()
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

// lastLogPositionLocked returns the index and term of the last log entry. The
// log arrives with replication; until then every node's log is empty.
func (n *Node) lastLogPositionLocked() (index, term uint64) {
	return 0, 0
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

// startElectionLocked makes this node a candidate for a new term and asks
// every peer for its vote (paper section 5.2).
func (n *Node) startElectionLocked() {
	n.currentTerm++
	n.role = Candidate
	n.votedFor = n.cfg.ID
	n.leaderID = None
	// If this election ends in a split vote, the timer fires again and a new
	// election starts with a fresh random timeout.
	n.resetElectionTimerLocked()
	n.log.Debug("starting election", "term", n.currentTerm)

	lastIndex, lastTerm := n.lastLogPositionLocked()
	args := &RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.cfg.ID,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	votes := 1 // our own; guarded by mu like the rest of the node
	if votes >= n.majority() {
		n.becomeLeaderLocked() // a cluster of one
		return
	}
	for _, peer := range n.cfg.Peers {
		n.goLocked(func() {
			reply, ok := n.sendRequestVote(peer, args)
			if !ok {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			// The reply may arrive after this node has moved on: it may have
			// won already, lost, or started a later election. A vote only
			// counts for the election it was requested in.
			if n.role != Candidate || n.currentTerm != args.Term {
				return
			}
			if !reply.VoteGranted {
				return
			}
			votes++
			if votes == n.majority() {
				n.becomeLeaderLocked()
			}
		})
	}
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.log.Info("became leader", "term", n.currentTerm)
	// Announce at once, so followers stop their election timers before any of
	// them times out and disturbs the new leader.
	n.broadcastHeartbeatLocked()
}

// broadcastHeartbeatLocked sends an empty AppendEntries to every peer.
func (n *Node) broadcastHeartbeatLocked() {
	n.lastHeartbeat = time.Now()
	args := &AppendEntriesArgs{Term: n.currentTerm, LeaderID: n.cfg.ID}
	for _, peer := range n.cfg.Peers {
		n.goLocked(func() {
			reply, ok := n.sendAppendEntries(peer, args)
			if !ok {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			// A higher term in any reply means a newer election has happened
			// and this node is no longer the leader.
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
			}
		})
	}
}

// sendRequestVote and sendAppendEntries make one attempt, bounded by the
// election timeout: after that long the reply could no longer matter. They
// must be called without mu held.
func (n *Node) sendRequestVote(peer int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
	defer cancel()
	reply, err := n.cfg.Transport.RequestVote(ctx, peer, args)
	return reply, err == nil
}

func (n *Node) sendAppendEntries(peer int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
	defer cancel()
	reply, err := n.cfg.Transport.AppendEntries(ctx, peer, args)
	return reply, err == nil
}

// HandleRequestVote decides whether to vote for a candidate (Figure 2,
// "RequestVote RPC, receiver implementation").
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	reply := &RequestVoteReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply
	}

	// One vote per term, first come first served. Asking again for the same
	// candidate is allowed so that a lost reply can be retried.
	canVote := n.votedFor == None || n.votedFor == args.CandidateID
	if !canVote || !n.candidateLogIsCurrentLocked(args) {
		return reply
	}
	n.votedFor = args.CandidateID
	reply.VoteGranted = true
	// Having voted, give the candidate time to win before competing with it.
	n.resetElectionTimerLocked()
	return reply
}

// candidateLogIsCurrentLocked reports whether the candidate's log is at least
// as up to date as this node's (paper section 5.4.1): a later last term wins;
// with equal last terms the longer log wins.
func (n *Node) candidateLogIsCurrentLocked(args *RequestVoteArgs) bool {
	lastIndex, lastTerm := n.lastLogPositionLocked()
	if args.LastLogTerm != lastTerm {
		return args.LastLogTerm > lastTerm
	}
	return args.LastLogIndex >= lastIndex
}

// HandleAppendEntries accepts a heartbeat from the leader of the current term.
func (n *Node) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	reply := &AppendEntriesReply{Term: n.currentTerm}
	if args.Term < n.currentTerm {
		return reply // a deposed leader; the reply's term tells it to step down
	}

	// args.Term == currentTerm: the sender won this term's election. A
	// candidate in the same term has therefore lost and returns to follower,
	// keeping its vote (it voted for itself in this term and must not vote again).
	n.role = Follower
	n.leaderID = args.LeaderID
	n.resetElectionTimerLocked()
	reply.Success = true
	return reply
}
