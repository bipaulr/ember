package raft

import "context"

// Entry is one slot of the replicated log.
type Entry struct {
	Index uint64 // position in the log, starting at 1
	Term  uint64 // term of the leader that created the entry
	Data  []byte // opaque command for the state machine
	// Noop marks the empty entry a leader appends when it is elected. It
	// carries no command; see becomeLeaderLocked for why it exists.
	Noop bool
}

// StateMachine is the application that the log drives. Apply is called once
// per committed entry, in index order, from a single goroutine. No-op entries
// are passed too (with Noop set), so the application sees every index and can
// tell when an index it was waiting on went to a different entry. Because every
// node applies the same entries in the same order, a deterministic Apply keeps
// all nodes in the same state.
type StateMachine interface {
	Apply(e Entry)
}

// RequestVoteArgs is sent by a candidate to ask for a vote (Raft paper,
// Figure 2, "RequestVote RPC").
type RequestVoteArgs struct {
	Term        uint64 // the candidate's term
	CandidateID int
	// Position of the candidate's last log entry. A voter refuses a candidate
	// whose log is behind its own, so a leader always holds every committed entry.
	LastLogIndex uint64
	LastLogTerm  uint64
}

// RequestVoteReply answers a RequestVote.
type RequestVoteReply struct {
	Term        uint64 // the voter's term, so a stale candidate can update itself
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries (Figure 2,
// "AppendEntries RPC"). With no entries it is a heartbeat.
type AppendEntriesArgs struct {
	Term     uint64 // the leader's term
	LeaderID int

	// The entry just before Entries. The follower accepts only if its own log
	// holds an entry with this index and term, which by induction means the two
	// logs are identical up to that point (the Log Matching property).
	PrevLogIndex uint64
	PrevLogTerm  uint64

	Entries      []Entry
	LeaderCommit uint64 // the leader's commit index
}

// AppendEntriesReply answers an AppendEntries.
type AppendEntriesReply struct {
	Term    uint64 // the follower's term, so a stale leader can step down
	Success bool

	// ConflictIndex is set when Success is false and Term is not newer: it is
	// where the leader should resume. If the follower's log is too short, it is
	// the first missing index. Otherwise it is the first index of the term that
	// the follower holds at PrevLogIndex, so a whole mismatched term is skipped
	// in one round trip instead of one entry at a time (paper, end of 5.3).
	ConflictIndex uint64
}

// Transport sends RPCs to other nodes. A call blocks until the reply arrives,
// the peer cannot be reached, or ctx is done. Raft tolerates any call failing
// at any time, so an implementation never needs to retry.
type Transport interface {
	RequestVote(ctx context.Context, to int, args *RequestVoteArgs) (*RequestVoteReply, error)
	AppendEntries(ctx context.Context, to int, args *AppendEntriesArgs) (*AppendEntriesReply, error)
}

// RPCHandler is the receiving side of Transport. Node implements it; a
// transport delivers each incoming request to these methods.
type RPCHandler interface {
	HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply
	HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply
}
