package raft

import "context"

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

// AppendEntriesArgs is sent by the leader. With no entries it is a heartbeat:
// it only tells followers that a leader for Term is alive. Log replication
// fields are added in a later phase.
type AppendEntriesArgs struct {
	Term     uint64 // the leader's term
	LeaderID int
}

// AppendEntriesReply answers an AppendEntries.
type AppendEntriesReply struct {
	Term    uint64 // the follower's term, so a stale leader can step down
	Success bool
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
