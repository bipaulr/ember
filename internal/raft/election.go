package raft

import (
	"context"
	"time"
)

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

	args := &RequestVoteArgs{
		Term:         n.currentTerm,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.lastLogIndexLocked(),
		LastLogTerm:  n.lastLogTermLocked(),
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

	// Assume every follower is fully caught up; the first AppendEntries to a
	// follower that is not will be rejected and nextIndex moves back.
	n.nextIndex = make(map[int]uint64)
	n.matchIndex = make(map[int]uint64)
	for _, peer := range n.cfg.Peers {
		n.nextIndex[peer] = n.lastLogIndexLocked() + 1
		n.matchIndex[peer] = 0
	}

	// Start the term with an empty entry (paper section 8). A leader may only
	// commit by counting replicas of an entry from its own term, so without
	// this, entries left over from earlier terms would stay uncommitted until a
	// client happened to write something, and on an idle cluster that is never.
	// Committing the no-op commits everything before it.
	n.entries = append(n.entries, Entry{Index: n.lastLogIndexLocked() + 1, Term: n.currentTerm, Noop: true})
	n.advanceCommitLocked() // commits immediately in a cluster of one

	// Announce at once, so followers stop their election timers before any of
	// them times out and disturbs the new leader. The message carries the no-op.
	n.lastHeartbeat = time.Now()
	for _, peer := range n.cfg.Peers {
		n.replicateLocked(peer)
	}
}

// sendRequestVote makes one attempt, bounded by the election timeout: after
// that long the election it belongs to is over. Call it without mu held.
func (n *Node) sendRequestVote(peer int, args *RequestVoteArgs) (*RequestVoteReply, bool) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
	defer cancel()
	reply, err := n.cfg.Transport.RequestVote(ctx, peer, args)
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
//
// This is what makes it safe for a new leader to never fetch entries from
// anyone: an entry is committed only when a majority stores it, a candidate
// needs votes from a majority, and the two majorities overlap in a node that
// refuses any candidate missing that entry.
func (n *Node) candidateLogIsCurrentLocked(args *RequestVoteArgs) bool {
	lastTerm := n.lastLogTermLocked()
	if args.LastLogTerm != lastTerm {
		return args.LastLogTerm > lastTerm
	}
	return args.LastLogIndex >= n.lastLogIndexLocked()
}
