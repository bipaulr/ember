package raft

import (
	"cmp"
	"context"
	"slices"
)

// DefaultMaxEntriesPerAppend is the default for Config.MaxEntriesPerAppend.
const DefaultMaxEntriesPerAppend = 1024

// Propose appends data to the leader's log and starts replicating it. It
// returns at once with the entry's index and term; the entry is committed only
// if the state machine later sees an entry with that index AND that term. If a
// different term shows up at that index, this node lost leadership and the
// entry was discarded.
//
// ok is false if this node is not the leader, in which case nothing was appended.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Leader || n.stopped {
		return 0, 0, false
	}
	entry := Entry{Index: n.lastLogIndexLocked() + 1, Term: n.currentTerm, Data: data}
	n.entries = append(n.entries, entry)

	for _, peer := range n.cfg.Peers {
		if n.inflight[peer] == 0 {
			n.replicateLocked(peer)
		}
	}
	n.advanceCommitLocked() // commits immediately in a cluster of one
	return entry.Index, entry.Term, true
}

// replicateLocked sends one AppendEntries to peer carrying whatever the peer
// is missing (possibly nothing, which makes it a heartbeat), and handles the
// reply on a separate goroutine.
func (n *Node) replicateLocked(peer int) {
	next := n.nextIndex[peer]
	prev := n.entries[next-1]
	end := min(uint64(len(n.entries)), next+uint64(n.cfg.MaxEntriesPerAppend))
	args := &AppendEntriesArgs{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: prev.Index,
		PrevLogTerm:  prev.Term,
		// Copy: the slice is read by the transport after the lock is released,
		// while the log underneath may be appended to or truncated.
		Entries:      slices.Clone(n.entries[next:end]),
		LeaderCommit: n.commitIndex,
	}

	n.inflight[peer]++
	n.goLocked(func() {
		reply, ok := n.sendAppendEntries(peer, args)

		n.mu.Lock()
		defer n.mu.Unlock()
		n.inflight[peer]--
		if !ok {
			return // unreachable; the next heartbeat tries again
		}
		if reply.Term > n.currentTerm {
			// A newer election has happened; this node is no longer leader.
			n.becomeFollowerLocked(reply.Term)
			return
		}
		// Ignore a reply to a request from an earlier leadership of this node.
		if n.role != Leader || n.currentTerm != args.Term {
			return
		}
		n.handleAppendReplyLocked(peer, args, reply)
	})
}

func (n *Node) handleAppendReplyLocked(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if reply.Success {
		// The follower now matches our log up to the last entry we sent.
		// Replies can arrive out of order, so matchIndex only moves forward.
		match := args.PrevLogIndex + uint64(len(args.Entries))
		if match > n.matchIndex[peer] {
			n.matchIndex[peer] = match
			n.nextIndex[peer] = match + 1
			n.advanceCommitLocked()
		}
	} else {
		// The follower's log differs from ours at PrevLogIndex. Back up to
		// where it says the difference starts, but never below what it has
		// already confirmed.
		n.nextIndex[peer] = max(reply.ConflictIndex, n.matchIndex[peer]+1)
	}
	// Keep going without waiting for the heartbeat if there is more to send.
	if n.nextIndex[peer] <= n.lastLogIndexLocked() && n.inflight[peer] == 0 {
		n.replicateLocked(peer)
	}
}

// advanceCommitLocked moves commitIndex to the highest index stored on a
// majority, provided that entry is from the current term.
//
// The term condition matters (paper section 5.4.2, Figure 8): an entry from an
// older term can be on a majority and still be overwritten by a future leader
// that never saw it. Once an entry of the current term is on a majority, no
// node without it can win an election, so it and everything before it are safe.
func (n *Node) advanceCommitLocked() {
	stored := []uint64{n.lastLogIndexLocked()} // the leader itself
	for _, peer := range n.cfg.Peers {
		stored = append(stored, n.matchIndex[peer])
	}
	slices.SortFunc(stored, func(a, b uint64) int { return cmp.Compare(b, a) }) // descending
	// The majority-th highest value is stored on at least a majority of nodes.
	candidate := stored[n.majority()-1]

	if candidate > n.commitIndex && n.entries[candidate].Term == n.currentTerm {
		n.commitIndex = candidate
		n.applyCond.Broadcast()
	}
}

// sendAppendEntries makes one attempt. Call it without mu held.
func (n *Node) sendAppendEntries(peer int, args *AppendEntriesArgs) (*AppendEntriesReply, bool) {
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.ElectionTimeout)
	defer cancel()
	reply, err := n.cfg.Transport.AppendEntries(ctx, peer, args)
	return reply, err == nil
}

// HandleAppendEntries is the follower side of replication (Figure 2,
// "AppendEntries RPC, receiver implementation").
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

	// Consistency check: we must hold the entry that precedes the new ones.
	if args.PrevLogIndex > n.lastLogIndexLocked() {
		reply.ConflictIndex = n.lastLogIndexLocked() + 1
		return reply
	}
	if ourTerm := n.entries[args.PrevLogIndex].Term; ourTerm != args.PrevLogTerm {
		first := args.PrevLogIndex
		for first > 1 && n.entries[first-1].Term == ourTerm {
			first--
		}
		reply.ConflictIndex = first
		return reply
	}

	n.appendFromLeaderLocked(args.Entries)

	// Everything up to the last entry in this message is now known to match
	// the leader, so it is safe to commit up to there, but no further: entries
	// we hold beyond it are unverified and might yet be replaced.
	lastVerified := args.PrevLogIndex + uint64(len(args.Entries))
	if commit := min(args.LeaderCommit, lastVerified); commit > n.commitIndex {
		n.commitIndex = commit
		n.applyCond.Broadcast()
	}
	reply.Success = true
	return reply
}

// appendFromLeaderLocked merges entries from the leader into the log. An entry
// we already hold with the same term is skipped. The log is cut only at a real
// conflict (same index, different term): a delayed, shorter message must not
// remove entries that a later message already added.
func (n *Node) appendFromLeaderLocked(entries []Entry) {
	for i, e := range entries {
		if e.Index <= n.lastLogIndexLocked() {
			if n.entries[e.Index].Term == e.Term {
				continue
			}
			n.entries = n.entries[:e.Index]
		}
		n.entries = append(n.entries, entries[i:]...)
		return
	}
}
