package raft_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"ember/internal/raft"
)

// scriptedPeers stands in for two followers that already hold entry 1 from
// term 1. They vote for anyone. While holdBack is set, they cannot be reached
// with any entry beyond index 1, as if those messages were being lost.
type scriptedPeers struct {
	holdBack atomic.Bool
}

func (*scriptedPeers) RequestVote(_ context.Context, _ int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return &raft.RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
}

func (p *scriptedPeers) AppendEntries(_ context.Context, _ int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	last := args.PrevLogIndex + uint64(len(args.Entries))
	if last > 1 && p.holdBack.Load() {
		if args.PrevLogIndex >= 1 {
			// Pretend not to have the previous entry, so the leader backs up
			// and offers entry 1 on its own.
			return &raft.AppendEntriesReply{Term: args.Term, ConflictIndex: 1}, nil
		}
		return nil, errors.New("message lost")
	}
	return &raft.AppendEntriesReply{Term: args.Term, Success: true}, nil
}

// A leader must not commit an entry from an earlier term just because a
// majority stores it (paper section 5.4.2, Figure 8). It commits such entries
// only indirectly, by committing an entry of its own term that follows them.
func TestLeaderCommitsOldTermEntriesOnlyThroughItsOwn(t *testing.T) {
	peers := &scriptedPeers{}
	peers.holdBack.Store(true)
	sm := &recordingSM{}
	n := raft.New(raft.Config{
		ID:                  1,
		Peers:               []int{2, 3},
		Transport:           peers,
		StateMachine:        sm,
		ElectionTimeout:     50 * time.Millisecond,
		HeartbeatInterval:   10 * time.Millisecond,
		MaxEntriesPerAppend: 1, // so entry 1 can be acknowledged without entry 2
	})
	// Receive an uncommitted entry from a term-1 leader, which then disappears.
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: entries(1, 1)})
	n.Start()
	defer n.Stop()

	waitFor(t, "node to become leader", func() bool { return n.Status().Role == raft.Leader })
	// The leader's log is now [1: term 1] [2: its own no-op]. Give it time to
	// learn that both peers store entry 1; neither stores entry 2.
	time.Sleep(150 * time.Millisecond)
	st := n.Status()
	if st.LastLogIndex != 2 {
		t.Fatalf("last log index = %d, want 2 (the old entry and the new leader's no-op)", st.LastLogIndex)
	}
	if st.CommitIndex != 0 {
		t.Fatalf("commit index = %d: the term-1 entry was committed by counting replicas", st.CommitIndex)
	}

	// Once the leader's own entry reaches a majority, both commit together.
	peers.holdBack.Store(false)
	waitFor(t, "both entries to commit", func() bool { return n.Status().CommitIndex == 2 })
	waitFor(t, "both entries to be applied", func() bool { return len(sm.entries()) == 2 })
}

// A new leader with nothing to write must still commit what earlier leaders
// left behind; its no-op entry is what makes that possible.
func TestNewLeaderCommitsLeftoverEntriesWithoutNewWrites(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitForLeader()
	c.waitApplied(c.ids(), nil)

	// The leader is cut off right after accepting a write, so the write is
	// stored only in its own log.
	c.disconnect(old.ID)
	c.propose(old.ID, "stranded")
	next := c.waitForLeader()
	c.propose(next.ID, "kept")
	c.waitApplied(c.except(old.ID), []string{"kept"})

	// Kill the new leader. Whoever leads next holds "kept" from an older term
	// and receives no further writes.
	c.kill(next.ID)
	c.reconnect(old.ID)
	c.waitForLeader()
	c.waitApplied(c.except(next.ID), []string{"kept"})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitLimit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
