package raft_test

import (
	"testing"

	"ember/internal/raft"
)

// entries builds consecutive log entries starting at index first, one per term given.
func entries(first uint64, terms ...uint64) []raft.Entry {
	var out []raft.Entry
	for i, term := range terms {
		out = append(out, raft.Entry{Index: first + uint64(i), Term: term, Data: []byte{byte(term)}})
	}
	return out
}

func TestAppendEntriesRejectsWhenPreviousEntryIsMissing(t *testing.T) {
	n := newIdleNode(t)

	// The leader thinks we hold entries 1..5; we hold none.
	reply := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: 2, PrevLogIndex: 5, PrevLogTerm: 1, Entries: entries(6, 1),
	})
	if reply.Success {
		t.Fatal("accepted entries that would leave a gap in the log")
	}
	if reply.ConflictIndex != 1 {
		t.Fatalf("conflict index = %d, want 1 (the first entry we are missing)", reply.ConflictIndex)
	}
	if got := n.Status().LastLogIndex; got != 0 {
		t.Fatalf("log grew to %d despite the rejection", got)
	}
}

func TestAppendEntriesRejectsWhenPreviousTermDiffers(t *testing.T) {
	n := newIdleNode(t)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: entries(1, 1, 1, 1)})

	reply := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 3, LeaderID: 3, PrevLogIndex: 3, PrevLogTerm: 2, Entries: entries(4, 3),
	})
	if reply.Success {
		t.Fatal("accepted entries on top of a previous entry from a different term")
	}
	// All of our term-1 entries are suspect, so the leader should jump back to
	// the first of them instead of probing one index at a time.
	if reply.ConflictIndex != 1 {
		t.Fatalf("conflict index = %d, want 1", reply.ConflictIndex)
	}
}

func TestAppendEntriesAppendsAndCommits(t *testing.T) {
	n := newIdleNode(t)

	reply := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 1, LeaderID: 2, Entries: entries(1, 1, 1, 1), LeaderCommit: 2,
	})
	if !reply.Success {
		t.Fatal("rejected entries that follow the empty log")
	}
	if st := n.Status(); st.LastLogIndex != 3 || st.CommitIndex != 2 {
		t.Fatalf("last index %d, commit %d; want 3 and 2", st.LastLogIndex, st.CommitIndex)
	}
}

func TestAppendEntriesNeverCommitsPastWhatItHolds(t *testing.T) {
	n := newIdleNode(t)

	// The leader has committed up to 10, but has only sent us entries 1..2.
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: entries(1, 1, 1), LeaderCommit: 10})
	if got := n.Status().CommitIndex; got != 2 {
		t.Fatalf("commit index = %d, want 2", got)
	}
}

func TestAppendEntriesReplacesConflictingEntries(t *testing.T) {
	n := newIdleNode(t)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: entries(1, 1, 1, 1)})

	// A new leader in term 2 has a different entry at index 2.
	reply := n.HandleAppendEntries(&raft.AppendEntriesArgs{
		Term: 2, LeaderID: 3, PrevLogIndex: 1, PrevLogTerm: 1, Entries: entries(2, 2),
	})
	if !reply.Success {
		t.Fatal("rejected a valid overwrite")
	}
	if got := n.Status().LastLogIndex; got != 2 {
		t.Fatalf("last index = %d, want 2: the old entries 2 and 3 must be removed", got)
	}
	// The entry at index 2 must now be the term-2 one.
	check := n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 2, LeaderID: 3, PrevLogIndex: 2, PrevLogTerm: 2})
	if !check.Success {
		t.Fatal("index 2 does not hold the new leader's entry")
	}
}

func TestAppendEntriesIgnoresDelayedDuplicate(t *testing.T) {
	n := newIdleNode(t)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: entries(1, 1, 1, 1), LeaderCommit: 3})

	// An older message carrying only entry 1 arrives late. It matches what we
	// have, so it must not cut the log back to length 1.
	reply := n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 1, LeaderID: 2, Entries: entries(1, 1), LeaderCommit: 1})
	if !reply.Success {
		t.Fatal("rejected a duplicate of entries we already hold")
	}
	if st := n.Status(); st.LastLogIndex != 3 || st.CommitIndex != 3 {
		t.Fatalf("last index %d, commit %d; want both to stay 3", st.LastLogIndex, st.CommitIndex)
	}
}

func TestRequestVoteRefusesCandidateWithOlderLog(t *testing.T) {
	n := newIdleNode(t)
	n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 2, LeaderID: 2, Entries: entries(1, 2, 2)})

	// A longer log that ends in an older term is behind ours.
	olderTerm := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 3, LastLogIndex: 9, LastLogTerm: 1})
	if olderTerm.VoteGranted {
		t.Fatal("voted for a candidate whose last entry is from an older term")
	}
	// Same last term but fewer entries is also behind.
	shorter := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 3, LastLogIndex: 1, LastLogTerm: 2})
	if shorter.VoteGranted {
		t.Fatal("voted for a candidate with a shorter log in the same term")
	}
	// Refusing must not use up the vote for term 3.
	equal := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 2, LastLogIndex: 2, LastLogTerm: 2})
	if !equal.VoteGranted {
		t.Fatal("refused a candidate whose log matches ours")
	}
}
