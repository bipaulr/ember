package raft_test

import (
	"testing"
	"time"

	"ember/internal/raft"
	"ember/internal/raft/simnet"
)

// newIdleNode returns a node that is never started: no timers run, so the test
// alone decides which RPCs it sees and in what order.
func newIdleNode(t *testing.T) *raft.Node {
	t.Helper()
	net := simnet.New()
	return raft.New(raft.Config{
		ID:                1,
		Peers:             []int{2, 3},
		Transport:         net.Endpoint(1),
		ElectionTimeout:   time.Hour,
		HeartbeatInterval: time.Hour,
	})
}

func TestRequestVoteGrantsOneVotePerTerm(t *testing.T) {
	n := newIdleNode(t)

	first := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: 2})
	if !first.VoteGranted {
		t.Fatal("first candidate of the term was refused")
	}
	second := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: 3})
	if second.VoteGranted {
		t.Fatal("voted for two different candidates in term 1")
	}
	// A retransmitted request from the candidate we already chose must get the
	// same answer, or a lost reply would cost the candidate the vote.
	again := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: 2})
	if !again.VoteGranted {
		t.Fatal("repeated request from the chosen candidate was refused")
	}
}

func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	n := newIdleNode(t)
	n.HandleRequestVote(&raft.RequestVoteArgs{Term: 5, CandidateID: 2})

	reply := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 3, CandidateID: 3})
	if reply.VoteGranted {
		t.Fatal("granted a vote to a candidate from an older term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply term = %d, want 5 so the stale candidate can catch up", reply.Term)
	}
}

func TestRequestVoteNewTermClearsOldVote(t *testing.T) {
	n := newIdleNode(t)
	n.HandleRequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: 2})

	reply := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 2, CandidateID: 3})
	if !reply.VoteGranted {
		t.Fatal("vote from term 1 blocked a vote in term 2")
	}
	if got := n.Status().Term; got != 2 {
		t.Fatalf("term = %d, want 2", got)
	}
}

func TestRequestVoteAcceptsCandidateWithLongerLog(t *testing.T) {
	n := newIdleNode(t)

	// The node's log is empty until replication exists, and any log is at least
	// as up to date as an empty one. The refusing side of this rule gets its
	// test when nodes have logs to compare.
	reply := n.HandleRequestVote(&raft.RequestVoteArgs{Term: 1, CandidateID: 2, LastLogIndex: 7, LastLogTerm: 1})
	if !reply.VoteGranted {
		t.Fatal("refused a candidate whose log is longer than ours")
	}
}

func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	n := newIdleNode(t)
	n.HandleRequestVote(&raft.RequestVoteArgs{Term: 4, CandidateID: 2})

	reply := n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 3, LeaderID: 3})
	if reply.Success {
		t.Fatal("accepted a heartbeat from a leader of an older term")
	}
	if reply.Term != 4 {
		t.Fatalf("reply term = %d, want 4", reply.Term)
	}
	if got := n.Status().LeaderID; got != raft.None {
		t.Fatalf("recorded stale leader %d", got)
	}
}

func TestAppendEntriesAdoptsTermAndLeader(t *testing.T) {
	n := newIdleNode(t)

	reply := n.HandleAppendEntries(&raft.AppendEntriesArgs{Term: 6, LeaderID: 3})
	if !reply.Success {
		t.Fatal("rejected a heartbeat from a current leader")
	}
	st := n.Status()
	if st.Term != 6 || st.LeaderID != 3 || st.Role != raft.Follower {
		t.Fatalf("status = %+v, want follower of 3 in term 6", st)
	}
}

func TestRoleString(t *testing.T) {
	for role, want := range map[raft.Role]string{raft.Follower: "follower", raft.Candidate: "candidate", raft.Leader: "leader"} {
		if got := role.String(); got != want {
			t.Errorf("Role(%d).String() = %q, want %q", role, got, want)
		}
	}
}
