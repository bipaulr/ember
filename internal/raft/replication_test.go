package raft_test

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"ember/internal/raft"
)

// recordingSM is a state machine that remembers every entry applied to it.
type recordingSM struct {
	mu      sync.Mutex
	applied []raft.Entry
}

func (r *recordingSM) Apply(e raft.Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applied = append(r.applied, e)
}

// entries returns everything applied, including the no-op entries of leaders.
func (r *recordingSM) entries() []raft.Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]raft.Entry(nil), r.applied...)
}

// commands returns the data of the applied entries that carry a command.
func (r *recordingSM) commands() []string {
	var out []string
	for _, e := range r.entries() {
		if !e.Noop {
			out = append(out, string(e.Data))
		}
	}
	return out
}

// checkAppliedAgree checks state machine safety (paper, Figure 3): if any two
// nodes applied an entry at the same index, it is the same entry. It also
// checks that each node applied indexes 1, 2, 3, ... with no gap or repeat.
// Every cluster test runs it on shutdown.
func (c *cluster) checkAppliedAgree() {
	c.t.Helper()
	var longest []raft.Entry
	for id, sm := range c.sms {
		got := sm.entries()
		for i, e := range got {
			if e.Index != uint64(i+1) {
				c.t.Errorf("node %d applied index %d at position %d: entries must be applied in order, once", id, e.Index, i+1)
				return
			}
		}
		for i := 0; i < len(got) && i < len(longest); i++ {
			if got[i].Term != longest[i].Term || got[i].Noop != longest[i].Noop || !bytes.Equal(got[i].Data, longest[i].Data) {
				c.t.Errorf("state machine safety violated at index %d: node %d applied (term %d, %q), another node applied (term %d, %q)",
					i+1, id, got[i].Term, got[i].Data, longest[i].Term, longest[i].Data)
				return
			}
		}
		if len(got) > len(longest) {
			longest = got
		}
	}
}

// partition splits the network so that the listed nodes can only talk to each
// other. waitForLeader then treats them as offline.
func (c *cluster) partition(minority ...int) {
	c.net.Partition(minority)
	c.mu.Lock()
	for _, id := range minority {
		c.offline[id] = true
	}
	c.mu.Unlock()
}

func (c *cluster) heal() {
	c.net.Heal()
	c.mu.Lock()
	clear(c.offline)
	c.mu.Unlock()
}

// propose submits data to the given node, which must be the leader.
func (c *cluster) propose(id int, data string) uint64 {
	c.t.Helper()
	index, _, ok := c.nodes[id].Propose([]byte(data))
	if !ok {
		c.t.Fatalf("node %d refused %q: not the leader", id, data)
	}
	return index
}

// waitApplied blocks until each listed node has applied exactly the commands
// in want, in order. No-op entries are ignored.
func (c *cluster) waitApplied(ids []int, want []string) {
	c.t.Helper()
	deadline := time.Now().Add(waitLimit)
	for {
		lagging := ""
		for _, id := range ids {
			got := c.sms[id].commands()
			if len(got) > len(want) {
				c.t.Fatalf("node %d applied %d commands, want only %d", id, len(got), len(want))
			}
			for i := range got {
				if got[i] != want[i] {
					c.t.Fatalf("node %d applied %q as command %d, want %q", id, got[i], i+1, want[i])
				}
			}
			if len(got) < len(want) {
				lagging = fmt.Sprintf("node %d has applied %d of %d commands", id, len(got), len(want))
			}
		}
		if lagging == "" {
			return
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("timed out: %s", lagging)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// except returns every node ID that is not in skip.
func (c *cluster) except(skip ...int) []int {
	var ids []int
	for id := range c.nodes {
		if !slices.Contains(skip, id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

func (c *cluster) ids() []int { return c.except() }

func TestWritesReplicateToAllNodes(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d nodes", size), func(t *testing.T) {
			c := newCluster(t, size)
			leader := c.waitForLeader()

			var want []string
			var last uint64
			for i := 1; i <= 20; i++ {
				data := fmt.Sprintf("write-%d", i)
				index := c.propose(leader.ID, data)
				if index <= last {
					t.Fatalf("entry %q got index %d, not above the previous entry at %d", data, index, last)
				}
				last = index
				want = append(want, data)
			}
			c.waitApplied(c.ids(), want)
		})
	}
}

func TestProposeOnFollowerIsRefused(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitForLeader()
	follower := c.except(leader.ID)[0]

	before := c.nodes[follower].Status().LastLogIndex
	if _, _, ok := c.nodes[follower].Propose([]byte("x")); ok {
		t.Fatal("a follower accepted a write")
	}
	time.Sleep(2 * testHeartbeat)
	if got := c.sms[follower].commands(); len(got) != 0 {
		t.Fatalf("the refused write was applied anyway: %q (log index before: %d)", got, before)
	}
}

func TestLaggingFollowerCatchesUp(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitForLeader()
	slow := c.except(leader.ID)[0]

	c.propose(leader.ID, "before")
	c.waitApplied(c.ids(), []string{"before"})

	c.disconnect(slow)
	want := []string{"before"}
	for i := 0; i < 50; i++ {
		data := fmt.Sprintf("missed-%d", i)
		c.propose(leader.ID, data)
		want = append(want, data)
	}
	c.waitApplied(c.except(slow), want) // two of three still commit
	if got := len(c.sms[slow].commands()); got != 1 {
		t.Fatalf("disconnected follower applied %d commands, want 1", got)
	}

	c.reconnect(slow)
	c.waitApplied(c.ids(), want)
}

func TestMinorityPartitionCannotCommit(t *testing.T) {
	c := newCluster(t, 5)
	old := c.waitForLeader()
	c.propose(old.ID, "agreed")
	c.waitApplied(c.ids(), []string{"agreed"})

	// Leave the leader with a single follower: 2 of 5.
	companion := c.except(old.ID)[0]
	c.partition(old.ID, companion)
	committed := map[int]uint64{}
	for _, id := range []int{old.ID, companion} {
		committed[id] = c.nodes[id].Status().CommitIndex
	}
	lastBefore := c.nodes[old.ID].Status().LastLogIndex

	// The old leader still believes it leads and accepts the write, but it can
	// reach only one other node, so the entry must never commit.
	c.propose(old.ID, "lost")

	next := c.waitForLeader() // the three-node side elects a leader
	c.propose(next.ID, "kept-1")
	c.propose(next.ID, "kept-2")
	majority := c.except(old.ID, companion)
	c.waitApplied(majority, []string{"agreed", "kept-1", "kept-2"})

	for _, id := range []int{old.ID, companion} {
		st := c.nodes[id].Status()
		if st.CommitIndex != committed[id] {
			t.Fatalf("minority node %d moved its commit index from %d to %d while cut off", id, committed[id], st.CommitIndex)
		}
		if got := c.sms[id].commands(); len(got) != 1 {
			t.Fatalf("minority node %d applied %q, want only the first write", id, got)
		}
	}
	if st := c.nodes[old.ID].Status(); st.LastLogIndex != lastBefore+1 {
		t.Fatalf("old leader's last log index = %d, want %d (it should hold the uncommitted write)", st.LastLogIndex, lastBefore+1)
	}

	// After healing, the minority drops "lost" and adopts the majority's log.
	c.heal()
	c.waitForLeader()
	c.waitApplied(c.ids(), []string{"agreed", "kept-1", "kept-2"})
}

func TestClusterKeepsWorkingAfterLeaderIsKilled(t *testing.T) {
	c := newCluster(t, 3)
	first := c.waitForLeader()
	c.propose(first.ID, "a")
	c.waitApplied(c.ids(), []string{"a"})

	c.kill(first.ID)
	second := c.waitForLeader()
	c.propose(second.ID, "b")
	c.waitApplied(c.except(first.ID), []string{"a", "b"})
}
