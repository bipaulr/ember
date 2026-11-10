package raft_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"ember/internal/raft"
	"ember/internal/raft/simnet"
)

const (
	testElectionTimeout = 150 * time.Millisecond
	testHeartbeat       = 30 * time.Millisecond
	// waitLimit is how long a test waits for the cluster to settle. It is many
	// election timeouts, so a failure means "stuck", not "slow machine".
	waitLimit = 5 * time.Second
)

// cluster is a set of Raft nodes joined by a simulated network.
type cluster struct {
	t     *testing.T
	net   *simnet.Network
	nodes map[int]*raft.Node

	mu      sync.Mutex
	offline map[int]bool // killed or disconnected: not expected to follow the majority

	// The safety monitor records every (term, leader) pair it sees.
	leaderOf map[uint64]int
	stopMon  chan struct{}
	monDone  chan struct{}
}

func newCluster(t *testing.T, size int) *cluster {
	t.Helper()
	c := &cluster{
		t:        t,
		net:      simnet.New(),
		nodes:    make(map[int]*raft.Node),
		offline:  make(map[int]bool),
		leaderOf: make(map[uint64]int),
		stopMon:  make(chan struct{}),
		monDone:  make(chan struct{}),
	}
	for id := 1; id <= size; id++ {
		var peers []int
		for p := 1; p <= size; p++ {
			if p != id {
				peers = append(peers, p)
			}
		}
		n := raft.New(raft.Config{
			ID:                id,
			Peers:             peers,
			Transport:         c.net.Endpoint(id),
			ElectionTimeout:   testElectionTimeout,
			HeartbeatInterval: testHeartbeat,
		})
		c.nodes[id] = n
		c.net.Register(id, n)
	}
	go c.monitor()
	for _, n := range c.nodes {
		n.Start()
	}
	t.Cleanup(c.shutdown)
	return c
}

// monitor checks election safety (Raft paper, Figure 3): at most one leader per
// term. It samples every node once a millisecond for the whole test. A leader
// keeps its role until it hears of a higher term, which takes at least one
// network round trip, so sampling this often does not miss leaders.
func (c *cluster) monitor() {
	defer close(c.monDone)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-c.stopMon:
			return
		case <-tick.C:
		}
		for id, n := range c.nodes {
			st := n.Status()
			if st.Role != raft.Leader {
				continue
			}
			c.mu.Lock()
			if other, ok := c.leaderOf[st.Term]; ok && other != id {
				c.t.Errorf("election safety violated: nodes %d and %d were both leader in term %d", other, id, st.Term)
			}
			c.leaderOf[st.Term] = id
			c.mu.Unlock()
		}
	}
}

func (c *cluster) shutdown() {
	close(c.stopMon)
	<-c.monDone
	for _, n := range c.nodes {
		n.Stop()
	}
}

// kill stops a node and cuts it off, like a crashed process.
func (c *cluster) kill(id int) {
	c.net.Disconnect(id)
	c.nodes[id].Stop()
	c.mu.Lock()
	c.offline[id] = true
	c.mu.Unlock()
}

// disconnect cuts a node off from the network but leaves it running.
func (c *cluster) disconnect(id int) {
	c.net.Disconnect(id)
	c.mu.Lock()
	c.offline[id] = true
	c.mu.Unlock()
}

func (c *cluster) reconnect(id int) {
	c.net.Connect(id)
	c.mu.Lock()
	delete(c.offline, id)
	c.mu.Unlock()
}

// online returns the status of every node that is running and connected.
func (c *cluster) online() []raft.Status {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []raft.Status
	for id, n := range c.nodes {
		if !c.offline[id] {
			out = append(out, n.Status())
		}
	}
	return out
}

// waitForLeader blocks until the online nodes agree: exactly one of them is
// leader and all the others follow it in the same term. It returns that leader.
func (c *cluster) waitForLeader() raft.Status {
	c.t.Helper()
	deadline := time.Now().Add(waitLimit)
	var last []raft.Status
	for time.Now().Before(deadline) {
		last = c.online()
		if leader, ok := agreedLeader(last); ok {
			return leader
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("no agreed leader after %v; last seen: %s", waitLimit, describe(last))
	return raft.Status{}
}

func agreedLeader(sts []raft.Status) (raft.Status, bool) {
	var leader raft.Status
	leaders := 0
	for _, st := range sts {
		if st.Role == raft.Leader {
			leader = st
			leaders++
		}
	}
	if leaders != 1 {
		return raft.Status{}, false
	}
	for _, st := range sts {
		if st.Term != leader.Term || st.LeaderID != leader.ID {
			return raft.Status{}, false
		}
	}
	return leader, true
}

func describe(sts []raft.Status) string {
	s := ""
	for _, st := range sts {
		s += fmt.Sprintf("[node %d: %s, term %d, leader %d] ", st.ID, st.Role, st.Term, st.LeaderID)
	}
	return s
}

func TestLeaderIsElected(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d nodes", size), func(t *testing.T) {
			c := newCluster(t, size)
			leader := c.waitForLeader()
			if leader.Term == 0 {
				t.Fatal("leader elected in term 0; an election must start a new term")
			}
		})
	}
}

func TestLeaderIsStableWithoutFailures(t *testing.T) {
	c := newCluster(t, 3)
	first := c.waitForLeader()

	// Heartbeats must keep followers from starting elections.
	time.Sleep(6 * testElectionTimeout)

	second := c.waitForLeader()
	if second.ID != first.ID || second.Term != first.Term {
		t.Fatalf("leadership changed with no failure: node %d term %d -> node %d term %d",
			first.ID, first.Term, second.ID, second.Term)
	}
}

func TestNewLeaderAfterLeaderIsKilled(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprintf("%d nodes", size), func(t *testing.T) {
			c := newCluster(t, size)
			old := c.waitForLeader()

			c.kill(old.ID)

			next := c.waitForLeader()
			if next.ID == old.ID {
				t.Fatal("the killed node is still reported as leader")
			}
			if next.Term <= old.Term {
				t.Fatalf("new leader's term %d is not above the old term %d", next.Term, old.Term)
			}
		})
	}
}

func TestFiveNodesSurviveTwoLeaderFailures(t *testing.T) {
	c := newCluster(t, 5)
	first := c.waitForLeader()
	c.kill(first.ID)
	second := c.waitForLeader()
	c.kill(second.ID)

	third := c.waitForLeader() // 3 of 5 remain: still a majority
	if third.ID == first.ID || third.ID == second.ID {
		t.Fatalf("a killed node (%d) became leader", third.ID)
	}
}

func TestNoLeaderWithoutMajority(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.waitForLeader()

	var survivor int
	for id := range c.nodes {
		if id != leader.ID {
			if survivor == 0 {
				survivor = id
			} else {
				c.kill(id)
			}
		}
	}
	c.kill(leader.ID)

	// One node of three keeps holding elections but can never win one.
	time.Sleep(8 * testElectionTimeout)
	st := c.nodes[survivor].Status()
	if st.Role == raft.Leader {
		t.Fatalf("node %d became leader with 1 of 3 votes", survivor)
	}
	if st.Term <= leader.Term {
		t.Fatalf("survivor never started an election: term %d, old leader term %d", st.Term, leader.Term)
	}
}

func TestPartitionedLeaderStepsDownOnRejoin(t *testing.T) {
	c := newCluster(t, 3)
	old := c.waitForLeader()

	c.disconnect(old.ID)
	next := c.waitForLeader()
	if next.ID == old.ID {
		t.Fatal("the isolated node is still counted as leader")
	}

	// Isolated, the old leader hears nothing and still believes it leads.
	if st := c.nodes[old.ID].Status(); st.Role != raft.Leader || st.Term != old.Term {
		t.Fatalf("isolated leader changed state on its own: %+v", st)
	}

	c.reconnect(old.ID)
	final := c.waitForLeader() // requires the old leader to follow the new one
	if final.Term < next.Term {
		t.Fatalf("term went backwards: %d -> %d", next.Term, final.Term)
	}
	if st := c.nodes[old.ID].Status(); st.Role == raft.Leader && st.Term == old.Term {
		t.Fatal("old leader did not step down after seeing a higher term")
	}
}

// With a slow network, a vote request takes long enough to arrive that several
// nodes time out and stand in the same term. This is where "one vote per term"
// does its work: the votes split, nobody or exactly one node wins, and the
// monitor fails the test if two nodes ever win the same term.
func TestOneLeaderPerTermWhenCandidatesCollide(t *testing.T) {
	c := newCluster(t, 5)
	c.net.SetLatency(testElectionTimeout / 3)

	for round := 0; round < 4; round++ {
		leader := c.waitForLeader()
		c.disconnect(leader.ID)
		c.waitForLeader()
		c.reconnect(leader.ID)
	}
	c.waitForLeader()

	c.mu.Lock()
	terms := len(c.leaderOf)
	c.mu.Unlock()
	if terms < 5 {
		t.Fatalf("saw leaders in only %d terms; expected at least one per round", terms)
	}
}
