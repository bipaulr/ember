package cluster_test

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"ember/internal/cluster"
	"ember/internal/command"
	"ember/internal/raft"
	"ember/internal/raft/simnet"
	"ember/internal/resp"
	"ember/internal/store"
)

const (
	electionTimeout = 150 * time.Millisecond
	heartbeat       = 30 * time.Millisecond
	waitLimit       = 5 * time.Second
)

// member is one Ember node: its own keyspace, command table and Raft node.
type member struct {
	id   int
	env  *command.Env
	raft *raft.Node
	cl   *cluster.Cluster
}

// do runs a command the way a client connection would and returns the raw
// RESP reply.
func (m *member) do(args ...string) string {
	var buf bytes.Buffer
	w := resp.NewWriter(&buf)
	c := &command.Ctx{W: w, Store: m.env.Store, Client: &command.Client{}, Env: m.env}
	for _, a := range args {
		c.Args = append(c.Args, []byte(a))
	}
	m.env.Table.Execute(c)
	_ = w.Flush()
	return buf.String()
}

type testCluster struct {
	t       *testing.T
	net     *simnet.Network
	members map[int]*member
}

func clientAddr(id int) string { return fmt.Sprintf("10.0.0.%d:6380", id) }

type options struct {
	proposeTimeout time.Duration
	start          bool
}

func newTestCluster(t *testing.T, size int, opt options) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, net: simnet.New(), members: make(map[int]*member)}
	addrs := make(map[int]string)
	for id := 1; id <= size; id++ {
		addrs[id] = clientAddr(id)
	}
	for id := 1; id <= size; id++ {
		var peers []int
		for p := 1; p <= size; p++ {
			if p != id {
				peers = append(peers, p)
			}
		}
		clock := &cluster.Clock{}
		env := command.NewEnv(store.NewWithClock(16, clock.Now))
		cl := cluster.New(cluster.Config{
			ID:             id,
			Env:            env,
			Clock:          clock,
			ClientAddrs:    addrs,
			ProposeTimeout: opt.proposeTimeout,
		})
		node := raft.New(raft.Config{
			ID:                id,
			Peers:             peers,
			Transport:         tc.net.Endpoint(id),
			StateMachine:      cl,
			ElectionTimeout:   electionTimeout,
			HeartbeatInterval: heartbeat,
		})
		cl.Attach(node)
		tc.net.Register(id, node)
		tc.members[id] = &member{id: id, env: env, raft: node, cl: cl}
	}
	if opt.start {
		for _, m := range tc.members {
			m.raft.Start()
			m.cl.Start()
		}
	}
	t.Cleanup(func() {
		for _, m := range tc.members {
			m.cl.Stop()
			m.raft.Stop()
		}
	})
	return tc
}

func startCluster(t *testing.T, size int) *testCluster {
	return newTestCluster(t, size, options{start: true})
}

// leader waits until exactly one of the given nodes (all, if none given) is
// leader and the rest of them follow it.
func (tc *testCluster) leader(among ...int) *member {
	tc.t.Helper()
	if len(among) == 0 {
		for id := range tc.members {
			among = append(among, id)
		}
	}
	deadline := time.Now().Add(waitLimit)
	for time.Now().Before(deadline) {
		var leader *member
		agreed := true
		for _, id := range among {
			if st := tc.members[id].raft.Status(); st.Role == raft.Leader {
				if leader != nil {
					agreed = false
				}
				leader = tc.members[id]
			}
		}
		if leader != nil && agreed {
			term := leader.raft.Status().Term
			for _, id := range among {
				if st := tc.members[id].raft.Status(); st.LeaderID != leader.id || st.Term != term {
					agreed = false
				}
			}
			if agreed {
				return leader
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	tc.t.Fatal("no leader elected")
	return nil
}

func (tc *testCluster) others(skip ...int) []int {
	var ids []int
	for id := range tc.members {
		if !slices.Contains(skip, id) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids
}

// eventually waits until running the command on node id gives want.
func (tc *testCluster) eventually(id int, want string, args ...string) {
	tc.t.Helper()
	deadline := time.Now().Add(waitLimit)
	var got string
	for time.Now().Before(deadline) {
		if got = tc.members[id].do(args...); got == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	tc.t.Fatalf("node %d: %v = %q, want %q", id, args, got, want)
}

func TestWriteOnLeaderIsVisibleOnEveryNode(t *testing.T) {
	tc := startCluster(t, 3)
	leader := tc.leader()

	if got := leader.do("SET", "k", "v"); got != "+OK\r\n" {
		t.Fatalf("SET on the leader = %q", got)
	}
	// The leader replies only after applying, so its own read sees the write at once.
	if got := leader.do("GET", "k"); got != "$1\r\nv\r\n" {
		t.Fatalf("GET on the leader right after SET = %q", got)
	}
	for _, id := range tc.others() {
		tc.eventually(id, "$1\r\nv\r\n", "GET", "k")
	}
}

func TestRepliesAreProducedByTheStateMachine(t *testing.T) {
	tc := startCluster(t, 3)
	leader := tc.leader()

	steps := []struct {
		cmd  []string
		want string
	}{
		{[]string{"INCR", "n"}, ":1\r\n"},
		{[]string{"INCR", "n"}, ":2\r\n"},
		{[]string{"SET", "once", "a", "NX"}, "+OK\r\n"},
		{[]string{"SET", "once", "b", "NX"}, "$-1\r\n"},
		{[]string{"RPUSH", "l", "x", "y"}, ":2\r\n"},
		{[]string{"LPOP", "l"}, "$1\r\nx\r\n"},
		{[]string{"DEL", "once", "missing"}, ":1\r\n"},
		{[]string{"INCR", "l"}, "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"},
	}
	for _, s := range steps {
		if got := leader.do(s.cmd...); got != s.want {
			t.Fatalf("%v = %q, want %q", s.cmd, got, s.want)
		}
	}
	for _, id := range tc.others() {
		tc.eventually(id, "$1\r\n2\r\n", "GET", "n")
		tc.eventually(id, ":1\r\n", "LLEN", "l")
		tc.eventually(id, ":0\r\n", "EXISTS", "once")
	}
}

func TestFollowerRedirectsWritesToTheLeader(t *testing.T) {
	tc := startCluster(t, 3)
	leader := tc.leader()
	follower := tc.members[tc.others(leader.id)[0]]

	want := "-MOVED 0 " + clientAddr(leader.id) + "\r\n"
	if got := follower.do("SET", "k", "v"); got != want {
		t.Fatalf("SET on a follower = %q, want %q", got, want)
	}
	if got := leader.do("EXISTS", "k"); got != ":0\r\n" {
		t.Fatal("the redirected write was applied anyway")
	}
	// Commands that do not write are still answered locally.
	if got := follower.do("PING"); got != "+PONG\r\n" {
		t.Fatalf("PING on a follower = %q", got)
	}
}

func TestWriteWithNoLeaderReportsClusterDown(t *testing.T) {
	tc := newTestCluster(t, 3, options{start: false}) // nobody ever holds an election

	got := tc.members[1].do("SET", "k", "v")
	if !strings.HasPrefix(got, "-CLUSTERDOWN ") {
		t.Fatalf("SET with no leader = %q, want a CLUSTERDOWN error", got)
	}
}

func TestExpiryTimeIsIdenticalOnEveryNode(t *testing.T) {
	tc := startCluster(t, 3)
	leader := tc.leader()

	if got := leader.do("SET", "k", "v", "EX", "100"); got != "+OK\r\n" {
		t.Fatalf("SET EX = %q", got)
	}
	if got := leader.do("EXPIRE", "k", "200"); got != ":1\r\n" {
		t.Fatalf("EXPIRE = %q", got)
	}
	expireAt := func(m *member) (at int64) {
		m.env.Store.View("k", func(e *store.Entry) {
			if e != nil {
				at = e.ExpireAt
			}
		})
		return at
	}
	want := expireAt(leader)
	if want == 0 {
		t.Fatal("the key has no expiry on the leader")
	}
	for _, id := range tc.others(leader.id) {
		tc.eventually(id, ":1\r\n", "EXISTS", "k")
		deadline := time.Now().Add(waitLimit)
		for expireAt(tc.members[id]) != want {
			if time.Now().After(deadline) {
				// Each node computing "now + 200s" from its own clock would differ.
				t.Fatalf("node %d expires the key at %d, leader at %d", id, expireAt(tc.members[id]), want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func TestKeyExpiresOnAnIdleCluster(t *testing.T) {
	tc := startCluster(t, 3)
	leader := tc.leader()

	if got := leader.do("SET", "k", "v", "PX", "200"); got != "+OK\r\n" {
		t.Fatalf("SET PX = %q", got)
	}
	// No more writes arrive. Time must still move forward on every node.
	for _, id := range tc.others() {
		tc.eventually(id, "$-1\r\n", "GET", "k")
	}
}

func TestWriteThatCannotReachAMajorityTimesOut(t *testing.T) {
	tc := newTestCluster(t, 3, options{start: true, proposeTimeout: 400 * time.Millisecond})
	old := tc.leader()
	tc.net.Disconnect(old.id)

	got := old.do("SET", "k", "v")
	if !strings.HasPrefix(got, "-ERR write timed out") {
		t.Fatalf("SET on an isolated leader = %q, want a timeout error", got)
	}
	if got := old.do("EXISTS", "k"); got != ":0\r\n" {
		t.Fatal("the isolated leader applied a write that no majority accepted")
	}

	rest := tc.others(old.id)
	next := tc.leader(rest...)
	if got := next.do("SET", "other", "1"); got != "+OK\r\n" {
		t.Fatalf("SET on the majority side = %q", got)
	}

	tc.net.Connect(old.id)
	tc.eventually(old.id, ":1\r\n", "EXISTS", "other")
	if got := old.do("EXISTS", "k"); got != ":0\r\n" {
		t.Fatal("the timed-out write appeared after the node rejoined")
	}
}

func TestPendingWriteFailsWhenLeadershipIsLost(t *testing.T) {
	tc := newTestCluster(t, 3, options{start: true, proposeTimeout: time.Minute})
	old := tc.leader()
	tc.net.Disconnect(old.id)

	reply := make(chan string, 1)
	go func() { reply <- old.do("SET", "k", "from-old-leader") }()

	// Wait until the write is in the old leader's log, then let the others
	// elect a leader and commit a different entry at the same index.
	deadline := time.Now().Add(waitLimit)
	for old.raft.Status().LastLogIndex == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the old leader never appended the write")
		}
		time.Sleep(time.Millisecond)
	}
	next := tc.leader(tc.others(old.id)...)
	if got := next.do("SET", "k", "from-new-leader"); got != "+OK\r\n" {
		t.Fatalf("SET on the new leader = %q", got)
	}

	tc.net.Connect(old.id)
	select {
	case got := <-reply:
		// The client must not be told OK: its write was thrown away.
		// It is redirected to whoever leads by then, which under load may
		// no longer be the node that replaced it.
		if !strings.HasPrefix(got, "-MOVED 0 ") || strings.Contains(got, clientAddr(old.id)) {
			t.Fatalf("reply to the discarded write = %q, want a redirect to another node", got)
		}
	case <-time.After(waitLimit):
		for id, m := range tc.members {
			t.Logf("node %d: %+v", id, m.raft.Status())
		}
		t.Fatal("the client of the discarded write was never answered")
	}
	tc.eventually(old.id, "$15\r\nfrom-new-leader\r\n", "GET", "k")
}

func TestConcurrentClientsEachGetTheirOwnReply(t *testing.T) {
	tc := startCluster(t, 3)
	leader := tc.leader()

	const clients, perClient = 16, 25
	var mu sync.Mutex
	seen := make(map[string]bool)
	var wg sync.WaitGroup
	for range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perClient {
				got := leader.do("INCR", "counter")
				mu.Lock()
				if seen[got] {
					t.Errorf("two clients received the same INCR result %q", got)
				}
				seen[got] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	total := clients * perClient
	for i := 1; i <= total; i++ {
		if !seen[fmt.Sprintf(":%d\r\n", i)] {
			t.Fatalf("no client received INCR result %d", i)
		}
	}
	want := fmt.Sprintf("$%d\r\n%d\r\n", len(fmt.Sprint(total)), total)
	for _, id := range tc.others() {
		tc.eventually(id, want, "GET", "counter")
	}
}

func TestInfoReportsRoleAndLeader(t *testing.T) {
	tc := startCluster(t, 3)
	leader := tc.leader()
	follower := tc.members[tc.others(leader.id)[0]]

	info := leader.do("INFO", "cluster")
	for _, want := range []string{"cluster_enabled:1", "raft_role:leader", fmt.Sprintf("raft_node_id:%d", leader.id)} {
		if !strings.Contains(info, want) {
			t.Errorf("leader INFO lacks %q:\n%s", want, info)
		}
	}
	info = follower.do("INFO", "cluster")
	for _, want := range []string{"raft_role:follower", "raft_leader_addr:" + clientAddr(leader.id)} {
		if !strings.Contains(info, want) {
			t.Errorf("follower INFO lacks %q:\n%s", want, info)
		}
	}
}
