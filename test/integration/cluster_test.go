package integration

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// clusterProcs starts n ember processes that form one Raft cluster, talking
// to each other over real TCP.
func clusterProcs(t *testing.T, n int) []*proc {
	t.Helper()
	var members []string
	clientAddrs := make([]string, n)
	for i := 0; i < n; i++ {
		raftAddr := "127.0.0.1:" + strconv.Itoa(freePort(t))
		clientAddrs[i] = "127.0.0.1:" + strconv.Itoa(freePort(t))
		members = append(members, fmt.Sprintf("%d=%s/%s", i+1, raftAddr, clientAddrs[i]))
	}
	spec := strings.Join(members, ",")

	procs := make([]*proc, n)
	for i := 0; i < n; i++ {
		host, port, _ := strings.Cut(clientAddrs[i], ":")
		p := &proc{t: t, addr: clientAddrs[i], out: &syncBuf{}, done: make(chan struct{})}
		p.cmd = exec.Command(binPath,
			"--host", host, "--port", port,
			"--node-id", strconv.Itoa(i+1), "--cluster", spec,
			"--election-timeout", "200ms", "--heartbeat-interval", "40ms")
		p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
		if err := p.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		go func() { p.err = p.cmd.Wait(); close(p.done) }()
		t.Cleanup(p.kill)
		procs[i] = p
	}
	return procs
}

// findLeader polls INFO on the given processes until one reports itself leader.
func findLeader(t *testing.T, procs []*proc) *proc {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, p := range procs {
			c := p.client()
			info, err := c.Info(ctx, "cluster").Result()
			_ = c.Close()
			if err == nil && strings.Contains(info, "raft_role:leader") {
				return p
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	for _, p := range procs {
		t.Logf("%s output:\n%s", p.addr, p.out.String())
	}
	t.Fatal("no process became leader")
	return nil
}

func without(procs []*proc, skip *proc) []*proc {
	var out []*proc
	for _, p := range procs {
		if p != skip {
			out = append(out, p)
		}
	}
	return out
}

func eventuallyGet(t *testing.T, c *redis.Client, key, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var got string
	var err error
	for time.Now().Before(deadline) {
		if got, err = c.Get(context.Background(), key).Result(); err == nil && got == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("GET %s = %q, %v; want %q", key, got, err, want)
}

func TestClusterOfRealProcesses(t *testing.T) {
	ctx := context.Background()
	procs := clusterProcs(t, 3)
	leader := findLeader(t, procs)
	followers := without(procs, leader)

	lc := leader.client()
	defer func() { _ = lc.Close() }()
	if err := lc.Set(ctx, "greeting", "hello", 0).Err(); err != nil {
		t.Fatalf("SET on the leader: %v", err)
	}
	if n, err := lc.Incr(ctx, "counter").Result(); err != nil || n != 1 {
		t.Fatalf("INCR on the leader = %d, %v", n, err)
	}

	// The write reaches every node.
	for _, f := range followers {
		fc := f.client()
		eventuallyGet(t, fc, "greeting", "hello")
		_ = fc.Close()
	}

	// A follower refuses writes and names the leader.
	fc := followers[0].client()
	defer func() { _ = fc.Close() }()
	err := fc.Set(ctx, "x", "y", 0).Err()
	if want := "MOVED 0 " + leader.addr; err == nil || err.Error() != want {
		t.Fatalf("SET on a follower: err = %v, want %q", err, want)
	}

	// Kill the leader. The two survivors elect a new one, keep the data, and
	// accept new writes.
	leader.kill()
	next := findLeader(t, followers)
	nc := next.client()
	defer func() { _ = nc.Close() }()
	eventuallyGet(t, nc, "greeting", "hello")
	if n, err := nc.Incr(ctx, "counter").Result(); err != nil || n != 2 {
		t.Fatalf("INCR on the new leader = %d, %v; want 2", n, err)
	}
}
