package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const totalKeys = 10000

func keyName(i int) string { return fmt.Sprintf("key:%05d", i) }

// TestCrashRecoveryAlways is the central durability test. With
// appendfsync=always, a write that was acknowledged must survive a hard kill.
// It writes 10,000 keys (half with TTLs), kills the process with no graceful
// shutdown, restarts it on the same file, and checks every key and TTL.
func TestCrashRecoveryAlways(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.aof")
	ctx := context.Background()
	p := startProc(t, path, "always")
	c := p.client()

	for batch := 0; batch < totalKeys/100; batch++ {
		pipe := c.Pipeline()
		for j := 0; j < 100; j++ {
			i := batch*100 + j
			ttl := time.Duration(0)
			if i%2 == 0 {
				ttl = time.Hour + time.Duration(i)*time.Second
			}
			pipe.Set(ctx, keyName(i), fmt.Sprintf("value-%d", i), ttl)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("batch %d: %v", batch, err)
		}
	}
	// A few non-string and non-idempotent commands as well.
	for i := 0; i < 250; i++ {
		if err := c.Incr(ctx, "counter").Err(); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		if err := c.RPush(ctx, "list", i).Err(); err != nil {
			t.Fatal(err)
		}
	}
	c.LPop(ctx, "list")
	c.Del(ctx, keyName(1))
	c.Expire(ctx, keyName(3), 2*time.Hour)
	_ = c.Close()

	p.kill() // no SIGTERM, no flush: the process just stops

	p2 := startProc(t, path, "always")
	c2 := p2.client()
	defer func() { _ = c2.Close() }()

	if n, _ := c2.DBSize(ctx).Result(); n != totalKeys+2-1 { // keys + counter + list - deleted key
		t.Fatalf("DBSIZE after crash = %d, want %d", n, totalKeys+1)
	}
	lost := 0
	for i := 0; i < totalKeys; i++ {
		if i == 1 {
			continue
		}
		v, err := c2.Get(ctx, keyName(i)).Result()
		if err != nil || v != fmt.Sprintf("value-%d", i) {
			lost++
			if lost <= 5 {
				t.Errorf("%s: %q, %v", keyName(i), v, err)
			}
			continue
		}
	}
	if lost != 0 {
		t.Fatalf("%d of %d keys lost with appendfsync=always", lost, totalKeys)
	}
	if _, err := c2.Get(ctx, keyName(1)).Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("deleted key came back")
	}
	// TTLs: even keys keep a deadline close to the original, odd keys have none.
	for _, i := range []int{0, 2, 4, 5000, 9998} {
		d, _ := c2.PTTL(ctx, keyName(i)).Result()
		want := time.Hour + time.Duration(i)*time.Second
		if d <= want-time.Minute || d > want {
			t.Errorf("%s TTL = %v, want just under %v", keyName(i), d, want)
		}
	}
	for _, i := range []int{5, 7, 9999} {
		if d, _ := c2.PTTL(ctx, keyName(i)).Result(); d != time.Duration(-1) {
			t.Errorf("%s should have no TTL, got %v", keyName(i), d)
		}
	}
	if d, _ := c2.TTL(ctx, keyName(3)).Result(); d < 119*time.Minute || d > 2*time.Hour {
		t.Errorf("EXPIRE after crash: %v", d)
	}
	if v, _ := c2.Get(ctx, "counter").Int(); v != 250 {
		t.Errorf("counter = %d, want 250", v)
	}
	if n, _ := c2.LLen(ctx, "list").Result(); n != 99 {
		t.Errorf("list length = %d, want 99", n)
	}
	t.Logf("all %d keys, their TTLs, and the counter and list survived kill -9", totalKeys)
}

// TestCrashRecoveryEverysec writes sequentially, kills the process mid-stream,
// and measures what was lost. The contract is at most about one second of
// writes. Killing the process (as opposed to the machine) does not lose data
// that already reached the OS page cache, so zero loss is the expected result
// here; the test asserts the contractual bound.
func TestCrashRecoveryEverysec(t *testing.T) {
	path := filepath.Join(t.TempDir(), "crash.aof")
	ctx := context.Background()
	p := startProc(t, path, "everysec")
	c := p.client()

	var acked atomic.Int64 // highest index whose SET was acknowledged, plus one
	stopped := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(stopped)
		for i := 0; ; i++ {
			if err := c.Set(ctx, fmt.Sprintf("w:%d", i), i, 0).Err(); err != nil {
				return
			}
			acked.Store(int64(i) + 1)
		}
	}()
	time.Sleep(2500 * time.Millisecond)
	elapsed := time.Since(started)
	p.kill()
	<-stopped
	_ = c.Close()
	total := acked.Load()
	if total < 100 {
		t.Fatalf("only %d writes acknowledged", total)
	}

	p2 := startProc(t, path, "everysec")
	c2 := p2.client()
	defer func() { _ = c2.Close() }()
	present := int64(0)
	for present < total+1000 {
		n, _ := c2.Exists(ctx, fmt.Sprintf("w:%d", present)).Result()
		if n == 0 {
			break
		}
		present++
	}
	// Ordered log: the survivors must be a prefix with no holes.
	if n, _ := c2.DBSize(ctx).Result(); n != present {
		t.Fatalf("holes in recovered data: prefix=%d but DBSIZE=%d", present, n)
	}
	lost := total - present
	if lost < 0 {
		lost = 0 // the server may have applied a write whose ack was cut off
	}
	rate := float64(total) / elapsed.Seconds()
	bound := int64(rate*1.1) + 10
	t.Logf("acknowledged=%d recovered=%d lost=%d (rate %.0f writes/s, allowed about %d)", total, present, lost, rate, bound)
	if lost > bound {
		t.Fatalf("lost %d writes, more than about one second (%d)", lost, bound)
	}
}

func TestTruncatedTailOnRealBinary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.aof")
	ctx := context.Background()
	p := startProc(t, path, "always")
	c := p.client()
	for i := 0; i < 100; i++ {
		if err := c.Set(ctx, keyName(i), "v", 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	_ = c.Close()
	p.kill()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("*3\r\n$3\r\nSET\r\n$7\r\npartial\r\n$10\r\nabc") // crash mid-write
	_ = f.Close()

	p2 := startProc(t, path, "always")
	c2 := p2.client()
	if n, _ := c2.DBSize(ctx).Result(); n != 100 {
		t.Fatalf("DBSIZE=%d, want 100", n)
	}
	if !strings.Contains(p2.out.String(), "partial entry") {
		t.Fatalf("expected a truncation warning in the log:\n%s", p2.out.String())
	}
	if err := c2.Set(ctx, "after", "x", 0).Err(); err != nil {
		t.Fatal(err)
	}
	_ = c2.Close()
	p2.kill()

	p3 := startProc(t, path, "always")
	c3 := p3.client()
	defer func() { _ = c3.Close() }()
	if n, _ := c3.DBSize(ctx).Result(); n != 101 {
		t.Fatalf("after repair and more writes DBSIZE=%d, want 101", n)
	}
}

func TestCorruptAOFFailsFast(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.aof")
	content := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n" + "*1\r\n:99\r\n" + "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	p := startProcNoWait(t, path, "everysec")
	select {
	case <-p.done:
	case <-time.After(10 * time.Second):
		t.Fatal("server should refuse to start on a corrupt AOF")
	}
	if p.err == nil {
		t.Fatal("expected a non-zero exit status")
	}
	out := p.out.String()
	if !strings.Contains(out, "corrupt") || !strings.Contains(out, "byte offset") {
		t.Fatalf("error should name the problem and offset:\n%s", out)
	}
	if after, _ := os.ReadFile(path); string(after) != content {
		t.Fatal("corrupt file must not be modified")
	}
}

func TestRewriteOverTheWire(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rw.aof")
	ctx := context.Background()
	p := startProc(t, path, "everysec")
	c := p.client()
	for i := 0; i < 3000; i++ {
		c.Incr(ctx, "counter")
	}
	c.Set(ctx, "ttl", "v", time.Hour)
	before, _ := os.Stat(path)

	if got, err := c.Do(ctx, "BGREWRITEAOF").Text(); err != nil || !strings.Contains(got, "rewriting started") {
		t.Fatalf("BGREWRITEAOF: %q %v", got, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, _ := c.Info(ctx, "persistence").Result()
		if strings.Contains(info, "aof_rewrite_in_progress:0") && strings.Contains(info, "aof_rewrites:1") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("rewrite did not finish:\n%s", info)
		}
		time.Sleep(20 * time.Millisecond)
	}
	after, _ := os.Stat(path)
	if after.Size() >= before.Size()/10 {
		t.Fatalf("AOF did not shrink: %d -> %d", before.Size(), after.Size())
	}
	c.Incr(ctx, "counter") // lands in the new file
	_ = c.Close()
	p.kill()

	p2 := startProc(t, path, "everysec")
	c2 := p2.client()
	defer func() { _ = c2.Close() }()
	if v, _ := c2.Get(ctx, "counter").Int(); v != 3001 {
		t.Fatalf("counter after rewrite+crash = %d, want 3001", v)
	}
	if d, _ := c2.TTL(ctx, "ttl").Result(); d < 59*time.Minute {
		t.Fatalf("ttl lost in rewrite: %v", d)
	}
	t.Logf("AOF shrank from %d to %d bytes", before.Size(), after.Size())
}
