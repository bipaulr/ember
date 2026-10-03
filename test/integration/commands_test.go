package integration

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

var bg = context.Background()

// wantErr asserts the error text exactly as real Redis words it.
func wantErr(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

func TestStringsViaGoRedis(t *testing.T) {
	c, _ := inProc(t)

	if err := c.Set(bg, "k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if v, _ := c.Get(bg, "k").Result(); v != "v" {
		t.Fatal(v)
	}
	if _, err := c.Get(bg, "missing").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("missing key should be redis.Nil, got %v", err)
	}
	if ok, _ := c.SetNX(bg, "k", "other", 0).Result(); ok {
		t.Fatal("SETNX on existing key succeeded")
	}
	if ok, _ := c.SetNX(bg, "fresh", "1", 0).Result(); !ok {
		t.Fatal("SETNX on new key failed")
	}
	if ok, _ := c.SetXX(bg, "nope", "1", 0).Result(); ok {
		t.Fatal("SETXX on missing key succeeded")
	}
	if old, err := c.SetArgs(bg, "k", "v2", redis.SetArgs{Get: true}).Result(); err != nil || old != "v" {
		t.Fatalf("SET GET: %q %v", old, err)
	}
	if err := c.Set(bg, "ex", "v", 90*time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if d, _ := c.TTL(bg, "ex").Result(); d < 89*time.Second || d > 90*time.Second {
		t.Fatalf("TTL %v", d)
	}
	if err := c.Set(bg, "px", "v", 1500*time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	if d, _ := c.PTTL(bg, "px").Result(); d <= 0 || d > 1500*time.Millisecond {
		t.Fatalf("PTTL %v", d)
	}
	if n, _ := c.Incr(bg, "n").Result(); n != 1 {
		t.Fatal(n)
	}
	if n, _ := c.IncrBy(bg, "n", 9).Result(); n != 10 {
		t.Fatal(n)
	}
	if n, _ := c.Decr(bg, "n").Result(); n != 9 {
		t.Fatal(n)
	}
	if n, _ := c.DecrBy(bg, "n", 10).Result(); n != -1 {
		t.Fatal(n)
	}
	if n, _ := c.Append(bg, "k", "xyz").Result(); n != 5 {
		t.Fatal(n)
	}
	if n, _ := c.StrLen(bg, "k").Result(); n != 5 {
		t.Fatal(n)
	}
	if err := c.MSet(bg, "a", "1", "b", "2").Err(); err != nil {
		t.Fatal(err)
	}
	vals, _ := c.MGet(bg, "a", "missing", "b").Result()
	if len(vals) != 3 || vals[0] != "1" || vals[1] != nil || vals[2] != "2" {
		t.Fatalf("MGET %v", vals)
	}
	if v, _ := c.GetDel(bg, "a").Result(); v != "1" {
		t.Fatal(v)
	}
	if _, err := c.Get(bg, "a").Result(); !errors.Is(err, redis.Nil) {
		t.Fatal("GETDEL did not delete")
	}

	// binary-safe values
	bin := "a\x00b\r\nc\xff"
	c.Set(bg, "bin", bin, 0)
	if got, _ := c.Get(bg, "bin").Result(); got != bin {
		t.Fatalf("binary value corrupted: %q", got)
	}
	big := strings.Repeat("0123456789", 100_000) // 1 MB
	c.Set(bg, "big", big, 0)
	if got, _ := c.Get(bg, "big").Result(); got != big {
		t.Fatal("large value corrupted")
	}
}

func TestErrorRepliesMatchRedis(t *testing.T) {
	c, _ := inProc(t)
	c.Set(bg, "s", "str", 0)
	c.RPush(bg, "l", "x")
	c.Set(bg, "n", "notanumber", 0)

	cases := []struct {
		name string
		err  error
		want string
	}{
		{"GET on list", c.Get(bg, "l").Err(), "WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"LPUSH on string", c.LPush(bg, "s", "x").Err(), "WRONGTYPE Operation against a key holding the wrong kind of value"},
		{"INCR on text", c.Incr(bg, "n").Err(), "ERR value is not an integer or out of range"},
		{"INCRBY bad arg", c.Do(bg, "INCRBY", "x", "abc").Err(), "ERR value is not an integer or out of range"},
		{"GET arity", c.Do(bg, "GET").Err(), "ERR wrong number of arguments for 'get' command"},
		{"SET arity", c.Do(bg, "SET", "a").Err(), "ERR wrong number of arguments for 'set' command"},
		{"unknown", c.Do(bg, "FROBNICATE", "a", "b").Err(), "ERR unknown command 'FROBNICATE', with args beginning with: 'a' 'b' "},
		{"SET syntax", c.Do(bg, "SET", "a", "b", "NOPE").Err(), "ERR syntax error"},
		{"SET EX 0", c.Do(bg, "SET", "a", "b", "EX", "0").Err(), "ERR invalid expire time in 'set' command"},
		{"SELECT 1", c.Do(bg, "SELECT", "1").Err(), "ERR DB index is out of range"},
		{"EXPIRE not int", c.Do(bg, "EXPIRE", "s", "soon").Err(), "ERR value is not an integer or out of range"},
		{"LRANGE not int", c.Do(bg, "LRANGE", "l", "a", "b").Err(), "ERR value is not an integer or out of range"},
		{"overflow", func() error {
			c.Set(bg, "max", "9223372036854775807", 0)
			return c.Incr(bg, "max").Err()
		}(), "ERR increment or decrement would overflow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { wantErr(t, tc.err, tc.want) })
	}
}

func TestKeysViaGoRedis(t *testing.T) {
	c, _ := inProc(t)
	c.MSet(bg, "user:1", "a", "user:2", "b", "order:1", "c")
	c.RPush(bg, "list", "x")

	if n, _ := c.Exists(bg, "user:1", "user:2", "nope").Result(); n != 2 {
		t.Fatal(n)
	}
	keys, _ := c.Keys(bg, "user:*").Result()
	sort.Strings(keys)
	if fmt.Sprint(keys) != "[user:1 user:2]" {
		t.Fatal(keys)
	}
	if keys, _ := c.Keys(bg, "user:[12]").Result(); len(keys) != 2 {
		t.Fatal(keys)
	}
	if n, _ := c.DBSize(bg).Result(); n != 4 {
		t.Fatal(n)
	}
	if ty, _ := c.Type(bg, "list").Result(); ty != "list" {
		t.Fatal(ty)
	}
	if ty, _ := c.Type(bg, "user:1").Result(); ty != "string" {
		t.Fatal(ty)
	}
	if ty, _ := c.Type(bg, "none").Result(); ty != "none" {
		t.Fatal(ty)
	}
	if ok, _ := c.Expire(bg, "user:1", time.Minute).Result(); !ok {
		t.Fatal("EXPIRE")
	}
	if ok, _ := c.PExpire(bg, "user:2", time.Minute).Result(); !ok {
		t.Fatal("PEXPIRE")
	}
	if ok, _ := c.Persist(bg, "user:1").Result(); !ok {
		t.Fatal("PERSIST")
	}
	if d, _ := c.TTL(bg, "user:1").Result(); d != time.Duration(-1) {
		t.Fatalf("TTL after PERSIST: %v", d)
	}
	if d, _ := c.TTL(bg, "nope").Result(); d != time.Duration(-2) {
		t.Fatalf("TTL missing: %v", d)
	}
	if n, _ := c.Del(bg, "user:1", "user:2", "ghost").Result(); n != 2 {
		t.Fatal(n)
	}
	if err := c.FlushAll(bg).Err(); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.DBSize(bg).Result(); n != 0 {
		t.Fatal(n)
	}
}

func TestListsViaGoRedis(t *testing.T) {
	c, _ := inProc(t)
	if n, _ := c.LPush(bg, "l", "a", "b", "c").Result(); n != 3 {
		t.Fatal(n)
	}
	if n, _ := c.RPush(bg, "l", "x").Result(); n != 4 {
		t.Fatal(n)
	}
	got, _ := c.LRange(bg, "l", 0, -1).Result()
	if fmt.Sprint(got) != "[c b a x]" {
		t.Fatal(got)
	}
	if n, _ := c.LLen(bg, "l").Result(); n != 4 {
		t.Fatal(n)
	}
	if v, _ := c.LPop(bg, "l").Result(); v != "c" {
		t.Fatal(v)
	}
	if v, _ := c.RPop(bg, "l").Result(); v != "x" {
		t.Fatal(v)
	}
	if vs, _ := c.LPopCount(bg, "l", 5).Result(); fmt.Sprint(vs) != "[b a]" {
		t.Fatal(vs)
	}
	if _, err := c.LPop(bg, "l").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("empty LPOP: %v", err)
	}
	if n, _ := c.Exists(bg, "l").Result(); n != 0 {
		t.Fatal("empty list should be deleted")
	}
}

func TestServerCommandsViaGoRedis(t *testing.T) {
	c, _ := inProc(t)
	if pong, _ := c.Ping(bg).Result(); pong != "PONG" {
		t.Fatal(pong)
	}
	if s, _ := c.Echo(bg, "hi").Result(); s != "hi" {
		t.Fatal(s)
	}
	info, err := c.Info(bg).Result()
	if err != nil || !strings.Contains(info, "# Server") || !strings.Contains(info, "# Keyspace") {
		t.Fatalf("%v %s", err, info)
	}
	if _, err := c.Command(bg).Result(); err != nil {
		t.Fatalf("COMMAND: %v", err)
	}
	if err := c.Do(bg, "COMMAND", "DOCS").Err(); err != nil {
		t.Fatalf("COMMAND DOCS: %v", err)
	}
	if err := c.Do(bg, "SELECT", "0").Err(); err != nil {
		t.Fatal(err)
	}
}

// TestActiveExpiryEndToEnd: a key nobody reads still disappears.
func TestActiveExpiryEndToEnd(t *testing.T) {
	c, env := inProc(t)
	for i := 0; i < 200; i++ {
		c.Set(bg, fmt.Sprintf("k%d", i), "v", 100*time.Millisecond)
	}
	c.Set(bg, "keeper", "v", 0)
	deadline := time.Now().Add(5 * time.Second)
	for env.Store.Len() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("active expiry left %d keys", env.Store.Len())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if env.Store.ExpiredKeys() != 200 {
		t.Fatalf("expired=%d", env.Store.ExpiredKeys())
	}
}

func TestLazyExpiryEndToEnd(t *testing.T) {
	c, _ := inProc(t)
	c.Set(bg, "k", "v", 50*time.Millisecond)
	time.Sleep(80 * time.Millisecond)
	// Whether the sweeper or the read removes it, the client sees it gone.
	if _, err := c.Get(bg, "k").Result(); !errors.Is(err, redis.Nil) {
		t.Fatalf("expired key still readable: %v", err)
	}
}

// TestStress hammers overlapping keys from 100 goroutines. SET/GET/DEL churn on
// shared keys while INCR counters must come out exact.
func TestStress(t *testing.T) {
	c, _ := inProc(t)
	const goroutines, iters, counters = 100, 400, 5
	var wg sync.WaitGroup
	perCounter := make([]int64, counters)
	var mu sync.Mutex
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			local := make([]int64, counters)
			for i := 0; i < iters; i++ {
				shared := fmt.Sprintf("shared:%d", (g+i)%20)
				ctr := (g * i) % counters
				switch i % 5 {
				case 0, 1:
					if _, err := c.Incr(bg, fmt.Sprintf("counter:%d", ctr)).Result(); err != nil {
						errs <- err
						return
					}
					local[ctr]++
				case 2:
					if err := c.Set(bg, shared, i, 0).Err(); err != nil {
						errs <- err
						return
					}
				case 3:
					if err := c.Get(bg, shared).Err(); err != nil && !errors.Is(err, redis.Nil) {
						errs <- err
						return
					}
				case 4:
					if err := c.Del(bg, shared).Err(); err != nil {
						errs <- err
						return
					}
				}
			}
			mu.Lock()
			for k, v := range local {
				perCounter[k] += v
			}
			mu.Unlock()
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var total int64
	for k := 0; k < counters; k++ {
		got, err := c.Get(bg, fmt.Sprintf("counter:%d", k)).Int64()
		if err != nil || got != perCounter[k] {
			t.Fatalf("counter:%d = %d (%v), want exactly %d", k, got, err, perCounter[k])
		}
		total += got
	}
	if want := int64(goroutines * iters * 2 / 5); total != want {
		t.Fatalf("total INCRs %d, want %d", total, want)
	}
}

func TestPipelineViaGoRedis(t *testing.T) {
	c, _ := inProc(t)
	pipe := c.Pipeline()
	const n = 5000
	cmds := make([]*redis.IntCmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = pipe.Incr(bg, "p")
	}
	if _, err := pipe.Exec(bg); err != nil {
		t.Fatal(err)
	}
	for i, cmd := range cmds {
		if cmd.Val() != int64(i+1) {
			t.Fatalf("reply %d = %d", i, cmd.Val())
		}
	}
}
