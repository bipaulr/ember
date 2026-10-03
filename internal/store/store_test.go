package store

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ ms atomic.Int64 }

func (c *fakeClock) now() int64 { return c.ms.Load() }

func newTest(shards int) (*Store, *fakeClock) {
	c := &fakeClock{}
	c.ms.Store(1000)
	return NewWithClock(shards, c.now), c
}

func put(s *Store, key, val string, expireAt int64) {
	s.Update(key, func(tx *Tx) {
		tx.Put(&Entry{Kind: KindString, Str: []byte(val), ExpireAt: expireAt})
	})
}

func get(s *Store, key string) (string, bool) {
	var v string
	var ok bool
	s.View(key, func(e *Entry) {
		if e != nil {
			v, ok = string(e.Str), true
		}
	})
	return v, ok
}

func TestPutGetDelete(t *testing.T) {
	s, _ := newTest(4)
	put(s, "a", "1", 0)
	if v, ok := get(s, "a"); !ok || v != "1" {
		t.Fatalf("got %q %v", v, ok)
	}
	if _, ok := get(s, "missing"); ok {
		t.Fatal("missing key found")
	}
	var deleted, again bool
	s.Update("a", func(tx *Tx) { deleted = tx.Delete() })
	s.Update("a", func(tx *Tx) { again = tx.Delete() })
	if !deleted || again {
		t.Fatalf("deleted=%v again=%v", deleted, again)
	}
	if s.Len() != 0 {
		t.Fatal("expected empty store")
	}
}

func TestDefaultShardsAndKindString(t *testing.T) {
	if New(0).NumShards() != DefaultShards {
		t.Fatal("default shards")
	}
	if KindString.String() != "string" || KindList.String() != "list" || Kind(0).String() != "none" {
		t.Fatal("kind names")
	}
}

func TestHashSpreadsKeys(t *testing.T) {
	s := New(16)
	counts := make([]int, 16)
	for i := 0; i < 16000; i++ {
		counts[s.shardIndex(fmt.Sprintf("key:%d", i))]++
	}
	for i, c := range counts {
		if c < 700 || c > 1300 {
			t.Fatalf("shard %d has %d keys, expected about 1000", i, c)
		}
	}
}

func TestLazyExpiry(t *testing.T) {
	s, clk := newTest(4)
	var logged [][]string
	s.SetPropagator(func(shard int, args [][]byte) {
		var a []string
		for _, b := range args {
			a = append(a, string(b))
		}
		logged = append(logged, a)
	})
	put(s, "k", "v", 2000)
	if _, ok := get(s, "k"); !ok {
		t.Fatal("should be live")
	}
	clk.ms.Store(2000)
	if _, ok := get(s, "k"); ok {
		t.Fatal("should be expired")
	}
	if s.Len() != 0 || s.TTLKeys() != 0 {
		t.Fatalf("len=%d ttl=%d", s.Len(), s.TTLKeys())
	}
	if s.ExpiredKeys() != 1 {
		t.Fatalf("expired=%d", s.ExpiredKeys())
	}
	if len(logged) != 1 || logged[0][0] != "DEL" || logged[0][1] != "k" {
		t.Fatalf("expected DEL to be propagated, got %v", logged)
	}
}

func TestSetExpireAndTTLIndex(t *testing.T) {
	s, _ := newTest(2)
	put(s, "a", "1", 0)
	s.Update("a", func(tx *Tx) { tx.SetExpire(tx.Entry(), 5000) })
	if s.TTLKeys() != 1 {
		t.Fatal("ttl not tracked")
	}
	s.Update("a", func(tx *Tx) { tx.SetExpire(tx.Entry(), 6000) }) // change, still one
	if s.TTLKeys() != 1 {
		t.Fatal("ttl double counted")
	}
	s.Update("a", func(tx *Tx) { tx.SetExpire(tx.Entry(), 0) })
	if s.TTLKeys() != 0 {
		t.Fatal("persist should untrack")
	}
	// Overwriting a key with a TTL via Put drops it from the index.
	put(s, "b", "1", 9000)
	put(s, "b", "2", 0)
	if s.TTLKeys() != 0 {
		t.Fatal("put should clear ttl index")
	}
	// Deleting a key with a TTL untracks it, and swap-remove keeps others valid.
	for i := 0; i < 5; i++ {
		put(s, fmt.Sprintf("t%d", i), "x", 9000)
	}
	s.Update("t2", func(tx *Tx) { tx.Delete() })
	if s.TTLKeys() != 4 {
		t.Fatalf("ttl=%d", s.TTLKeys())
	}
	for _, k := range []string{"t0", "t1", "t3", "t4"} {
		s.Update(k, func(tx *Tx) { tx.SetExpire(tx.Entry(), 0) })
	}
	if s.TTLKeys() != 0 {
		t.Fatal("index corrupted by swap-remove")
	}
}

func TestActiveExpiry(t *testing.T) {
	s, clk := newTest(16)
	for i := 0; i < 1000; i++ {
		put(s, fmt.Sprintf("short:%d", i), "x", 2000)
	}
	for i := 0; i < 100; i++ {
		put(s, fmt.Sprintf("long:%d", i), "x", 1_000_000)
	}
	put(s, "forever", "x", 0)
	if n := s.ExpireCycle(20, time.Second); n != 0 {
		t.Fatalf("nothing is expired yet, deleted %d", n)
	}
	clk.ms.Store(2500)
	total := 0
	for i := 0; i < 200 && total < 1000; i++ {
		total += s.ExpireCycle(20, time.Second)
	}
	if total != 1000 {
		t.Fatalf("active expiry removed %d of 1000 expired keys", total)
	}
	if s.Len() != 101 {
		t.Fatalf("len=%d, want 101", s.Len())
	}
	if _, ok := get(s, "forever"); !ok {
		t.Fatal("key without TTL must survive")
	}
}

func TestActiveExpiryRepeatsOnHighHitRate(t *testing.T) {
	s, clk := newTest(4)
	for i := 0; i < 5000; i++ {
		put(s, fmt.Sprintf("k%d", i), "x", 1500)
	}
	clk.ms.Store(2000)
	// A single cycle must keep sampling because every sample is expired.
	if n := s.ExpireCycle(20, time.Second); n != 5000 {
		t.Fatalf("one cycle removed %d, want all 5000", n)
	}
}

func TestExpireCycleBudget(t *testing.T) {
	s, clk := newTest(4)
	for i := 0; i < 2000; i++ {
		put(s, fmt.Sprintf("k%d", i), "x", 1500)
	}
	clk.ms.Store(2000)
	n := s.ExpireCycle(20, 0) // zero budget: stop after the first sample
	if n == 0 || n > 20 {
		t.Fatalf("removed %d with no budget", n)
	}
}

func TestRunExpiry(t *testing.T) {
	s, clk := newTest(4)
	put(s, "k", "x", 1500)
	clk.ms.Store(2000)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() { s.RunExpiry(5*time.Millisecond, stop); close(done) }()
	deadline := time.After(2 * time.Second)
	for s.Len() != 0 {
		select {
		case <-deadline:
			t.Fatal("background expiry never removed the key")
		case <-time.After(5 * time.Millisecond):
		}
	}
	close(stop)
	<-done
}

func TestKeysSkipsExpired(t *testing.T) {
	s, clk := newTest(4)
	put(s, "live", "1", 0)
	put(s, "dead", "1", 1500)
	clk.ms.Store(2000)
	keys := s.Keys(func(string) bool { return true })
	if len(keys) != 1 || keys[0] != "live" {
		t.Fatalf("keys=%v", keys)
	}
}

func TestFlushAll(t *testing.T) {
	s, _ := newTest(4)
	var logged []int
	s.SetPropagator(func(shard int, args [][]byte) { logged = append(logged, shard) })
	for i := 0; i < 50; i++ {
		put(s, fmt.Sprintf("k%d", i), "x", int64(5000+i))
	}
	s.FlushAll()
	if s.Len() != 0 || s.TTLKeys() != 0 {
		t.Fatal("flush incomplete")
	}
	if len(logged) != 1 || logged[0] != -1 {
		t.Fatalf("FLUSHALL should be logged once for all shards, got %v", logged)
	}
	put(s, "after", "1", 0)
	if _, ok := get(s, "after"); !ok {
		t.Fatal("store unusable after flush")
	}
}

func TestSnapshotShard(t *testing.T) {
	s, clk := newTest(1)
	put(s, "str", "val", 0)
	put(s, "ttl", "val", 9000)
	put(s, "dead", "val", 1500)
	s.Update("list", func(tx *Tx) {
		l := NewList()
		for i := 0; i < 1100; i++ {
			l.PushBack([]byte("x"))
		}
		tx.Put(&Entry{Kind: KindList, List: l})
	})
	clk.ms.Store(2000)
	marked := false
	cmds := map[string]int{}
	s.SnapshotShard(0, func() { marked = true }, func(args [][]byte) {
		if !marked {
			t.Fatal("mark must run before emit")
		}
		cmds[string(args[0])+" "+string(args[1])]++
	})
	if cmds["SET str"] != 1 || cmds["SET ttl"] != 1 || cmds["PEXPIREAT ttl"] != 1 {
		t.Fatalf("cmds=%v", cmds)
	}
	if cmds["SET dead"] != 0 {
		t.Fatal("expired key must not be snapshotted")
	}
	if cmds["RPUSH list"] != 3 { // 1100 items in chunks of 512
		t.Fatalf("list chunks: %v", cmds)
	}
}

func TestItoa(t *testing.T) {
	for _, n := range []int64{0, 7, -7, 1234567890123, -1 << 63, 1<<63 - 1} {
		if got, want := string(itoa(n)), fmt.Sprint(n); got != want {
			t.Errorf("itoa(%d) = %s", n, got)
		}
	}
}

func TestListOps(t *testing.T) {
	l := NewList()
	for i := 0; i < 20; i++ {
		l.PushBack([]byte{byte(i)})
	}
	for i := 0; i < 10; i++ {
		l.PushFront([]byte{byte(100 + i)})
	}
	if l.Len() != 30 || l.At(0)[0] != 109 || l.At(29)[0] != 19 || l.At(10)[0] != 0 {
		t.Fatal("ordering wrong")
	}
	if l.PopFront()[0] != 109 || l.PopBack()[0] != 19 || l.Len() != 28 {
		t.Fatal("pop wrong")
	}
	for l.Len() > 0 {
		l.PopFront()
	}
	l.PushFront([]byte("x"))
	if string(l.At(0)) != "x" {
		t.Fatal("reuse after drain")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New(16)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := fmt.Sprintf("k%d", i%50)
				s.Update(k, func(tx *Tx) {
					tx.Put(&Entry{Kind: KindString, Str: []byte("v"), ExpireAt: time.Now().UnixMilli() + int64(i%3)})
				})
				get(s, k)
				if i%7 == 0 {
					s.Update(k, func(tx *Tx) { tx.Delete() })
				}
			}
		}(g)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			s.ExpireCycle(20, time.Millisecond)
			s.Keys(func(string) bool { return true })
		}
	}()
	wg.Wait()
}
