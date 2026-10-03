package pubsub

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func newSub() *Subscriber { return NewSubscriber(16, 1<<20, nil) }

func drain(s *Subscriber) []string {
	var out []string
	for {
		select {
		case m := <-s.C:
			out = append(out, string(m))
			s.Release(len(m))
		default:
			return out
		}
	}
}

func TestPublishToChannel(t *testing.T) {
	h := NewHub()
	a, b := newSub(), newSub()
	if n := h.Subscribe(a, "news"); n != 1 {
		t.Fatal(n)
	}
	h.Subscribe(b, "news")
	h.Subscribe(b, "other")
	if n := h.Publish("news", []byte("hello")); n != 2 {
		t.Fatalf("receivers = %d", n)
	}
	if n := h.Publish("nobody", []byte("x")); n != 0 {
		t.Fatalf("receivers = %d", n)
	}
	want := "*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
	if got := drain(a); len(got) != 1 || got[0] != want {
		t.Fatalf("got %q", got)
	}
	if got := drain(b); len(got) != 1 || got[0] != want {
		t.Fatalf("got %q", got)
	}
	if h.Published.Load() != 2 {
		t.Fatal("published counter")
	}
}

func TestPatterns(t *testing.T) {
	h := NewHub()
	s := newSub()
	h.PSubscribe(s, "news.*")
	h.Subscribe(s, "news.sports") // matches both a channel and a pattern
	if n := h.Publish("news.sports", []byte("goal")); n != 2 {
		t.Fatalf("receivers = %d, want 2 (channel + pattern)", n)
	}
	if n := h.Publish("weather", []byte("rain")); n != 0 {
		t.Fatal(n)
	}
	got := drain(s)
	if len(got) != 2 {
		t.Fatalf("got %q", got)
	}
	var sawP bool
	for _, m := range got {
		if m == "*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$11\r\nnews.sports\r\n$4\r\ngoal\r\n" {
			sawP = true
		}
	}
	if !sawP {
		t.Fatalf("pmessage not encoded as expected: %q", got)
	}
	if h.NumPat() != 1 {
		t.Fatal("NumPat")
	}
}

func TestUnsubscribeAndCounts(t *testing.T) {
	h := NewHub()
	s := newSub()
	h.Subscribe(s, "a")
	h.Subscribe(s, "b")
	h.Subscribe(s, "a") // idempotent
	h.PSubscribe(s, "p*")
	if s.Count() != 3 {
		t.Fatalf("count=%d", s.Count())
	}
	if fmt.Sprint(s.Channels()) != "[a b]" || fmt.Sprint(s.Patterns()) != "[p*]" {
		t.Fatal(s.Channels(), s.Patterns())
	}
	if n := h.Unsubscribe(s, "a"); n != 2 {
		t.Fatal(n)
	}
	if n := h.Unsubscribe(s, "a"); n != 2 { // not subscribed any more: harmless
		t.Fatal(n)
	}
	if n := h.PUnsubscribe(s, "p*"); n != 1 {
		t.Fatal(n)
	}
	if h.Publish("a", nil) != 0 {
		t.Fatal("still subscribed to a")
	}
	if h.NumSub("b") != 1 || h.NumChannels() != 1 {
		t.Fatal("registry wrong")
	}
	if fmt.Sprint(h.Channels("")) != "[b]" || len(h.Channels("x*")) != 0 || fmt.Sprint(h.Channels("?")) != "[b]" {
		t.Fatal("Channels")
	}
}

func TestRemoveAllLeavesHubEmpty(t *testing.T) {
	h := NewHub()
	s := newSub()
	for i := 0; i < 50; i++ {
		h.Subscribe(s, fmt.Sprint("c", i))
		h.PSubscribe(s, fmt.Sprint("p", i, "*"))
	}
	h.RemoveAll(s)
	if h.NumChannels() != 0 || h.NumPat() != 0 || s.Count() != 0 {
		t.Fatalf("channels=%d patterns=%d count=%d", h.NumChannels(), h.NumPat(), s.Count())
	}
}

func TestOverflowByMessageCount(t *testing.T) {
	h := NewHub()
	var kicked atomic.Int32
	slow := NewSubscriber(3, 1<<20, func() { kicked.Add(1) })
	fast := newSub()
	h.Subscribe(slow, "c")
	h.Subscribe(fast, "c")
	for i := 0; i < 3; i++ {
		if n := h.Publish("c", []byte("m")); n != 2 {
			t.Fatalf("publish %d reached %d", i, n)
		}
	}
	// The slow subscriber's queue (3) is full: the next publish drops it but
	// still reaches the healthy one.
	if n := h.Publish("c", []byte("m")); n != 1 {
		t.Fatalf("receivers = %d, want 1", n)
	}
	if !slow.Overflowed() || kicked.Load() != 1 {
		t.Fatalf("overflowed=%v kicked=%d", slow.Overflowed(), kicked.Load())
	}
	h.Publish("c", []byte("m"))
	if kicked.Load() != 1 {
		t.Fatal("onOverflow must fire once")
	}
	if len(drain(fast)) != 5 {
		t.Fatal("healthy subscriber lost messages")
	}
}

func TestOverflowByBytes(t *testing.T) {
	h := NewHub()
	var kicked atomic.Int32
	s := NewSubscriber(1000, 1000, func() { kicked.Add(1) })
	h.Subscribe(s, "c")
	big := make([]byte, 600)
	if n := h.Publish("c", big); n != 1 {
		t.Fatal(n)
	}
	if n := h.Publish("c", big); n != 0 || kicked.Load() != 1 {
		t.Fatalf("second 600-byte message should exceed the 1000-byte budget (n=%d kicked=%d)", n, kicked.Load())
	}
}

func TestReleaseRestoresBudget(t *testing.T) {
	h := NewHub()
	s := NewSubscriber(100, 1000, nil)
	h.Subscribe(s, "c")
	big := make([]byte, 400)
	for i := 0; i < 20; i++ {
		if n := h.Publish("c", big); n != 1 {
			t.Fatalf("iteration %d: dropped although the consumer keeps up", i)
		}
		drain(s)
	}
}

func TestCloseAndDefaults(t *testing.T) {
	s := NewSubscriber(0, 0, nil)
	if cap(s.C) != DefaultQueueMessages || s.maxBytes != DefaultQueueBytes {
		t.Fatal("defaults not applied")
	}
	s.Close()
	s.Close()
	select {
	case <-s.Done():
	default:
		t.Fatal("Done not closed")
	}
}

func TestEncodeBinaryPayload(t *testing.T) {
	got := string(encode(nil, "message", "", "c", []byte("a\r\nb")))
	if !strings.HasSuffix(got, "$4\r\na\r\nb\r\n") {
		t.Fatalf("%q", got)
	}
}

func TestConcurrentPublishSubscribe(t *testing.T) {
	h := NewHub()
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			s := NewSubscriber(8, 1<<16, func() {})
			for i := 0; i < 500; i++ {
				ch := fmt.Sprint("c", i%5)
				h.Subscribe(s, ch)
				h.PSubscribe(s, "c*")
				h.Publish(ch, []byte("x"))
				drain(s)
				if i%3 == 0 {
					h.Unsubscribe(s, ch)
				}
			}
			h.RemoveAll(s)
		}(g)
	}
	wg.Wait()
	if h.NumChannels() != 0 || h.NumPat() != 0 {
		t.Fatal("leftover subscriptions")
	}
}
