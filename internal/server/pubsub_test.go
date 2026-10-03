package server

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ember/internal/command"
	"ember/internal/resp"
	"ember/internal/store"
)

// psClient is a minimal raw RESP client for pub/sub tests.
type psClient struct {
	t    *testing.T
	conn net.Conn
	r    *resp.Reader
}

func newPS(t *testing.T, addr string) *psClient {
	t.Helper()
	conn := dial(t, addr)
	return &psClient{t: t, conn: conn, r: resp.NewReader(conn)}
}

func (c *psClient) send(args ...string) {
	c.t.Helper()
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	if _, err := c.conn.Write(resp.AppendCommand(nil, b...)); err != nil {
		c.t.Fatal(err)
	}
}

// next reads one reply and renders it as a space-joined string.
func (c *psClient) next() string {
	c.t.Helper()
	v, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return render(v)
}

func render(v resp.Value) string {
	switch v.Type {
	case resp.Array:
		parts := make([]string, len(v.Array))
		for i, e := range v.Array {
			parts[i] = render(e)
		}
		return strings.Join(parts, " ")
	case resp.Integer:
		return fmt.Sprint(v.Int)
	}
	if v.Null {
		return "<nil>"
	}
	return string(v.Str)
}

func startCfg(t *testing.T, cfg Config) (*Server, *command.Env, string) {
	t.Helper()
	env := command.NewEnv(store.New(8))
	cfg.Addr = "127.0.0.1:0"
	srv := New(env, cfg)
	ln, err := srv.Listen()
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return srv, env, ln.Addr().String()
}

func TestSubscribePublishFlow(t *testing.T) {
	_, _, addr := startCfg(t, Config{})
	sub := newPS(t, addr)
	pub := newPS(t, addr)

	sub.send("SUBSCRIBE", "news", "sport")
	if got := sub.next(); got != "subscribe news 1" {
		t.Fatal(got)
	}
	if got := sub.next(); got != "subscribe sport 2" {
		t.Fatal(got)
	}
	sub.send("PSUBSCRIBE", "alerts.*")
	if got := sub.next(); got != "psubscribe alerts.* 3" {
		t.Fatal(got)
	}

	pub.send("PUBLISH", "news", "hello")
	if got := pub.next(); got != "1" {
		t.Fatalf("receivers = %s", got)
	}
	if got := sub.next(); got != "message news hello" {
		t.Fatal(got)
	}
	pub.send("PUBLISH", "alerts.fire", "now")
	pub.next()
	if got := sub.next(); got != "pmessage alerts.* alerts.fire now" {
		t.Fatal(got)
	}
	pub.send("PUBLISH", "nobody", "x")
	if got := pub.next(); got != "0" {
		t.Fatal(got)
	}

	// Restricted command set while subscribed.
	sub.send("GET", "k")
	if got := sub.next(); got != "ERR Can't execute 'get': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context" {
		t.Fatal(got)
	}
	sub.send("SET", "k", "v")
	if got := sub.next(); !strings.HasPrefix(got, "ERR Can't execute 'set'") {
		t.Fatal(got)
	}
	sub.send("PING")
	if got := sub.next(); got != "pong " {
		t.Fatalf("PING in subscribed mode: %q", got)
	}
	sub.send("PING", "x")
	if got := sub.next(); got != "pong x" {
		t.Fatal(got)
	}

	sub.send("UNSUBSCRIBE", "news")
	if got := sub.next(); got != "unsubscribe news 2" {
		t.Fatal(got)
	}
	sub.send("PUNSUBSCRIBE")
	if got := sub.next(); got != "punsubscribe alerts.* 1" {
		t.Fatal(got)
	}
	sub.send("UNSUBSCRIBE")
	if got := sub.next(); got != "unsubscribe sport 0" {
		t.Fatal(got)
	}
	// Back to normal mode: ordinary commands work again.
	sub.send("SET", "k", "v")
	if got := sub.next(); got != "OK" {
		t.Fatalf("after leaving subscribed mode: %q", got)
	}
	sub.send("UNSUBSCRIBE")
	if got := sub.next(); got != "unsubscribe <nil> 0" {
		t.Fatal(got)
	}
	sub.send("QUIT")
	if got := sub.next(); got != "OK" {
		t.Fatal(got)
	}
}

func TestQuitAllowedWhileSubscribed(t *testing.T) {
	_, _, addr := startCfg(t, Config{})
	c := newPS(t, addr)
	c.send("SUBSCRIBE", "x")
	c.next()
	c.send("QUIT")
	if got := c.next(); got != "OK" {
		t.Fatal(got)
	}
}

func TestPubSubIntrospection(t *testing.T) {
	_, _, addr := startCfg(t, Config{})
	a, b := newPS(t, addr), newPS(t, addr)
	a.send("SUBSCRIBE", "c1", "c2")
	a.next()
	a.next()
	b.send("PSUBSCRIBE", "p*")
	b.next()
	q := newPS(t, addr)
	q.send("PUBSUB", "CHANNELS")
	if got := q.next(); got != "c1 c2" {
		t.Fatal(got)
	}
	q.send("PUBSUB", "CHANNELS", "c1")
	if got := q.next(); got != "c1" {
		t.Fatal(got)
	}
	q.send("PUBSUB", "NUMSUB", "c1", "zz")
	if got := q.next(); got != "c1 1 zz 0" {
		t.Fatal(got)
	}
	q.send("PUBSUB", "NUMPAT")
	if got := q.next(); got != "1" {
		t.Fatal(got)
	}
	q.send("PUBSUB", "BOGUS")
	if got := q.next(); !strings.HasPrefix(got, "ERR unknown subcommand") {
		t.Fatal(got)
	}
	q.send("INFO", "stats")
	if got := q.next(); !strings.Contains(got, "pubsub_channels:2") || !strings.Contains(got, "pubsub_patterns:1") {
		t.Fatal(got)
	}
}

// TestSlowSubscriberIsDisconnected: a client that subscribes and never reads
// must be dropped once its bounded queue fills, while PUBLISH stays fast and a
// healthy subscriber on the same channel keeps receiving everything.
func TestSlowSubscriberIsDisconnected(t *testing.T) {
	_, env, addr := startCfg(t, Config{SubQueueMessages: 8, SubQueueBytes: 4 << 20})

	slow := newPS(t, addr)
	slow.send("SUBSCRIBE", "feed")
	slow.next()
	// slow never reads again.

	fast := newPS(t, addr)
	fast.send("SUBSCRIBE", "feed")
	fast.next()
	var got atomic.Int64
	received := make(chan int, 1)
	go func() {
		for {
			_ = fast.conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, err := fast.r.ReadValue(); err != nil {
				received <- int(got.Load())
				return
			}
			got.Add(1)
		}
	}()

	pub := newPS(t, addr)
	payload := strings.Repeat("x", 512*1024)
	const total = 80
	var worst time.Duration
	for i := 0; i < total; i++ {
		// Keep the healthy subscriber within a few messages of the publisher,
		// as a consumer that is merely busy rather than stuck would be.
		for wait := 0; int64(i)-got.Load() > 4 && wait < 5000; wait++ {
			time.Sleep(time.Millisecond)
		}
		start := time.Now()
		pub.send("PUBLISH", "feed", payload)
		pub.next()
		if d := time.Since(start); d > worst {
			worst = d
		}
	}
	if worst > 2*time.Second {
		t.Fatalf("PUBLISH blocked for %v behind a slow subscriber", worst)
	}

	deadline := time.Now().Add(5 * time.Second)
	for env.Hub.Overflows.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("slow subscriber was never disconnected")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Its subscription is cleaned up and the server reports the drop.
	for env.Hub.NumSub("feed") != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("subscribers left on feed: %d", env.Hub.NumSub("feed"))
		}
		time.Sleep(10 * time.Millisecond)
	}
	pub.send("INFO", "stats")
	if got := pub.next(); !strings.Contains(got, "pubsub_slow_subscribers_disconnected:1") {
		t.Fatal("INFO does not report the disconnect")
	}
	if n := <-received; n != total {
		t.Fatalf("healthy subscriber got %d of %d messages", n, total)
	}
	t.Logf("worst PUBLISH latency %v", worst)
}

// TestNoGoroutineLeaks subscribes and disconnects many clients in several ways
// and checks that goroutines return to the baseline.
func TestNoGoroutineLeaks(t *testing.T) {
	srv, env, addr := startCfg(t, Config{})
	_ = srv
	settle := func() int {
		var n int
		for i := 0; i < 40; i++ {
			runtime.GC()
			n = runtime.NumGoroutine()
			time.Sleep(25 * time.Millisecond)
		}
		return n
	}
	// Warm up so lazily started runtime goroutines are not counted as leaks.
	w := newPS(t, addr)
	w.send("PING")
	w.next()
	_ = w.conn.Close()
	for env.Stats().Clients != 0 {
		time.Sleep(5 * time.Millisecond)
	}
	before := settle()

	const clients = 60
	conns := make([]*psClient, clients)
	for i := range conns {
		c := newPS(t, addr)
		conns[i] = c
		c.send("SUBSCRIBE", fmt.Sprint("ch", i%7), "shared")
		c.next()
		c.next()
		c.send("PSUBSCRIBE", "p*")
		c.next()
	}
	pub := newPS(t, addr)
	for i := 0; i < 20; i++ {
		pub.send("PUBLISH", "shared", "msg")
		pub.next()
	}
	if env.Hub.NumSub("shared") != clients {
		t.Fatalf("subscribed=%d", env.Hub.NumSub("shared"))
	}
	for i, c := range conns {
		switch i % 3 {
		case 0: // abrupt disconnect
			_ = c.conn.Close()
		case 1: // polite unsubscribe, then close
			c.send("UNSUBSCRIBE")
			c.send("PUNSUBSCRIBE")
			_ = c.conn.Close()
		case 2: // QUIT
			c.send("QUIT")
			_ = c.conn.Close()
		}
	}
	_ = pub.conn.Close()

	deadline := time.Now().Add(5 * time.Second)
	for env.Stats().Clients != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("clients still registered: %d", env.Stats().Clients)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if env.Hub.NumChannels() != 0 || env.Hub.NumPat() != 0 {
		t.Fatalf("hub not empty: %d channels, %d patterns", env.Hub.NumChannels(), env.Hub.NumPat())
	}
	after := settle()
	if after > before+2 {
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Fatalf("goroutines: before=%d after=%d\n%s", before, after, buf[:n])
	}
	t.Logf("goroutines before=%d after=%d", before, after)
}

func TestShutdownWithSubscribers(t *testing.T) {
	env := command.NewEnv(store.New(4))
	srv := New(env, Config{Addr: "127.0.0.1:0"})
	ln, _ := srv.Listen()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	c := newPS(t, ln.Addr().String())
	c.send("SUBSCRIBE", "x")
	c.next()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown with an idle subscriber: %v", err)
	}
	<-done
}
