package server

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"ember/internal/command"
	"ember/internal/store"
)

func start(t *testing.T, maxClients int) (*Server, string) {
	t.Helper()
	env := command.NewEnv(store.New(8))
	srv := New(env, Config{Addr: "127.0.0.1:0", MaxClients: maxClients})
	ln, err := srv.Listen()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v", err)
		}
	})
	return srv, ln.Addr().String()
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(10 * time.Second))
	return c
}

func mustWrite(t *testing.T, w io.Writer, s string) {
	t.Helper()
	if _, err := io.WriteString(w, s); err != nil {
		t.Fatal(err)
	}
}

func readN(t *testing.T, r *bufio.Reader, n int) string {
	t.Helper()
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(buf)
}

func TestPingInlineAndArray(t *testing.T) {
	_, addr := start(t, 0)
	c := dial(t, addr)
	r := bufio.NewReader(c)
	mustWrite(t, c, "PING\r\n")
	if got := readN(t, r, 7); got != "+PONG\r\n" {
		t.Fatalf("inline: %q", got)
	}
	mustWrite(t, c, "*1\r\n$4\r\nPING\r\n")
	if got := readN(t, r, 7); got != "+PONG\r\n" {
		t.Fatalf("array: %q", got)
	}
}

// TestPipelining sends many commands in a single write and expects all
// replies, in order, without sending anything else.
func TestPipelining(t *testing.T) {
	_, addr := start(t, 0)
	c := dial(t, addr)
	const n = 2000
	var sb strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&sb, "*2\r\n$4\r\nINCR\r\n$1\r\nc\r\n")
	}
	if _, err := io.WriteString(c, sb.String()); err != nil {
		t.Fatal(err)
	}
	r := bufio.NewReader(c)
	for i := 1; i <= n; i++ {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf(":%d\r\n", i); line != want {
			t.Fatalf("reply %d: got %q want %q", i, line, want)
		}
	}
}

// TestPartialCommandAfterReplies makes sure a half-sent command does not hold
// back the replies to the complete commands before it.
func TestPartialCommandAfterReplies(t *testing.T) {
	_, addr := start(t, 0)
	c := dial(t, addr)
	r := bufio.NewReader(c)
	mustWrite(t, c, "PING\r\n*2\r\n$4\r\nEC")
	if got := readN(t, r, 7); got != "+PONG\r\n" {
		t.Fatalf("got %q", got)
	}
	mustWrite(t, c, "HO\r\n$2\r\nhi\r\n")
	if got := readN(t, r, 8); got != "$2\r\nhi\r\n" {
		t.Fatalf("got %q", got)
	}
}

func TestProtocolErrorClosesOnlyThatConnection(t *testing.T) {
	_, addr := start(t, 0)
	bad := dial(t, addr)
	good := dial(t, addr)
	gr := bufio.NewReader(good)

	mustWrite(t, bad, "*1\r\n:5\r\n")
	br := bufio.NewReader(bad)
	line, err := br.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "-ERR Protocol error") {
		t.Fatalf("got %q, %v", line, err)
	}
	if _, err := br.ReadString('\n'); err == nil {
		t.Fatal("bad connection should be closed")
	}
	mustWrite(t, good, "PING\r\n")
	if got := readN(t, gr, 7); got != "+PONG\r\n" {
		t.Fatalf("healthy connection broke: %q", got)
	}
}

func TestQuit(t *testing.T) {
	_, addr := start(t, 0)
	c := dial(t, addr)
	r := bufio.NewReader(c)
	mustWrite(t, c, "QUIT\r\n")
	if got := readN(t, r, 5); got != "+OK\r\n" {
		t.Fatal(got)
	}
	if _, err := r.ReadByte(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}

func TestMaxClients(t *testing.T) {
	srv, addr := start(t, 1)
	first := dial(t, addr)
	fr := bufio.NewReader(first)
	mustWrite(t, first, "PING\r\n")
	readN(t, fr, 7) // ensure the first connection is registered

	second := dial(t, addr)
	line, err := bufio.NewReader(second).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "-ERR max number of clients reached") {
		t.Fatalf("got %q, %v", line, err)
	}
	if srv.env.Rejected.Load() != 1 {
		t.Fatal("rejection not counted")
	}
}

func TestClientCleanup(t *testing.T) {
	srv, addr := start(t, 0)
	c := dial(t, addr)
	mustWrite(t, c, "PING\r\n")
	readN(t, bufio.NewReader(c), 7)
	_ = c.Close()
	deadline := time.Now().Add(3 * time.Second)
	for srv.env.Stats().Clients != 0 {
		if time.Now().After(deadline) {
			t.Fatal("client was not removed after disconnect")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestGracefulShutdown checks that Shutdown returns promptly with idle clients
// connected and that a reply already requested is delivered.
func TestGracefulShutdown(t *testing.T) {
	env := command.NewEnv(store.New(8))
	srv := New(env, Config{Addr: "127.0.0.1:0"})
	ln, _ := srv.Listen()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()

	c := dial(t, ln.Addr().String())
	r := bufio.NewReader(c)
	mustWrite(t, c, "SET k v\r\n")
	readN(t, r, 5)
	idle := dial(t, ln.Addr().String())
	mustWrite(t, idle, "PING\r\n")
	readN(t, bufio.NewReader(idle), 7)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	start := time.Now()
	if err := srv.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("shutdown took %v", d)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := net.DialTimeout("tcp", ln.Addr().String(), 300*time.Millisecond); err == nil {
		t.Fatal("listener should be closed")
	}
}

func TestLargeReplyOverBufferSize(t *testing.T) {
	_, addr := start(t, 0)
	c := dial(t, addr)
	r := bufio.NewReader(c)
	big := strings.Repeat("x", 200_000)
	mustWrite(t, c, fmt.Sprintf("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$%d\r\n%s\r\n", len(big), big))
	readN(t, r, 5)
	mustWrite(t, c, "GET k\r\n")
	want := fmt.Sprintf("$%d\r\n%s\r\n", len(big), big)
	if got := readN(t, r, len(want)); got != want {
		t.Fatal("large value corrupted")
	}
}

func TestIsClosedErr(t *testing.T) {
	if !isClosedErr(io.EOF) || !isClosedErr(net.ErrClosed) || isClosedErr(fmt.Errorf("boom")) {
		t.Fatal("isClosedErr misclassifies")
	}
}
