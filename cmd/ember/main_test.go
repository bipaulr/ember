package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type safeBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

var addrRe = regexp.MustCompile(`addr=(\S+)`)

func startRun(t *testing.T, args ...string) (addr string, cancel context.CancelFunc, done chan error, out *safeBuf) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	out = &safeBuf{}
	done = make(chan error, 1)
	go func() {
		done <- run(ctx, append([]string{"--host", "127.0.0.1", "--port", "0"}, args...), out)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if m := addrRe.FindStringSubmatch(out.String()); m != nil {
			return m[1], cancel, done, out
		}
		select {
		case err := <-done:
			t.Fatalf("run exited early: %v\n%s", err, out.String())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not become ready:\n%s", out.String())
	return
}

func roundTrip(t *testing.T, addr, cmd string) string {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = fmt.Fprint(c, cmd)
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	return line
}

// TestGracefulShutdownFlushesAOF: after cancel, run returns cleanly, and every
// acknowledged write is in the file even under the weakest fsync policy.
func TestGracefulShutdownFlushesAOF(t *testing.T) {
	path := filepath.Join(t.TempDir(), "g.aof")
	addr, cancel, done, out := startRun(t, "--aof-path", path, "--appendfsync", "no")
	if got := roundTrip(t, addr, "SET k v\r\n"); got != "+OK\r\n" {
		t.Fatal(got)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("shutdown returned %v\n%s", err, out.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown hung")
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), "SET") || !strings.Contains(out.String(), "shutting down") {
		t.Fatalf("aof=%q log=%s", data, out.String())
	}

	// A second start must see the data.
	addr2, cancel2, done2, _ := startRun(t, "--aof-path", path)
	if got := roundTrip(t, addr2, "GET k\r\n"); got != "$1\r\n" {
		t.Fatalf("GET after restart: %q", got)
	}
	cancel2()
	<-done2
}

func TestNoPersistence(t *testing.T) {
	addr, cancel, done, _ := startRun(t, "--aof-path", "")
	if got := roundTrip(t, addr, "PING\r\n"); got != "+PONG\r\n" {
		t.Fatal(got)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBadFlags(t *testing.T) {
	cases := [][]string{
		{"--appendfsync", "sometimes"},
		{"--nonsense"},
		{"--aof-path", filepath.Join(t.TempDir(), "no", "such", "dir", "x.aof")},
		{"--port", "-5"},
	}
	for _, args := range cases {
		out := &safeBuf{}
		err := run(context.Background(), append([]string{"--host", "127.0.0.1"}, args...), out)
		if err == nil {
			t.Errorf("%v: expected an error", args)
		}
	}
}

func TestPortInUse(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	port := ln.Addr().(*net.TCPAddr).Port
	err = run(context.Background(), []string{"--host", "127.0.0.1", "--port", fmt.Sprint(port), "--aof-path", ""}, &safeBuf{})
	if err == nil {
		t.Fatal("expected bind error")
	}
}

func TestHealthcheck(t *testing.T) {
	addr, cancel, done, _ := startRun(t, "--aof-path", "")
	defer func() { cancel(); <-done }()
	_, port, _ := net.SplitHostPort(addr)
	var p int
	if _, err := fmt.Sscan(port, &p); err != nil {
		t.Fatal(err)
	}
	if err := run(context.Background(), []string{"--healthcheck", "--port", port}, &safeBuf{}); err != nil {
		t.Fatalf("healthcheck against a live server: %v", err)
	}
	if err := run(context.Background(), []string{"--healthcheck", "--port", "1"}, &safeBuf{}); err == nil {
		t.Fatal("healthcheck must fail when nothing is listening")
	}
}

func TestPprofFlag(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	pp := ln.Addr().String()
	_ = ln.Close()
	_, cancel, done, _ := startRun(t, "--aof-path", "", "--pprof-addr", pp)
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := net.Dial("tcp", pp)
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("pprof endpoint never came up")
		}
		time.Sleep(20 * time.Millisecond)
	}
}
