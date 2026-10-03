// Package integration holds end-to-end tests that drive Ember with a real
// client (go-redis) and, for crash tests, with the compiled binary.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"ember/internal/command"
	"ember/internal/server"
	"ember/internal/store"
)

var binPath string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ember-bin")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	name := "ember"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binPath = filepath.Join(dir, name)
	build := exec.Command("go", "build", "-o", binPath, "ember/cmd/ember")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "building ember failed: %v\n%s", err, out)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

// syncBuf is a goroutine-safe buffer for capturing process output.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func freePort(t testing.TB) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
}

// proc is a running ember subprocess.
type proc struct {
	t    testing.TB
	cmd  *exec.Cmd
	addr string
	out  *syncBuf
	done chan struct{}
	err  error
}

func startProc(t testing.TB, aofPath, policy string, extra ...string) *proc {
	t.Helper()
	p := startProcNoWait(t, aofPath, policy, extra...)
	deadline := time.Now().Add(15 * time.Second)
	for {
		select {
		case <-p.done:
			t.Fatalf("ember exited during startup: %v\n%s", p.err, p.out.String())
		default:
		}
		conn, err := net.DialTimeout("tcp", p.addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return p
		}
		if time.Now().After(deadline) {
			p.kill()
			t.Fatalf("ember did not start:\n%s", p.out.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func startProcNoWait(t testing.TB, aofPath, policy string, extra ...string) *proc {
	t.Helper()
	port := freePort(t)
	args := []string{"--host", "127.0.0.1", "--port", strconv.Itoa(port), "--aof-path", aofPath, "--appendfsync", policy}
	args = append(args, extra...)
	p := &proc{t: t, addr: "127.0.0.1:" + strconv.Itoa(port), out: &syncBuf{}, done: make(chan struct{})}
	p.cmd = exec.Command(binPath, args...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { p.err = p.cmd.Wait(); close(p.done) }()
	t.Cleanup(p.kill)
	return p
}

// kill ends the process immediately, with no chance to flush or clean up.
func (p *proc) kill() {
	_ = p.cmd.Process.Kill()
	<-p.done
}

func (p *proc) client() *redis.Client { return newClient(p.addr) }

func newClient(addr string) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:            addr,
		Protocol:        2,
		DisableIdentity: true,
		MaxRetries:      -1, // a failed write must surface, not be silently retried
		PoolSize:        128,
		DialTimeout:     5 * time.Second,
	})
}

// inProc runs the server inside the test process, so the race detector
// covers it. It returns a connected client.
func inProc(t testing.TB) (*redis.Client, *command.Env) {
	t.Helper()
	env := command.NewEnv(store.New(64))
	srv := server.New(env, server.Config{Addr: "127.0.0.1:0"})
	ln, err := srv.Listen()
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	go env.Store.RunExpiry(20*time.Millisecond, stop)
	go func() { _ = srv.Serve(ln) }()
	c := newClient(ln.Addr().String())
	t.Cleanup(func() {
		_ = c.Close()
		close(stop)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	return c, env
}
