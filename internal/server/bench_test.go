package server

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"

	"ember/internal/command"
	"ember/internal/resp"
	"ember/internal/store"
)

var benchValue = strings.Repeat("x", 64)

// batch builds one pipelined request of depth commands for connection c.
func batch(c, depth int, mk func(key string) []byte) []byte {
	var out []byte
	for d := 0; d < depth; d++ {
		out = append(out, mk(fmt.Sprintf("key:%012d", c*1000+d))...)
	}
	return out
}

func setCmd(key string) []byte {
	return resp.AppendCommand(nil, []byte("SET"), []byte(key), []byte(benchValue))
}
func getCmd(key string) []byte { return resp.AppendCommand(nil, []byte("GET"), []byte(key)) }

// runPipelined drives the whole stack over loopback TCP: 50 connections each
// send a prebuilt batch of depth commands and read every reply, the shape of
// redis-benchmark -c 50 -P depth. The client does no per-iteration allocation,
// so allocs/op reflects the server. One b.N iteration is one command.
func runPipelined(b *testing.B, depth int, mk func(key string) []byte, replyLen int) {
	env := command.NewEnv(store.New(256))
	srv := New(env, Config{Addr: "127.0.0.1:0"})
	ln, err := srv.Listen()
	if err != nil {
		b.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = ln.Close() }()

	const conns = 50
	type client struct {
		conn net.Conn
		req  []byte
		rep  []byte
	}
	clients := make([]client, conns)
	for c := range clients {
		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			b.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		// Populate the keys first so GET benchmarks hit.
		seed := batch(c, depth, setCmd)
		if _, err := conn.Write(seed); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(conn, make([]byte, 5*depth)); err != nil {
			b.Fatal(err)
		}
		clients[c] = client{conn: conn, req: batch(c, depth, mk), rep: make([]byte, replyLen*depth)}
	}

	batches := b.N/(conns*depth) + 1
	b.ReportAllocs()
	b.ResetTimer()
	var wg sync.WaitGroup
	for c := range clients {
		wg.Add(1)
		go func(cl client) {
			defer wg.Done()
			for n := 0; n < batches; n++ {
				if _, err := cl.conn.Write(cl.req); err != nil {
					b.Error(err)
					return
				}
				if _, err := io.ReadFull(cl.conn, cl.rep); err != nil {
					b.Error(err)
					return
				}
			}
		}(clients[c])
	}
	wg.Wait()
	b.StopTimer()
	b.ReportMetric(float64(conns*batches*depth)/b.Elapsed().Seconds(), "cmds/s")
}

func BenchmarkServerSET_P1(b *testing.B)  { runPipelined(b, 1, setCmd, 5) }
func BenchmarkServerSET_P16(b *testing.B) { runPipelined(b, 16, setCmd, 5) }
func BenchmarkServerGET_P16(b *testing.B) { runPipelined(b, 16, getCmd, 5+64+2) }
