package command

import (
	"io"
	"strings"
	"testing"

	"ember/internal/resp"
	"ember/internal/store"
)

func benchCtx() *Ctx {
	env := NewEnv(store.New(256))
	return &Ctx{W: resp.NewWriter(io.Discard), Store: env.Store, Client: env.NewClient("bench"), Env: env}
}

func benchExec(b *testing.B, args ...string) {
	c := benchCtx()
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	c.Args = argv
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Env.Table.Execute(c)
		if c.W.Buffered() > 8192 {
			_ = c.W.Flush()
		}
	}
}

func BenchmarkExecuteSET(b *testing.B) {
	benchExec(b, "SET", "key:000000012345", strings.Repeat("v", 64))
}

func BenchmarkExecuteGET(b *testing.B) {
	c := benchCtx()
	c.Args = [][]byte{[]byte("SET"), []byte("key:000000012345"), []byte("value")}
	c.Env.Table.Execute(c)
	c.Args = [][]byte{[]byte("GET"), []byte("key:000000012345")}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Env.Table.Execute(c)
		if c.W.Buffered() > 8192 {
			_ = c.W.Flush()
		}
	}
}

func BenchmarkExecuteINCR(b *testing.B)  { benchExec(b, "INCR", "counter") }
func BenchmarkExecuteLPUSH(b *testing.B) { benchExec(b, "LPUSH", "list", "element") }
func BenchmarkExecutePING(b *testing.B)  { benchExec(b, "PING") }
