package resp

import (
	"io"
	"strings"
	"testing"
)

// loopReader serves the same bytes forever, so a benchmark can parse b.N
// commands without building a b.N-sized buffer.
type loopReader struct {
	data []byte
	pos  int
}

func (l *loopReader) Read(p []byte) (int, error) {
	n := copy(p, l.data[l.pos:])
	l.pos = (l.pos + n) % len(l.data)
	return n, nil
}

func benchRead(b *testing.B, wire []byte, perIter int) {
	r := NewReader(&loopReader{data: wire})
	b.SetBytes(int64(len(wire) / perIter))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadCommand(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadCommandGET(b *testing.B) {
	benchRead(b, AppendCommand(nil, []byte("GET"), []byte("key:000000012345")), 1)
}

func BenchmarkReadCommandSET(b *testing.B) {
	benchRead(b, AppendCommand(nil, []byte("SET"), []byte("key:000000012345"), []byte(strings.Repeat("v", 64))), 1)
}

// BenchmarkReadCommandPipeline16 parses a stream of 16 mixed commands, the shape
// of a redis-benchmark -P 16 request.
func BenchmarkReadCommandPipeline16(b *testing.B) {
	var wire []byte
	for i := 0; i < 16; i++ {
		wire = AppendCommand(wire, []byte("SET"), []byte("key:000000012345"), []byte(strings.Repeat("v", 64)))
	}
	benchRead(b, wire, 16)
}

func BenchmarkReadCommandInline(b *testing.B) {
	benchRead(b, []byte("SET key:000000012345 value\r\n"), 1)
}

func BenchmarkReadValueArray(b *testing.B) {
	wire := AppendCommand(nil, []byte("SET"), []byte("key:000000012345"), []byte(strings.Repeat("v", 64)))
	r := NewReader(&loopReader{data: wire})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriteReplies(b *testing.B) {
	w := NewWriter(io.Discard)
	val := []byte(strings.Repeat("v", 64))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.WriteBulk(val)
		w.WriteInt(int64(i))
		w.WriteSimple("OK")
		if w.Buffered() > 8192 {
			_ = w.Flush()
		}
	}
}
