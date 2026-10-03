package resp

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestReadValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Value
	}{
		{"simple", "+OK\r\n", Value{Type: SimpleString, Str: []byte("OK")}},
		{"error", "-ERR boom\r\n", Value{Type: Error, Str: []byte("ERR boom")}},
		{"int", ":1000\r\n", Value{Type: Integer, Int: 1000}},
		{"negative int", ":-42\r\n", Value{Type: Integer, Int: -42}},
		{"bulk", "$5\r\nhello\r\n", Value{Type: BulkString, Str: []byte("hello")}},
		{"empty bulk", "$0\r\n\r\n", Value{Type: BulkString, Str: []byte{}}},
		{"bulk with crlf inside", "$4\r\na\r\nb\r\n", Value{Type: BulkString, Str: []byte("a\r\nb")}},
		{"null bulk", "$-1\r\n", Value{Type: BulkString, Null: true}},
		{"null array", "*-1\r\n", Value{Type: Array, Null: true}},
		{"empty array", "*0\r\n", Value{Type: Array, Array: []Value{}}},
		{"array", "*2\r\n$3\r\nfoo\r\n:7\r\n", Value{Type: Array, Array: []Value{
			{Type: BulkString, Str: []byte("foo")}, {Type: Integer, Int: 7},
		}}},
		{"nested", "*1\r\n*1\r\n+x\r\n", Value{Type: Array, Array: []Value{
			{Type: Array, Array: []Value{{Type: SimpleString, Str: []byte("x")}}},
		}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := NewReader(strings.NewReader(tc.in)).ReadValue()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestReadValueErrors(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		proto bool // expect *ProtocolError
		err   error
	}{
		{"unknown type", "?what\r\n", true, nil},
		{"bad int", ":12x\r\n", true, nil},
		{"bulk too big", "$536870913\r\n", true, nil},
		{"bulk negative", "$-5\r\n", true, nil},
		{"array too big", "*1048577\r\n", true, nil},
		{"array negative", "*-2\r\n", true, nil},
		{"bulk bad terminator", "$3\r\nabcXY", true, nil},
		{"bare LF header", "$3\n", true, nil},
		{"truncated bulk", "$10\r\nabc", false, io.ErrUnexpectedEOF},
		{"truncated array", "*2\r\n+a\r\n", false, io.ErrUnexpectedEOF},
		{"truncated line", "+OK", false, io.ErrUnexpectedEOF},
		{"empty", "", false, io.EOF},
		{"too deep", strings.Repeat("*1\r\n", MaxDepth+2) + "+x\r\n", true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tc.in)).ReadValue()
			if err == nil {
				t.Fatal("expected error")
			}
			if tc.proto && !IsProtocolError(err) {
				t.Fatalf("expected protocol error, got %v", err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("got %v, want %v", err, tc.err)
			}
		})
	}
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"array", "*2\r\n$4\r\nPING\r\n$2\r\nhi\r\n", []string{"PING", "hi"}},
		{"inline", "PING\r\n", []string{"PING"}},
		{"inline bare LF", "SET a b\n", []string{"SET", "a", "b"}},
		{"inline spaces", "  SET   a   b  \r\n", []string{"SET", "a", "b"}},
		{"inline double quotes", "SET k \"hello world\"\r\n", []string{"SET", "k", "hello world"}},
		{"inline escapes", `SET k "a\nb\"c"` + "\r\n", []string{"SET", "k", "a\nb\"c"}},
		{"inline single quotes", "SET k 'x y'\r\n", []string{"SET", "k", "x y"}},
		{"binary safe bulk", "*1\r\n$3\r\n\x00\r\n\r\n", []string{"\x00\r\n"}},
		{"empty inline", "\r\n", nil},
		{"empty array", "*0\r\n", nil},
		{"null array", "*-1\r\n", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args, err := NewReader(strings.NewReader(tc.in)).ReadCommand()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var got []string
			for _, a := range args {
				got = append(got, string(a))
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestReadCommandErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"non-bulk element", "*1\r\n:5\r\n"},
		{"bad header", "*abc\r\n"},
		{"huge array", "*99999999\r\n"},
		{"huge bulk", "*1\r\n$999999999999\r\n"},
		{"negative bulk", "*1\r\n$-1\r\n"},
		{"unbalanced quote", "SET k \"oops\r\n"},
		{"quote glued", "SET \"a\"b c\r\n"},
		{"line too long", strings.Repeat("a", ReadBufferSize*2) + "\r\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tc.in)).ReadCommand()
			if !IsProtocolError(err) {
				t.Fatalf("expected protocol error, got %v", err)
			}
		})
	}
}

// TestPipelining feeds many commands in one buffer and expects them in order.
func TestPipelining(t *testing.T) {
	var in bytes.Buffer
	const n = 500
	for i := 0; i < n; i++ {
		in.Write(AppendCommand(nil, []byte("INCR"), []byte("counter")))
	}
	in.WriteString("PING\r\n")
	r := NewReader(&in)
	for i := 0; i < n; i++ {
		args, err := r.ReadCommand()
		if err != nil || len(args) != 2 || string(args[0]) != "INCR" {
			t.Fatalf("command %d: %q %v", i, args, err)
		}
	}
	args, err := r.ReadCommand()
	if err != nil || string(args[0]) != "PING" {
		t.Fatalf("inline tail: %q %v", args, err)
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
}

// oneByteReader delivers a single byte per Read to exercise partial reads.
type oneByteReader struct{ r io.Reader }

func (o oneByteReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return o.r.Read(p[:1])
}

func TestPartialReads(t *testing.T) {
	in := string(AppendCommand(nil, []byte("SET"), []byte("key"), []byte("value"))) + "PING\r\n"
	r := NewReader(oneByteReader{strings.NewReader(in)})
	a, err := r.ReadCommand()
	if err != nil || len(a) != 3 || string(a[2]) != "value" {
		t.Fatalf("got %q %v", a, err)
	}
	a, err = r.ReadCommand()
	if err != nil || string(a[0]) != "PING" {
		t.Fatalf("got %q %v", a, err)
	}
}

// TestLargeBulkDoesNotPreallocate declares a huge bulk but sends little data.
func TestLargeBulkDoesNotPreallocate(t *testing.T) {
	in := "*1\r\n$536870912\r\nabc"
	_, err := NewReader(strings.NewReader(in)).ReadCommand()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v", err)
	}
}

func TestLargeBulkRoundTrip(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 3*allocChunk+17)
	wire := AppendCommand(nil, []byte("SET"), []byte("k"), payload)
	args, err := NewReader(bytes.NewReader(wire)).ReadCommand()
	if err != nil || !bytes.Equal(args[2], payload) {
		t.Fatalf("round trip failed: %v", err)
	}
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteSimple("OK")
	w.WriteError("ERR bad")
	w.WriteInt(-5)
	w.WriteBulk([]byte("hi"))
	w.WriteBulkString("yo")
	w.WriteNull()
	w.WriteNullArray()
	w.WriteArrayHeader(2)
	w.WriteRaw([]byte(":1\r\n"))
	if w.Buffered() == 0 {
		t.Fatal("expected buffered bytes")
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n-ERR bad\r\n:-5\r\n$2\r\nhi\r\n$2\r\nyo\r\n$-1\r\n*-1\r\n*2\r\n:1\r\n"
	if buf.String() != want {
		t.Fatalf("got %q, want %q", buf.String(), want)
	}
}

func TestWriterReaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteArrayHeader(3)
	w.WriteBulk([]byte("a"))
	w.WriteNull()
	w.WriteInt(9)
	_ = w.Flush()
	v, err := NewReader(&buf).ReadValue()
	if err != nil || len(v.Array) != 3 || !v.Array[1].Null || v.Array[2].Int != 9 {
		t.Fatalf("got %+v %v", v, err)
	}
}

func TestProtocolErrorMessage(t *testing.T) {
	err := protoErr("bad %d", 1)
	if err.Error() != "Protocol error: bad 1" {
		t.Fatal(err.Error())
	}
	if IsProtocolError(io.EOF) {
		t.Fatal("EOF is not a protocol error")
	}
}

func TestNewReaderReusesBufio(t *testing.T) {
	r := NewReader(strings.NewReader("PING\r\n"))
	if r.Buffered() != 0 {
		t.Fatal("nothing should be buffered before first read")
	}
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	r2 := NewReader(r.br)
	if r2.br != r.br {
		t.Fatal("expected reuse of bufio.Reader")
	}
}

func TestParseInt(t *testing.T) {
	good := map[string]int64{"0": 0, "7": 7, "-1": -1, "1234567890": 1234567890,
		"9223372036854775807": 1<<63 - 1, "-9223372036854775808": -1 << 63}
	for s, want := range good {
		if got, ok := ParseInt([]byte(s)); !ok || got != want {
			t.Errorf("ParseInt(%q) = %d, %v", s, got, ok)
		}
	}
	for _, s := range []string{"", "-", "+1", "01", "-0", "00", "1x", " 1", "1 ", "-a",
		"9223372036854775808", "-9223372036854775809", "99999999999999999999", "123456789012345678901"} {
		if _, ok := ParseInt([]byte(s)); ok {
			t.Errorf("ParseInt(%q) should fail", s)
		}
	}
}

// TestArenaReuseAcrossManyCommands parses thousands of commands of varied
// shapes through one Reader, crossing arena block boundaries, and checks each
// against what was sent.
func TestArenaReuseAcrossManyCommands(t *testing.T) {
	sizes := []int{0, 1, 7, 100, 1000, 4095, 4096, 4097, 9000, arenaMaxArg - 1, arenaMaxArg, arenaMaxArg + 1, 40000}
	var wire []byte
	var want [][][]byte
	for i := 0; i < 3000; i++ {
		n := 1 + i%6
		args := make([][]byte, n)
		for j := range args {
			sz := sizes[(i*7+j*3)%len(sizes)]
			args[j] = bytes.Repeat([]byte{byte('a' + (i+j)%26)}, sz)
		}
		want = append(want, args)
		wire = AppendCommand(wire, args...)
	}
	r := NewReader(bytes.NewReader(wire))
	for i, w := range want {
		got, err := r.ReadCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if len(got) != len(w) {
			t.Fatalf("command %d: %d args, want %d", i, len(got), len(w))
		}
		for j := range w {
			if !bytes.Equal(got[j], w[j]) {
				t.Fatalf("command %d arg %d corrupted", i, j)
			}
		}
	}
}

// TestArgumentsAreIsolated: appending to one argument must not overwrite its
// neighbour, even though they share an arena.
func TestArgumentsAreIsolated(t *testing.T) {
	wire := AppendCommand(nil, []byte("AAAA"), []byte("BBBB"), []byte("CCCC"))
	args, err := NewReader(bytes.NewReader(wire)).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	_ = append(args[0], 'X', 'X', 'X')
	if string(args[1]) != "BBBB" || string(args[2]) != "CCCC" {
		t.Fatalf("neighbouring arguments were overwritten: %q %q", args[1], args[2])
	}
}

// TestReadCommandDoesNotAllocate pins the optimisation: after warm-up, parsing
// a command of small arguments allocates nothing.
func TestReadCommandDoesNotAllocate(t *testing.T) {
	wire := AppendCommand(nil, []byte("SET"), []byte("key:000000012345"), bytes.Repeat([]byte("v"), 64))
	r := NewReader(&loopReader{data: wire})
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if _, err := r.ReadCommand(); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("ReadCommand allocated %.1f times per command, want 0", allocs)
	}
}

func TestReadCommandErrorsMidArena(t *testing.T) {
	// A bad terminator on a small (arena) argument is still a protocol error.
	_, err := NewReader(strings.NewReader("*2\r\n$3\r\nabc\r\n$2\r\nxyZZ")).ReadCommand()
	if !IsProtocolError(err) {
		t.Fatalf("got %v", err)
	}
	_, err = NewReader(strings.NewReader("*1\r\n$5\r\nab")).ReadCommand()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("got %v", err)
	}
	_, err = NewReader(strings.NewReader("*1\r\n$2\r\nab")).ReadCommand()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("missing CRLF: got %v", err)
	}
}
