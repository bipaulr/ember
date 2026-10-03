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
