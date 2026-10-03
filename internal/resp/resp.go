// Package resp implements the RESP2 wire protocol: a streaming parser that
// reads from a bufio.Reader and a writer that appends replies to a bufio.Writer.
package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
)

// Protocol limits. They match the defaults of Redis.
const (
	// MaxBulkLen is the largest bulk string accepted (512 MB).
	MaxBulkLen = 512 * 1024 * 1024
	// MaxArrayLen is the largest array accepted (1M elements).
	MaxArrayLen = 1024 * 1024
	// MaxDepth bounds nesting of arrays in ReadValue.
	MaxDepth = 32
	// ReadBufferSize is the size of the buffered reader. A header or inline
	// line longer than this is rejected.
	ReadBufferSize = 32 * 1024

	// allocChunk caps how much memory is reserved up front for a declared
	// length. Bigger payloads grow as bytes actually arrive, so a client
	// cannot make the server allocate 512 MB by sending only a header.
	allocChunk = 64 * 1024

	// Argument arena for ReadCommand.
	arenaSize   = 4 * 1024  // size of each arena block
	arenaMaxArg = 16 * 1024 // larger arguments get their own allocation
)

// Type identifies the kind of a RESP value.
type Type byte

// RESP2 type markers, which are also the first byte on the wire.
const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

// Value is a decoded RESP2 value.
type Value struct {
	Type  Type
	Str   []byte  // SimpleString, Error, BulkString
	Int   int64   // Integer
	Array []Value // Array
	Null  bool    // null bulk string or null array
}

// ProtocolError is returned for malformed input. The connection that produced
// it must be closed, because the stream can no longer be trusted.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Msg }

func protoErr(format string, a ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, a...)}
}

// IsProtocolError reports whether err is a *ProtocolError.
func IsProtocolError(err error) bool {
	var pe *ProtocolError
	return errors.As(err, &pe)
}

// Reader parses RESP2 from a stream.
type Reader struct {
	br *bufio.Reader

	// Reused by ReadCommand so that steady-state parsing allocates nothing.
	argBuf []byte
	argv   [][]byte
}

// NewReader wraps r. If r is already a *bufio.Reader it is used directly.
func NewReader(r io.Reader) *Reader {
	if br, ok := r.(*bufio.Reader); ok {
		return &Reader{br: br}
	}
	return &Reader{br: bufio.NewReaderSize(r, ReadBufferSize)}
}

// Buffered returns the number of bytes that can be read without blocking.
// The server uses it to decide whether more pipelined commands are waiting.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// readLine returns one line without its terminator. If crlfOnly is set, a bare
// LF is a protocol error; inline commands pass false so telnet works.
func (r *Reader) readLine(crlfOnly bool) ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return nil, protoErr("too big inline request or header")
		}
		if err == io.EOF && len(line) > 0 {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	line = line[:len(line)-1]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		return line[:n-1], nil
	}
	if crlfOnly {
		return nil, protoErr("expected CRLF line ending")
	}
	return line, nil
}

func (r *Reader) readInt(crlfOnly bool) (int64, error) {
	line, err := r.readLine(crlfOnly)
	if err != nil {
		return 0, err
	}
	n, ok := ParseInt(line)
	if !ok {
		return 0, protoErr("invalid integer %q", truncate(line))
	}
	return n, nil
}

// ParseInt parses a canonical base-10 int64: an optional '-', digits with no
// leading zeros, nothing else. It is the strictness of Redis string2ll, and
// unlike strconv.ParseInt(string(b)) it cannot allocate.
func ParseInt(b []byte) (int64, bool) {
	n := len(b)
	if n == 0 || n > 20 {
		return 0, false
	}
	i, neg := 0, false
	if b[0] == '-' {
		neg = true
		i = 1
		if n == 1 {
			return 0, false
		}
	}
	if b[i] == '0' {
		return 0, n == 1
	}
	var u uint64
	for ; i < n; i++ {
		d := b[i] - '0'
		if d > 9 {
			return 0, false
		}
		if u > (math.MaxUint64-uint64(d))/10 {
			return 0, false
		}
		u = u*10 + uint64(d)
	}
	if neg {
		if u > 1<<63 {
			return 0, false
		}
		return int64(-u), true
	}
	if u > math.MaxInt64 {
		return 0, false
	}
	return int64(u), true
}

func truncate(b []byte) []byte {
	if len(b) > 32 {
		return b[:32]
	}
	return b
}

// readBulkBody reads n bytes plus the trailing CRLF.
func (r *Reader) readBulkBody(n int) ([]byte, error) {
	var buf []byte
	if n <= allocChunk {
		buf = make([]byte, n)
		if _, err := io.ReadFull(r.br, buf); err != nil {
			return nil, unexpected(err)
		}
	} else {
		buf = make([]byte, 0, allocChunk)
		for len(buf) < n {
			want := min(n-len(buf), allocChunk)
			start := len(buf)
			buf = append(buf, make([]byte, want)...)
			if _, err := io.ReadFull(r.br, buf[start:]); err != nil {
				return nil, unexpected(err)
			}
		}
	}
	if err := r.expectCRLF(); err != nil {
		return nil, err
	}
	return buf, nil
}

// expectCRLF consumes the terminator after a bulk string. It reads single
// bytes because passing a local array through io.ReadFull would heap-allocate it.
func (r *Reader) expectCRLF() error {
	c1, err := r.br.ReadByte()
	if err != nil {
		return unexpected(err)
	}
	c2, err := r.br.ReadByte()
	if err != nil {
		return unexpected(err)
	}
	if c1 != '\r' || c2 != '\n' {
		return protoErr("bulk string not terminated by CRLF")
	}
	return nil
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// ReadValue parses one complete RESP2 value.
func (r *Reader) ReadValue() (Value, error) { return r.readValue(0) }

func (r *Reader) readValue(depth int) (Value, error) {
	if depth > MaxDepth {
		return Value{}, protoErr("nesting too deep")
	}
	b, err := r.br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Type(b) {
	case SimpleString, Error:
		line, err := r.readLine(true)
		if err != nil {
			return Value{}, unexpected(err)
		}
		return Value{Type: Type(b), Str: bytes.Clone(line)}, nil
	case Integer:
		n, err := r.readInt(true)
		if err != nil {
			return Value{}, unexpected(err)
		}
		return Value{Type: Integer, Int: n}, nil
	case BulkString:
		n, err := r.readInt(true)
		if err != nil {
			return Value{}, unexpected(err)
		}
		if n == -1 {
			return Value{Type: BulkString, Null: true}, nil
		}
		if n < 0 || n > MaxBulkLen {
			return Value{}, protoErr("invalid bulk length")
		}
		body, err := r.readBulkBody(int(n))
		if err != nil {
			return Value{}, err
		}
		return Value{Type: BulkString, Str: body}, nil
	case Array:
		n, err := r.readInt(true)
		if err != nil {
			return Value{}, unexpected(err)
		}
		if n == -1 {
			return Value{Type: Array, Null: true}, nil
		}
		if n < 0 || n > MaxArrayLen {
			return Value{}, protoErr("invalid multibulk length")
		}
		arr := make([]Value, 0, min(int(n), 1024))
		for i := int64(0); i < n; i++ {
			v, err := r.readValue(depth + 1)
			if err != nil {
				return Value{}, unexpected(err)
			}
			arr = append(arr, v)
		}
		return Value{Type: Array, Array: arr}, nil
	default:
		return Value{}, protoErr("unknown type byte %q", b)
	}
}

// ReadCommand reads one client command as a list of arguments. It accepts the
// array-of-bulk-strings form and inline commands. Blank inline lines and empty
// arrays yield a nil slice with a nil error; the caller should just continue.
//
// The returned slice and the bytes of every argument are only valid until the
// next call to ReadCommand: they live in buffers the Reader reuses. A handler
// that keeps an argument must copy it.
func (r *Reader) ReadCommand() ([][]byte, error) {
	first, err := r.br.Peek(1)
	if err != nil {
		return nil, err
	}
	if first[0] != '*' {
		return r.readInline()
	}
	_, _ = r.br.ReadByte()
	n, err := r.readInt(true)
	if err != nil {
		return nil, unexpected(err)
	}
	if n < 0 {
		return nil, nil
	}
	if n > MaxArrayLen {
		return nil, protoErr("invalid multibulk length")
	}
	clear(r.argv) // drop references left by the previous command
	args := r.argv[:0]
	buf := r.argBuf[:0]
	for i := int64(0); i < n; i++ {
		b, err := r.br.ReadByte()
		if err != nil {
			return nil, unexpected(err)
		}
		if b != '$' {
			return nil, protoErr("expected '$', got '%c'", b)
		}
		l, err := r.readInt(true)
		if err != nil {
			return nil, unexpected(err)
		}
		if l < 0 || l > MaxBulkLen {
			return nil, protoErr("invalid bulk length")
		}
		var body []byte
		if l <= arenaMaxArg {
			// Small argument: carve it out of the shared arena. If the arena is
			// full, start a new one; arguments already handed out keep pointing
			// at the old array, which stays alive and unchanged.
			if cap(buf)-len(buf) < int(l) {
				buf = make([]byte, 0, max(arenaSize, int(l)))
			}
			start := len(buf)
			buf = buf[:start+int(l)]
			body = buf[start : start+int(l) : start+int(l)]
			if _, err := io.ReadFull(r.br, body); err != nil {
				return nil, unexpected(err)
			}
			if err := r.expectCRLF(); err != nil {
				return nil, err
			}
		} else {
			body, err = r.readBulkBody(int(l))
			if err != nil {
				return nil, err
			}
		}
		args = append(args, body)
	}
	r.argv = args
	r.argBuf = buf // at most max(arenaSize, arenaMaxArg) bytes, so safe to keep
	return args, nil
}

func (r *Reader) readInline() ([][]byte, error) {
	line, err := r.readLine(false)
	if err != nil {
		return nil, err
	}
	return splitInline(line)
}

// splitInline splits an inline command on whitespace, honouring single and
// double quotes the way redis-cli does for the common cases.
func splitInline(line []byte) ([][]byte, error) {
	var args [][]byte
	i := 0
	for i < len(line) {
		for i < len(line) && isSpace(line[i]) {
			i++
		}
		if i >= len(line) {
			break
		}
		var tok []byte
		switch line[i] {
		case '"', '\'':
			q := line[i]
			i++
			closed := false
			for i < len(line) {
				c := line[i]
				if c == '\\' && q == '"' && i+1 < len(line) {
					i++
					switch line[i] {
					case 'n':
						tok = append(tok, '\n')
					case 'r':
						tok = append(tok, '\r')
					case 't':
						tok = append(tok, '\t')
					default:
						tok = append(tok, line[i])
					}
					i++
					continue
				}
				if c == q {
					closed = true
					i++
					break
				}
				tok = append(tok, c)
				i++
			}
			if !closed || (i < len(line) && !isSpace(line[i])) {
				return nil, protoErr("unbalanced quotes in request")
			}
		default:
			start := i
			for i < len(line) && !isSpace(line[i]) {
				i++
			}
			tok = bytes.Clone(line[start:i])
		}
		args = append(args, tok)
	}
	return args, nil
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' || c == '\n' }
