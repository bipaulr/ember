package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer appends RESP2 replies to a buffered writer. Nothing reaches the
// network until Flush, which lets the server batch replies for pipelined input.
type Writer struct {
	bw      *bufio.Writer
	scratch [24]byte
}

// NewWriter wraps w with a buffered writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 16*1024)}
}

// Flush sends buffered replies.
func (w *Writer) Flush() error { return w.bw.Flush() }

// Buffered returns the number of unflushed bytes.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

func (w *Writer) line(prefix byte, s string) {
	w.bw.WriteByte(prefix)
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// WriteSimple writes +s.
func (w *Writer) WriteSimple(s string) { w.line('+', s) }

// WriteError writes -s. The caller supplies the full text, e.g. "ERR bad".
func (w *Writer) WriteError(s string) { w.line('-', s) }

// WriteInt writes :n.
func (w *Writer) WriteInt(n int64) {
	w.bw.WriteByte(':')
	w.bw.Write(strconv.AppendInt(w.scratch[:0], n, 10))
	w.bw.WriteString("\r\n")
}

// WriteBulk writes a bulk string.
func (w *Writer) WriteBulk(b []byte) {
	w.bw.WriteByte('$')
	w.bw.Write(strconv.AppendInt(w.scratch[:0], int64(len(b)), 10))
	w.bw.WriteString("\r\n")
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

// WriteBulkString writes a bulk string from a Go string.
func (w *Writer) WriteBulkString(s string) {
	w.bw.WriteByte('$')
	w.bw.Write(strconv.AppendInt(w.scratch[:0], int64(len(s)), 10))
	w.bw.WriteString("\r\n")
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

// WriteNull writes the null bulk string ($-1).
func (w *Writer) WriteNull() { w.bw.WriteString("$-1\r\n") }

// WriteNullArray writes the null array (*-1).
func (w *Writer) WriteNullArray() { w.bw.WriteString("*-1\r\n") }

// WriteArrayHeader starts an array of n elements.
func (w *Writer) WriteArrayHeader(n int) {
	w.bw.WriteByte('*')
	w.bw.Write(strconv.AppendInt(w.scratch[:0], int64(n), 10))
	w.bw.WriteString("\r\n")
}

// WriteRaw appends pre-encoded RESP bytes.
func (w *Writer) WriteRaw(b []byte) { w.bw.Write(b) }

// AppendCommand appends args encoded as a RESP array of bulk strings to dst.
// The AOF uses it to serialise commands.
func AppendCommand(dst []byte, args ...[]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}
