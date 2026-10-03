package aof

import (
	"errors"
	"fmt"
	"io"
	"os"

	"ember/internal/resp"
)

// ReplayResult describes what Replay found.
type ReplayResult struct {
	Commands  int   // commands applied
	Size      int64 // size of the valid prefix, which is the file size after repair
	Truncated bool  // a partial final entry was cut off
	// DiscardedBytes is how many bytes were removed from the tail.
	DiscardedBytes int64
}

// CorruptError reports damage that is not a clean truncation at the end.
type CorruptError struct {
	Path   string
	Offset int64
	Err    error
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("aof %s is corrupt at byte offset %d: %v", e.Path, e.Offset, e.Err)
}

func (e *CorruptError) Unwrap() error { return e.Err }

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Replay reads the AOF at path and calls apply for each command, in order. A
// missing file is not an error.
//
// If the file ends in the middle of an entry, which is what a crash during a
// write leaves behind, that partial entry is cut off from the file and replay
// succeeds. Any other damage (malformed protocol in the middle, or a command
// that apply rejects) returns a *CorruptError and leaves the file untouched,
// so the operator can inspect it.
func Replay(path string, apply func(args [][]byte) error) (ReplayResult, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return ReplayResult{}, nil
	}
	if err != nil {
		return ReplayResult{}, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return ReplayResult{}, err
	}
	total := st.Size()

	cr := &countingReader{r: f}
	r := resp.NewReader(cr)
	var res ReplayResult
	var truncated bool
	for {
		args, err := r.ReadCommand()
		if err == io.EOF {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			truncated = true
			break
		}
		if err != nil {
			_ = f.Close()
			return res, &CorruptError{Path: path, Offset: cr.n - int64(r.Buffered()), Err: err}
		}
		if len(args) == 0 {
			res.Size = cr.n - int64(r.Buffered())
			continue
		}
		if err := apply(args); err != nil {
			_ = f.Close()
			return res, &CorruptError{Path: path, Offset: res.Size, Err: fmt.Errorf("command %q: %w", args[0], err)}
		}
		res.Commands++
		res.Size = cr.n - int64(r.Buffered())
	}
	_ = f.Close()

	if truncated || res.Size < total {
		res.Truncated = true
		res.DiscardedBytes = total - res.Size
		if err := os.Truncate(path, res.Size); err != nil {
			return res, fmt.Errorf("truncating damaged aof tail: %w", err)
		}
	}
	return res, nil
}
