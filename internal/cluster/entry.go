package cluster

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"

	"ember/internal/resp"
)

// A log entry's payload is:
//
//	8 bytes   the leader's clock when the command was proposed (Unix ms, big endian)
//	the rest  the command as a RESP array, exactly as a client would send it;
//	          empty for a clock tick
//
// Reusing RESP means the log holds the same bytes the AOF would.

func encodeEntry(timestamp int64, args [][]byte) []byte {
	data := binary.BigEndian.AppendUint64(nil, uint64(timestamp))
	if len(args) > 0 {
		data = resp.AppendCommand(data, args...)
	}
	return data
}

func decodeEntry(data []byte) (timestamp int64, args [][]byte, err error) {
	if len(data) < 8 {
		return 0, nil, errors.New("shorter than the 8-byte timestamp")
	}
	timestamp = int64(binary.BigEndian.Uint64(data))
	if len(data) == 8 {
		return timestamp, nil, nil
	}
	args, err = resp.NewReader(bytes.NewReader(data[8:])).ReadCommand()
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return timestamp, args, err
}

// Clock is the time source for a clustered node's store. It does not read the
// machine's clock: it reports the timestamp of the latest applied log entry.
//
// Why: commands such as SET k v EX 10, or any command that touches a key
// whose TTL has run out, depend on "now". If each node asked its own clock,
// two nodes applying the same entry could disagree on whether a key had
// expired, and their stores would diverge. With time taken from the log, every
// node sees the same "now" for the same entry.
type Clock struct {
	ms atomic.Int64
}

// Now returns the log time in Unix milliseconds. Pass this method to
// store.NewWithClock.
func (c *Clock) Now() int64 { return c.ms.Load() }

// advance moves the clock to t. It never moves backwards: a new leader whose
// machine clock is behind the old leader's must not make expired keys reappear.
// Only the apply goroutine calls it.
func (c *Clock) advance(t int64) {
	if t > c.ms.Load() {
		c.ms.Store(t)
	}
}
