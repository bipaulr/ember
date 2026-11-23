// Package aof implements the append-only file: logging write commands in RESP
// format, replaying them at startup, and compacting the file with a rewrite.
package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"ember/internal/command"
	"ember/internal/resp"
)

// Policy controls when the file is fsynced.
type Policy int

// Fsync policies.
const (
	// EverySec fsyncs in the background once per second (the default).
	EverySec Policy = iota
	// Always makes every write durable before its reply is sent.
	Always
	// No never fsyncs; the operating system decides.
	No
)

func (p Policy) String() string {
	switch p {
	case Always:
		return "always"
	case No:
		return "no"
	}
	return "everysec"
}

// ParsePolicy converts a flag value to a Policy.
func ParsePolicy(s string) (Policy, error) {
	switch s {
	case "always":
		return Always, nil
	case "everysec", "":
		return EverySec, nil
	case "no":
		return No, nil
	}
	return 0, fmt.Errorf("invalid appendfsync %q (want always, everysec or no)", s)
}

// Defaults for automatic rewrite.
const (
	DefaultRewriteMinSize = 64 << 20
	DefaultRewriteRatio   = 2.0
	tickInterval          = 100 * time.Millisecond
	rewriteChunk          = 1 << 20
)

// Snapshotter is what a rewrite reads from. The store implements it.
type Snapshotter interface {
	NumShards() int
	// SnapshotShard calls mark under the shard's read lock before emitting the
	// commands that rebuild the shard.
	SnapshotShard(i int, mark func(), emit func(args [][]byte))
}

// Options configure Open.
type Options struct {
	Path   string
	Policy Policy
	Shards int
	Logger *slog.Logger
	// RewriteMinSize and RewriteRatio control the automatic rewrite: it starts
	// when the file is at least RewriteMinSize bytes and RewriteRatio times its
	// size after the previous rewrite. A negative RewriteMinSize disables it.
	RewriteMinSize int64
	RewriteRatio   float64
}

var errRewriteAborted = errors.New("rewrite aborted")

// AOF is an open append-only file. All methods are safe for concurrent use.
type AOF struct {
	opts Options
	log  *slog.Logger
	src  Snapshotter

	seq atomic.Uint64 // number of records appended so far

	mu       sync.Mutex
	cond     *sync.Cond
	f        *os.File
	pending  []byte // encoded records not yet written to f
	synced   uint64 // records known durable (Always policy)
	flushing bool   // a group-commit leader is writing outside the lock
	dirty    bool   // bytes written but not fsynced
	size     int64  // logical file size including pending
	baseSize int64  // size after the last rewrite (or at startup)
	err      error  // sticky write error
	closed   bool

	rewriting      bool
	rewriteAborted bool
	rbuf           [][]byte // per-shard records appended since the shard's snapshot
	rewrites       int64
	lastRewriteOK  bool

	stop chan struct{}
	wg   sync.WaitGroup
}

// Open opens (creating if needed) the AOF at opts.Path for appending. Replay
// the file first with Replay; Open must come after.
func Open(opts Options) (*AOF, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.RewriteRatio <= 0 {
		opts.RewriteRatio = DefaultRewriteRatio
	}
	if opts.RewriteMinSize == 0 {
		opts.RewriteMinSize = DefaultRewriteMinSize
	}
	_ = os.Remove(tempPath(opts.Path)) // leftover from a crash mid-rewrite
	f, err := os.OpenFile(opts.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	a := &AOF{opts: opts, log: opts.Logger, f: f, size: st.Size(), baseSize: st.Size(),
		lastRewriteOK: true, stop: make(chan struct{})}
	a.cond = sync.NewCond(&a.mu)
	a.wg.Add(1)
	go a.loop()
	return a, nil
}

func tempPath(path string) string { return path + ".rewrite.tmp" }

// SetSource tells the AOF where to read the keyspace from for rewrites.
func (a *AOF) SetSource(s Snapshotter) { a.src = s }

// Append encodes args and queues them for writing. It is called with the
// owning shard's lock held, which is what guarantees that the order of records
// in the file matches the order in which writes were applied to each key. It
// does no I/O.
func (a *AOF) Append(shard int, args [][]byte) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	start := len(a.pending)
	a.pending = resp.AppendCommand(a.pending, args...)
	n := int64(len(a.pending) - start)
	a.size += n
	if a.rewriting {
		if shard < 0 {
			// FLUSHALL cannot be ordered against per-shard snapshots taken at
			// different moments, so the rewrite is abandoned.
			a.rewriteAborted = true
		} else {
			a.rbuf[shard] = append(a.rbuf[shard], a.pending[start:]...)
		}
	}
	a.seq.Add(1)
	a.mu.Unlock()
}

// Seq returns the number of records appended so far.
func (a *AOF) Seq() uint64 { return a.seq.Load() }

// Commit makes everything appended so far visible to the operating system, and
// under the Always policy durable. The server calls it before sending replies
// to a client whose commands wrote data. Concurrent callers share one fsync
// (group commit): the first becomes the leader and syncs for everyone waiting.
func (a *AOF) Commit() error {
	target := a.seq.Load()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.opts.Policy != Always {
		return a.writeLocked()
	}
	for a.synced < target {
		if a.err != nil {
			return a.err
		}
		if a.flushing {
			a.cond.Wait()
			continue
		}
		a.flushing = true
		data := a.pending
		a.pending = nil
		upTo := a.seq.Load()
		f := a.f
		a.mu.Unlock()
		err := writeAndSync(f, data)
		a.mu.Lock()
		a.flushing = false
		if err != nil {
			a.err = err
		} else {
			a.synced = upTo
		}
		a.cond.Broadcast()
	}
	return a.err
}

func writeAndSync(f *os.File, data []byte) error {
	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			return err
		}
	}
	return f.Sync()
}

// writeLocked writes pending bytes to the OS. The caller holds mu.
func (a *AOF) writeLocked() error {
	for a.flushing {
		a.cond.Wait()
	}
	if a.err != nil {
		return a.err
	}
	if len(a.pending) == 0 {
		return nil
	}
	if _, err := a.f.Write(a.pending); err != nil {
		a.err = err
		a.log.Error("aof write failed", "err", err)
		return err
	}
	a.pending = a.pending[:0]
	a.dirty = true
	return nil
}

// loop is the background housekeeper: it pushes records appended without a
// client (expiry deletes) to the OS, fsyncs once a second under everysec, and
// starts automatic rewrites.
func (a *AOF) loop() {
	defer a.wg.Done()
	t := time.NewTicker(tickInterval)
	defer t.Stop()
	lastSync := time.Now()
	for {
		select {
		case <-a.stop:
			return
		case <-t.C:
		}
		a.mu.Lock()
		_ = a.writeLocked()
		f, dirty, pol := a.f, a.dirty, a.opts.Policy
		needSync := dirty && (pol == Always || (pol == EverySec && time.Since(lastSync) >= time.Second))
		if needSync {
			a.dirty = false
		}
		auto := a.shouldRewriteLocked()
		a.mu.Unlock()
		if needSync {
			lastSync = time.Now()
			if err := f.Sync(); err != nil {
				a.mu.Lock()
				if a.f == f { // not merely closed by a concurrent rewrite swap
					a.dirty = true
					a.log.Error("aof fsync failed", "err", err)
				}
				a.mu.Unlock()
			}
		}
		if auto {
			a.log.Info("starting automatic AOF rewrite")
			_ = a.StartRewrite()
		}
	}
}

func (a *AOF) shouldRewriteLocked() bool {
	if a.rewriting || a.closed || a.src == nil || a.opts.RewriteMinSize < 0 {
		return false
	}
	return a.size >= a.opts.RewriteMinSize && float64(a.size) >= a.opts.RewriteRatio*float64(max(a.baseSize, 1))
}

// Info reports state for the INFO command.
func (a *AOF) Info() command.PersistInfo {
	a.mu.Lock()
	defer a.mu.Unlock()
	return command.PersistInfo{
		Path: a.opts.Path, Policy: a.opts.Policy.String(), Size: a.size, BaseSize: a.baseSize,
		RewriteInProgress: a.rewriting, Rewrites: a.rewrites, LastRewriteOK: a.lastRewriteOK,
	}
}

// Close writes and fsyncs everything outstanding, then closes the file. A
// rewrite in progress is abandoned (the old file stays valid).
func (a *AOF) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	close(a.stop)
	a.mu.Unlock()
	a.wg.Wait()

	a.mu.Lock()
	defer a.mu.Unlock()
	err := a.writeLocked()
	if serr := a.f.Sync(); err == nil {
		err = serr
	}
	if cerr := a.f.Close(); err == nil {
		err = cerr
	}
	a.cond.Broadcast()
	return err
}

// StartRewrite begins a background rewrite and returns at once.
func (a *AOF) StartRewrite() error {
	a.mu.Lock()
	switch {
	case a.closed:
		a.mu.Unlock()
		return errors.New("AOF is closed")
	case a.src == nil:
		a.mu.Unlock()
		return errors.New("AOF has no snapshot source")
	case a.rewriting:
		a.mu.Unlock()
		return command.ErrRewriteInProgress
	}
	a.rewriting = true
	a.rewriteAborted = false
	a.rbuf = make([][]byte, a.src.NumShards())
	a.wg.Add(1)
	a.mu.Unlock()

	go func() {
		defer a.wg.Done()
		start := time.Now()
		err := a.rewrite()
		// Remove a leftover temp file while this rewrite still owns it. Once
		// rewriting is false a new rewrite may create the same path, and a late
		// Remove would delete its file (Linux allows unlinking an open file),
		// making its rename fail.
		_ = os.Remove(tempPath(a.opts.Path))
		a.mu.Lock()
		a.rewriting = false
		a.rbuf = nil
		a.lastRewriteOK = err == nil
		if err == nil {
			a.rewrites++
		}
		size := a.size
		a.mu.Unlock()
		if err != nil {
			a.log.Warn("aof rewrite failed", "err", err)
		} else {
			a.log.Info("aof rewrite done", "size", size, "took", time.Since(start).Round(time.Millisecond))
		}
	}()
	return nil
}

// Rewrite runs a rewrite and waits for it to finish.
func (a *AOF) Rewrite() error {
	if err := a.StartRewrite(); err != nil {
		return err
	}
	for {
		a.mu.Lock()
		done := !a.rewriting
		ok := a.lastRewriteOK
		a.mu.Unlock()
		if done {
			if !ok {
				return errors.New("aof rewrite failed")
			}
			return nil
		}
		time.Sleep(time.Millisecond)
	}
}

// rewrite builds a compacted file from the live keyspace and swaps it in.
//
// Consistency: every write is appended to the log while holding its shard lock,
// and during a rewrite also to rbuf[shard]. Each shard is snapshotted under its
// read lock, and at that instant rbuf[shard] is cleared, because the snapshot
// already contains those writes. What remains in rbuf[shard] afterwards is
// exactly the writes the snapshot lacks. The new file is therefore, for every
// shard, "snapshot, then later writes in order", which replays to the same
// state as the old file.
func (a *AOF) rewrite() error {
	tmp := tempPath(a.opts.Path)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	bw := bufio.NewWriterSize(f, rewriteChunk)
	var written int64
	var chunk []byte

	for i := 0; i < a.src.NumShards(); i++ {
		a.mu.Lock()
		stop := a.closed || a.rewriteAborted
		a.mu.Unlock()
		if stop {
			return errRewriteAborted
		}
		chunk = chunk[:0]
		a.src.SnapshotShard(i, func() {
			a.mu.Lock()
			a.rbuf[i] = a.rbuf[i][:0]
			a.mu.Unlock()
		}, func(args [][]byte) {
			chunk = resp.AppendCommand(chunk, args...)
		})
		n, err := bw.Write(chunk)
		if err != nil {
			return err
		}
		written += int64(n)
	}

	// Finalise with appends blocked. Appends hold shard locks while waiting for
	// mu, so this pause is as long as the tail copy and the swap.
	a.mu.Lock()
	defer a.mu.Unlock()
	for a.flushing {
		a.cond.Wait()
	}
	if a.closed || a.rewriteAborted {
		return errRewriteAborted
	}
	for _, b := range a.rbuf {
		n, err := bw.Write(b)
		if err != nil {
			return err
		}
		written += int64(n)
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	// Close the old file before renaming: Windows cannot replace an open file.
	if err := a.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, a.opts.Path); err != nil {
		// Keep serving from the old file.
		if nf, oerr := os.OpenFile(a.opts.Path, os.O_WRONLY|os.O_APPEND, 0o644); oerr == nil {
			a.f = nf
		}
		return err
	}
	syncDir(filepath.Dir(a.opts.Path))
	nf, err := os.OpenFile(a.opts.Path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		a.err = err
		return err
	}
	a.f = nf
	// Everything appended so far is in the new file, written or still pending.
	a.pending = a.pending[:0]
	a.size = written
	a.baseSize = written
	a.dirty = false
	a.synced = a.seq.Load()
	a.cond.Broadcast()
	return nil
}

// syncDir fsyncs a directory so a rename survives power loss. It is best effort
// and does nothing useful on Windows.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}
