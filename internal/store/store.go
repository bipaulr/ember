// Package store implements the in-memory keyspace: a hash-sharded map of typed
// values with per-key expiry.
package store

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultShards is the shard count used when none is configured.
const DefaultShards = 256

// Kind is the data type stored under a key.
type Kind uint8

// Supported value kinds.
const (
	KindString Kind = iota + 1
	KindList
)

// String returns the name reported by the TYPE command.
func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	}
	return "none"
}

// Entry is one stored value. Handlers may modify Str and List in place while
// holding the shard lock through a Tx, but must never mutate the bytes of a
// slice that was already handed out. Replace the slice instead.
type Entry struct {
	Kind Kind
	Str  []byte
	List *List
	// ExpireAt is an absolute Unix time in milliseconds, or 0 for no expiry.
	// Only Tx.SetExpire may change it, because the shard keeps an index of
	// keys that have a TTL.
	ExpireAt int64
}

// Propagator receives the canonical form of every successful write, in the
// exact order writes were applied to the shard. It is called with the shard
// lock held. shard is the shard index, or -1 for commands that affect all
// shards (FLUSHALL).
type Propagator func(shard int, args [][]byte)

type shard struct {
	mu       sync.RWMutex
	m        map[string]*Entry
	ttlKeys  []string       // keys that have an expiry, for random sampling
	ttlPos   map[string]int // key -> index in ttlKeys
	ttlCount atomic.Int32   // len(ttlKeys), readable without the lock
}

// Store is the sharded keyspace.
type Store struct {
	shards  []*shard
	clock   func() int64
	prop    Propagator
	expired atomic.Int64
	loading atomic.Bool
}

// New creates a store with n shards (DefaultShards if n <= 0).
func New(n int) *Store { return NewWithClock(n, nil) }

// NewWithClock is like New but uses clock (Unix ms) as the time source.
func NewWithClock(n int, clock func() int64) *Store {
	if n <= 0 {
		n = DefaultShards
	}
	if clock == nil {
		clock = func() int64 { return time.Now().UnixMilli() }
	}
	s := &Store{shards: make([]*shard, n), clock: clock}
	for i := range s.shards {
		s.shards[i] = &shard{m: make(map[string]*Entry), ttlPos: make(map[string]int)}
	}
	return s
}

// SetPropagator installs the write-ahead hook. Call it before serving.
func (s *Store) SetPropagator(p Propagator) { s.prop = p }

// Now returns the store clock in Unix milliseconds. While the store is loading
// (AOF replay) time is frozen at 0, so nothing expires mid-replay; see SetLoading.
func (s *Store) Now() int64 { return s.now() }

func (s *Store) now() int64 {
	if s.loading.Load() {
		return 0
	}
	return s.clock()
}

// SetLoading freezes expiry while an AOF is replayed. A command in the log was
// valid at the time it ran, so replaying it must not see keys as expired just
// because the server was down for a while. Expired keys are removed normally
// once loading ends.
func (s *Store) SetLoading(on bool) { s.loading.Store(on) }

// NumShards returns the shard count.
func (s *Store) NumShards() int { return len(s.shards) }

// ExpiredKeys returns the total number of keys removed because they expired.
func (s *Store) ExpiredKeys() int64 { return s.expired.Load() }

// hash is 32-bit FNV-1a, computed inline to avoid allocating a hash.Hash.
func hash(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

func (s *Store) shardIndex(key string) int { return int(hash(key) % uint32(len(s.shards))) }

// Tx is a handle for one key while its shard is locked. It must not be used
// after the callback that received it returns.
type Tx struct {
	s   *Store
	sh  *shard
	idx int
	key string
}

// Update runs fn with the key's shard exclusively locked.
func (s *Store) Update(key string, fn func(tx *Tx)) {
	idx := s.shardIndex(key)
	sh := s.shards[idx]
	sh.mu.Lock()
	tx := Tx{s: s, sh: sh, idx: idx, key: key}
	fn(&tx)
	sh.mu.Unlock()
}

// View runs fn with the live entry for key, or nil if absent or expired. The
// shard is read-locked unless the key has expired and must be removed, in which
// case it is write-locked. The entry must not be retained or modified.
func (s *Store) View(key string, fn func(e *Entry)) {
	idx := s.shardIndex(key)
	sh := s.shards[idx]
	sh.mu.RLock()
	e := sh.m[key]
	if e == nil {
		fn(nil)
		sh.mu.RUnlock()
		return
	}
	if e.ExpireAt == 0 || s.now() < e.ExpireAt {
		fn(e)
		sh.mu.RUnlock()
		return
	}
	sh.mu.RUnlock()
	// Lazy expiry: upgrade to a write lock and remove the key.
	sh.mu.Lock()
	tx := Tx{s: s, sh: sh, idx: idx, key: key}
	fn(tx.Entry())
	sh.mu.Unlock()
}

// Entry returns the live entry, or nil. If the key has expired it is deleted
// here (lazy expiry) and a DEL is propagated so the AOF stays in sync.
func (tx *Tx) Entry() *Entry {
	e := tx.sh.m[tx.key]
	if e == nil {
		return nil
	}
	if e.ExpireAt != 0 && tx.s.now() >= e.ExpireAt {
		tx.sh.remove(tx.key, e)
		tx.s.expired.Add(1)
		tx.Log([]byte("DEL"), []byte(tx.key))
		return nil
	}
	return e
}

// Put stores e under the key, replacing any existing value. Any previous TTL is
// dropped, then e.ExpireAt (if non-zero) is applied.
func (tx *Tx) Put(e *Entry) {
	if old := tx.sh.m[tx.key]; old != nil {
		tx.sh.untrack(tx.key, old)
	}
	tx.sh.m[tx.key] = e
	if e.ExpireAt != 0 {
		tx.sh.track(tx.key)
	}
}

// Delete removes the key and reports whether a live entry existed.
func (tx *Tx) Delete() bool {
	e := tx.Entry()
	if e == nil {
		return false
	}
	tx.sh.remove(tx.key, e)
	return true
}

// SetExpire sets the absolute expiry in Unix ms on an existing live entry; 0
// removes the TTL.
func (tx *Tx) SetExpire(e *Entry, at int64) {
	switch {
	case at == 0 && e.ExpireAt != 0:
		tx.sh.untrack(tx.key, e)
		e.ExpireAt = 0
	case at != 0:
		if e.ExpireAt == 0 {
			tx.sh.track(tx.key)
		}
		e.ExpireAt = at
	}
}

// Logging reports whether writes are being propagated, so callers can skip
// building arguments when no AOF is attached.
func (tx *Tx) Logging() bool { return tx.s.prop != nil }

// Log propagates a canonical write command while the shard lock is held.
func (tx *Tx) Log(args ...[]byte) {
	if tx.s.prop != nil {
		tx.s.prop(tx.idx, args)
	}
}

// remove deletes the entry and its TTL index slot. The caller holds the lock.
func (sh *shard) remove(key string, e *Entry) {
	sh.untrack(key, e)
	delete(sh.m, key)
}

func (sh *shard) track(key string) {
	if _, ok := sh.ttlPos[key]; ok {
		return
	}
	sh.ttlPos[key] = len(sh.ttlKeys)
	sh.ttlKeys = append(sh.ttlKeys, key)
	sh.ttlCount.Store(int32(len(sh.ttlKeys)))
}

func (sh *shard) untrack(key string, e *Entry) {
	if e.ExpireAt == 0 {
		return
	}
	pos, ok := sh.ttlPos[key]
	if !ok {
		return
	}
	last := len(sh.ttlKeys) - 1
	moved := sh.ttlKeys[last]
	sh.ttlKeys[pos] = moved
	sh.ttlPos[moved] = pos
	sh.ttlKeys = sh.ttlKeys[:last]
	delete(sh.ttlPos, key)
	sh.ttlCount.Store(int32(len(sh.ttlKeys)))
}

// Len returns the number of keys, including ones that expired but have not
// been removed yet (the same as Redis DBSIZE).
func (s *Store) Len() int {
	n := 0
	for _, sh := range s.shards {
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

// TTLKeys returns how many keys currently carry an expiry.
func (s *Store) TTLKeys() int {
	n := 0
	for _, sh := range s.shards {
		n += int(sh.ttlCount.Load())
	}
	return n
}

// Keys returns every live key for which match returns true.
func (s *Store) Keys(match func(key string) bool) []string {
	var out []string
	now := s.now()
	for _, sh := range s.shards {
		sh.mu.RLock()
		for k, e := range sh.m {
			if e.ExpireAt != 0 && now >= e.ExpireAt {
				continue
			}
			if match(k) {
				out = append(out, k)
			}
		}
		sh.mu.RUnlock()
	}
	return out
}

// FlushAll removes every key. It locks all shards in index order so that the
// single propagated FLUSHALL is ordered correctly against every other write.
func (s *Store) FlushAll() {
	for _, sh := range s.shards {
		sh.mu.Lock()
	}
	for _, sh := range s.shards {
		sh.m = make(map[string]*Entry)
		sh.ttlKeys = nil
		sh.ttlPos = make(map[string]int)
		sh.ttlCount.Store(0)
	}
	if s.prop != nil {
		s.prop(-1, [][]byte{[]byte("FLUSHALL")})
	}
	for i := len(s.shards) - 1; i >= 0; i-- {
		s.shards[i].mu.Unlock()
	}
}

// ExpireCycle is one round of active expiry, modelled on Redis. It samples up
// to sampleSize random keys that have a TTL, deletes the expired ones, and
// repeats while more than 25% of a sample was expired or until budget runs out.
// It returns how many keys it deleted.
func (s *Store) ExpireCycle(sampleSize int, budget time.Duration) int {
	deadline := time.Now().Add(budget)
	total := 0
	for {
		sampled, expired := s.sampleOnce(sampleSize)
		total += expired
		if sampled == 0 || expired*4 <= sampled {
			return total
		}
		if !time.Now().Before(deadline) {
			return total
		}
	}
}

func (s *Store) sampleOnce(n int) (sampled, expired int) {
	for i := 0; i < n; i++ {
		sh := s.pickTTLShard()
		if sh == nil {
			return sampled, expired
		}
		sh.mu.Lock()
		if len(sh.ttlKeys) == 0 {
			sh.mu.Unlock()
			continue
		}
		key := sh.ttlKeys[rand.Intn(len(sh.ttlKeys))]
		e := sh.m[key]
		sampled++
		if e != nil && e.ExpireAt != 0 && s.now() >= e.ExpireAt {
			sh.remove(key, e)
			s.expired.Add(1)
			expired++
			if s.prop != nil {
				s.prop(s.shardIndex(key), [][]byte{[]byte("DEL"), []byte(key)})
			}
		}
		sh.mu.Unlock()
	}
	return sampled, expired
}

// pickTTLShard starts at a random shard and returns the first one that has keys
// with a TTL, so a handful of expiring keys spread over many shards are still
// found quickly.
func (s *Store) pickTTLShard() *shard {
	n := len(s.shards)
	start := rand.Intn(n)
	for j := 0; j < n; j++ {
		sh := s.shards[(start+j)%n]
		if sh.ttlCount.Load() > 0 {
			return sh
		}
	}
	return nil
}

// RunExpiry runs the active expiry cycle every interval until stop is closed.
func (s *Store) RunExpiry(interval time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
			s.ExpireCycle(20, 25*time.Millisecond)
		}
	}
}

// SnapshotShard emits the commands that rebuild shard i. mark is called while
// the shard's read lock is held, before any command is emitted; the AOF uses it
// to discard buffered writes that the snapshot already contains.
func (s *Store) SnapshotShard(i int, mark func(), emit func(args [][]byte)) {
	sh := s.shards[i]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	if mark != nil {
		mark()
	}
	now := s.now()
	for k, e := range sh.m {
		if e.ExpireAt != 0 && now >= e.ExpireAt {
			continue
		}
		key := []byte(k)
		switch e.Kind {
		case KindString:
			emit([][]byte{[]byte("SET"), key, e.Str})
		case KindList:
			const chunk = 512
			for start := 0; start < e.List.Len(); start += chunk {
				end := min(start+chunk, e.List.Len())
				args := make([][]byte, 0, end-start+2)
				args = append(args, []byte("RPUSH"), key)
				for j := start; j < end; j++ {
					args = append(args, e.List.At(j))
				}
				emit(args)
			}
		}
		if e.ExpireAt != 0 {
			emit([][]byte{[]byte("PEXPIREAT"), key, itoa(e.ExpireAt)})
		}
	}
}

func itoa(n int64) []byte {
	var b [20]byte
	i := len(b)
	neg := n < 0
	u := uint64(n)
	if neg {
		u = -u
	}
	for {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
		if u == 0 {
			break
		}
	}
	if neg {
		i--
		b[i] = '-'
	}
	return append([]byte(nil), b[i:]...)
}
