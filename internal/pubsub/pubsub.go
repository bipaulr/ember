// Package pubsub routes published messages to subscribers.
//
// Slow-subscriber policy: every subscriber owns a bounded queue of encoded
// messages. Publish never blocks. If a subscriber's queue is full, or holds too
// many bytes, that subscriber is marked as overflowed and its connection is
// closed. One slow reader can therefore never stall PUBLISH or other clients,
// and server memory per subscriber is bounded.
package pubsub

import (
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"ember/internal/glob"
)

// Default queue limits per subscriber.
const (
	DefaultQueueMessages = 1024
	DefaultQueueBytes    = 8 << 20
)

// Subscriber is one client's inbox plus its subscription sets. The sets are
// owned by the connection goroutine; only C, the byte counter and the overflow
// flag are touched by publishers.
type Subscriber struct {
	// C carries fully encoded RESP messages for the connection's pump.
	C chan []byte

	maxBytes   int64
	queued     atomic.Int64
	overflowed atomic.Bool
	onOverflow func()
	done       chan struct{}
	closeOnce  sync.Once

	channels map[string]struct{}
	patterns map[string]struct{}
}

// NewSubscriber creates a subscriber. onOverflow is called at most once, from
// the publishing goroutine, and must not block or take hub locks; closing the
// connection is the intended action.
func NewSubscriber(maxMessages int, maxBytes int64, onOverflow func()) *Subscriber {
	if maxMessages <= 0 {
		maxMessages = DefaultQueueMessages
	}
	if maxBytes <= 0 {
		maxBytes = DefaultQueueBytes
	}
	return &Subscriber{
		C: make(chan []byte, maxMessages), maxBytes: maxBytes, onOverflow: onOverflow,
		done:     make(chan struct{}),
		channels: make(map[string]struct{}), patterns: make(map[string]struct{}),
	}
}

// Count returns the number of channels plus patterns this subscriber is on.
func (s *Subscriber) Count() int { return len(s.channels) + len(s.patterns) }

// Channels returns the subscribed channel names, sorted.
func (s *Subscriber) Channels() []string { return sortedKeys(s.channels) }

// Patterns returns the subscribed patterns, sorted.
func (s *Subscriber) Patterns() []string { return sortedKeys(s.patterns) }

// Done is closed when the subscriber is closed.
func (s *Subscriber) Done() <-chan struct{} { return s.done }

// Close signals the pump to stop. It is idempotent.
func (s *Subscriber) Close() { s.closeOnce.Do(func() { close(s.done) }) }

// Overflowed reports whether the subscriber was dropped for being too slow.
func (s *Subscriber) Overflowed() bool { return s.overflowed.Load() }

// Release must be called by the pump after it has written a message, to
// return its bytes to the queue budget.
func (s *Subscriber) Release(n int) { s.queued.Add(int64(-n)) }

func (s *Subscriber) enqueue(msg []byte) bool {
	if s.overflowed.Load() {
		return false
	}
	if s.queued.Add(int64(len(msg))) <= s.maxBytes {
		select {
		case s.C <- msg:
			return true
		default:
		}
	}
	s.queued.Add(int64(-len(msg)))
	if s.overflowed.CompareAndSwap(false, true) && s.onOverflow != nil {
		s.onOverflow()
	}
	return false
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Hub holds the channel and pattern registries.
type Hub struct {
	mu    sync.RWMutex
	chans map[string]map[*Subscriber]struct{}
	pats  map[string]map[*Subscriber]struct{}

	// Overflows counts subscribers dropped for being too slow.
	Overflows atomic.Int64
	// Published counts PUBLISH calls.
	Published atomic.Int64
}

// NewHub returns an empty hub.
func NewHub() *Hub {
	return &Hub{chans: make(map[string]map[*Subscriber]struct{}), pats: make(map[string]map[*Subscriber]struct{})}
}

// Subscribe adds s to channel and returns s's new subscription count.
func (h *Hub) Subscribe(s *Subscriber, channel string) int {
	h.mu.Lock()
	set := h.chans[channel]
	if set == nil {
		set = make(map[*Subscriber]struct{})
		h.chans[channel] = set
	}
	set[s] = struct{}{}
	h.mu.Unlock()
	s.channels[channel] = struct{}{}
	return s.Count()
}

// Unsubscribe removes s from channel and returns s's new subscription count.
func (h *Hub) Unsubscribe(s *Subscriber, channel string) int {
	h.mu.Lock()
	removeFrom(h.chans, channel, s)
	h.mu.Unlock()
	delete(s.channels, channel)
	return s.Count()
}

// PSubscribe adds s to a glob pattern.
func (h *Hub) PSubscribe(s *Subscriber, pattern string) int {
	h.mu.Lock()
	set := h.pats[pattern]
	if set == nil {
		set = make(map[*Subscriber]struct{})
		h.pats[pattern] = set
	}
	set[s] = struct{}{}
	h.mu.Unlock()
	s.patterns[pattern] = struct{}{}
	return s.Count()
}

// PUnsubscribe removes s from a pattern.
func (h *Hub) PUnsubscribe(s *Subscriber, pattern string) int {
	h.mu.Lock()
	removeFrom(h.pats, pattern, s)
	h.mu.Unlock()
	delete(s.patterns, pattern)
	return s.Count()
}

func removeFrom(m map[string]map[*Subscriber]struct{}, key string, s *Subscriber) {
	if set := m[key]; set != nil {
		delete(set, s)
		if len(set) == 0 {
			delete(m, key)
		}
	}
}

// RemoveAll drops every subscription of s. The server calls it when the
// connection closes so nothing is left behind in the hub.
func (h *Hub) RemoveAll(s *Subscriber) {
	h.mu.Lock()
	for ch := range s.channels {
		removeFrom(h.chans, ch, s)
	}
	for p := range s.patterns {
		removeFrom(h.pats, p, s)
	}
	h.mu.Unlock()
	clear(s.channels)
	clear(s.patterns)
}

// Publish delivers payload to every subscriber of channel and to every
// pattern subscriber whose pattern matches. It returns the number of
// deliveries queued. It never blocks on a subscriber.
func (h *Hub) Publish(channel string, payload []byte) int {
	h.Published.Add(1)
	n := 0
	h.mu.RLock()
	defer h.mu.RUnlock()
	if set := h.chans[channel]; len(set) > 0 {
		msg := encode(nil, "message", "", channel, payload)
		for s := range set {
			if s.enqueue(msg) {
				n++
			}
		}
	}
	for pat, set := range h.pats {
		if !glob.Match(pat, channel) {
			continue
		}
		msg := encode(nil, "pmessage", pat, channel, payload)
		for s := range set {
			if s.enqueue(msg) {
				n++
			}
		}
	}
	return n
}

// encode builds ["message", channel, payload] or ["pmessage", pattern, channel, payload].
func encode(dst []byte, kind, pattern, channel string, payload []byte) []byte {
	parts := 3
	if kind == "pmessage" {
		parts = 4
	}
	dst = append(dst, '*', byte('0'+parts), '\r', '\n')
	dst = appendBulk(dst, kind)
	if kind == "pmessage" {
		dst = appendBulk(dst, pattern)
	}
	dst = appendBulk(dst, channel)
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(payload)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, payload...)
	return append(dst, '\r', '\n')
}

func appendBulk(dst []byte, s string) []byte {
	dst = append(dst, '$')
	dst = strconv.AppendInt(dst, int64(len(s)), 10)
	dst = append(dst, '\r', '\n')
	dst = append(dst, s...)
	return append(dst, '\r', '\n')
}

// Channels lists active channels (those with at least one subscriber) that
// match pattern, or all if pattern is empty.
func (h *Hub) Channels(pattern string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []string
	for ch := range h.chans {
		if pattern == "" || glob.Match(pattern, ch) {
			out = append(out, ch)
		}
	}
	sort.Strings(out)
	return out
}

// NumSub returns the subscriber count of a channel (not counting patterns).
func (h *Hub) NumSub(channel string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.chans[channel])
}

// NumPat returns the number of distinct patterns with subscribers.
func (h *Hub) NumPat() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.pats)
}

// NumChannels returns the number of channels with subscribers.
func (h *Hub) NumChannels() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.chans)
}
