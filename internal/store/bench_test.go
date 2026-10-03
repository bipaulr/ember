package store

import (
	"fmt"
	"sync"
	"testing"
)

const benchKeys = 100_000

func benchStore(shards int) (*Store, []string) {
	s := New(shards)
	keys := make([]string, benchKeys)
	val := []byte("value-value-value-value-value-value-value-value-")
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%012d", i)
		k := keys[i]
		s.Update(k, func(tx *Tx) { tx.Put(&Entry{Kind: KindString, Str: val}) })
	}
	return s, keys
}

func BenchmarkStoreGet(b *testing.B) {
	s, keys := benchStore(DefaultShards)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.View(keys[i%benchKeys], func(e *Entry) { _ = e })
	}
}

func BenchmarkStoreSet(b *testing.B) {
	s, keys := benchStore(DefaultShards)
	val := []byte("value-value-value-value-value-value-value-value-")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Update(keys[i%benchKeys], func(tx *Tx) { tx.Put(&Entry{Kind: KindString, Str: val}) })
	}
}

// BenchmarkContention compares shard counts under a parallel 90% read / 10%
// write mix. shards=1 is the single-mutex design.
func BenchmarkContention(b *testing.B) {
	for _, shards := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("shards=%d", shards), func(b *testing.B) {
			s, keys := benchStore(shards)
			val := []byte("value-value-value-value-value-value-value-value-")
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					i += 7919
					k := keys[i%benchKeys]
					if i%10 == 0 {
						s.Update(k, func(tx *Tx) { tx.Put(&Entry{Kind: KindString, Str: val}) })
					} else {
						s.View(k, func(e *Entry) { _ = e })
					}
				}
			})
		})
	}
}

// BenchmarkSyncMapContention is the same mix on a sync.Map, for the design notes.
func BenchmarkSyncMapContention(b *testing.B) {
	var m sync.Map
	keys := make([]string, benchKeys)
	val := &Entry{Kind: KindString, Str: []byte("value-value-value-value-value-value-value-value-")}
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%012d", i)
		m.Store(keys[i], val)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i += 7919
			k := keys[i%benchKeys]
			if i%10 == 0 {
				m.Store(k, &Entry{Kind: KindString, Str: val.Str})
			} else {
				m.Load(k)
			}
		}
	})
}

// BenchmarkMutexMapContention is a plain map behind one RWMutex, without any of
// the store's expiry bookkeeping, as the lower bound for the single-lock design.
func BenchmarkMutexMapContention(b *testing.B) {
	var mu sync.RWMutex
	m := make(map[string]*Entry)
	keys := make([]string, benchKeys)
	val := &Entry{Kind: KindString, Str: []byte("value-value-value-value-value-value-value-value-")}
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%012d", i)
		m[keys[i]] = val
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i += 7919
			k := keys[i%benchKeys]
			if i%10 == 0 {
				mu.Lock()
				m[k] = &Entry{Kind: KindString, Str: val.Str}
				mu.Unlock()
			} else {
				mu.RLock()
				_ = m[k]
				mu.RUnlock()
			}
		}
	})
}
