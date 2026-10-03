package command

import (
	"bytes"

	"ember/internal/store"
)

func registerLists(t *Table) {
	w, ro := []string{FlagWrite, FlagFast}, []string{FlagReadonly}
	t.add(Spec{Name: "lpush", Arity: -3, Flags: w, Handler: pushHandler(true), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "rpush", Arity: -3, Flags: w, Handler: pushHandler(false), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "lpop", Arity: -2, Flags: w, Handler: popHandler(true), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "rpop", Arity: -2, Flags: w, Handler: popHandler(false), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "llen", Arity: 2, Flags: []string{FlagReadonly, FlagFast}, Handler: cmdLLen, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "lrange", Arity: 4, Flags: ro, Handler: cmdLRange, FirstKey: 1, LastKey: 1, Step: 1})
}

func pushHandler(front bool) Handler {
	return func(c *Ctx) {
		c.Store.Update(string(c.Args[1]), func(tx *store.Tx) {
			e := tx.Entry()
			if e == nil {
				e = &store.Entry{Kind: store.KindList, List: store.NewList()}
				tx.Put(e)
			} else if e.Kind != store.KindList {
				c.W.WriteError(errWrongType)
				return
			}
			for _, v := range c.Args[2:] {
				if front {
					e.List.PushFront(bytes.Clone(v))
				} else {
					e.List.PushBack(bytes.Clone(v))
				}
			}
			if tx.Logging() {
				tx.Log(c.Args...)
			}
			c.W.WriteInt(int64(e.List.Len()))
		})
	}
}

func popHandler(front bool) Handler {
	return func(c *Ctx) {
		if len(c.Args) > 3 {
			c.W.WriteError("ERR wrong number of arguments for '" + lowerName(c.Args[0]) + "' command")
			return
		}
		count, withCount := int64(1), len(c.Args) == 3
		if withCount {
			n, ok := parseInt(c.Args[2])
			if !ok || n < 0 {
				c.W.WriteError(errOutOfRange)
				return
			}
			count = n
		}
		c.Store.Update(string(c.Args[1]), func(tx *store.Tx) {
			e := tx.Entry()
			if e == nil {
				c.Client.Misses.Add(1)
				if withCount {
					c.W.WriteNullArray()
				} else {
					c.W.WriteNull()
				}
				return
			}
			if e.Kind != store.KindList {
				c.W.WriteError(errWrongType)
				return
			}
			n := int(min(count, int64(e.List.Len())))
			if withCount {
				c.W.WriteArrayHeader(n)
			}
			for i := 0; i < n; i++ {
				var v []byte
				if front {
					v = e.List.PopFront()
				} else {
					v = e.List.PopBack()
				}
				c.W.WriteBulk(v)
			}
			if e.List.Len() == 0 {
				tx.Delete()
			}
			if n > 0 && tx.Logging() {
				tx.Log(c.Args...)
			}
		})
	}
}

func cmdLLen(c *Ctx) {
	c.Store.View(string(c.Args[1]), func(e *store.Entry) {
		switch {
		case e == nil:
			c.W.WriteInt(0)
		case e.Kind != store.KindList:
			c.W.WriteError(errWrongType)
		default:
			c.W.WriteInt(int64(e.List.Len()))
		}
	})
}

func cmdLRange(c *Ctx) {
	start, ok1 := parseInt(c.Args[2])
	stop, ok2 := parseInt(c.Args[3])
	if !ok1 || !ok2 {
		c.W.WriteError(errNotInt)
		return
	}
	c.Store.View(string(c.Args[1]), func(e *store.Entry) {
		if e == nil {
			c.W.WriteArrayHeader(0)
			return
		}
		if e.Kind != store.KindList {
			c.W.WriteError(errWrongType)
			return
		}
		n := int64(e.List.Len())
		if start < 0 {
			start = max(start+n, 0)
		}
		if stop < 0 {
			stop += n
		}
		if start > stop || start >= n || stop < 0 {
			c.W.WriteArrayHeader(0)
			return
		}
		stop = min(stop, n-1)
		c.W.WriteArrayHeader(int(stop - start + 1))
		for i := start; i <= stop; i++ {
			c.W.WriteBulk(e.List.At(int(i)))
		}
	})
}
