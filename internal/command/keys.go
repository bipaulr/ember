package command

import (
	"math"
	"strconv"

	"ember/internal/glob"
	"ember/internal/store"
)

func registerKeys(t *Table) {
	w, ro := []string{FlagWrite}, []string{FlagReadonly, FlagFast}
	t.add(Spec{Name: "del", Arity: -2, Flags: w, Handler: cmdDel, FirstKey: 1, LastKey: -1, Step: 1})
	t.add(Spec{Name: "exists", Arity: -2, Flags: ro, Handler: cmdExists, FirstKey: 1, LastKey: -1, Step: 1})
	t.add(Spec{Name: "expire", Arity: -3, Flags: w, Handler: expireHandler(false, false), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "pexpire", Arity: -3, Flags: w, Handler: expireHandler(true, false), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "expireat", Arity: -3, Flags: w, Handler: expireHandler(false, true), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "pexpireat", Arity: -3, Flags: w, Handler: expireHandler(true, true), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "ttl", Arity: 2, Flags: ro, Handler: ttlHandler(false), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "pttl", Arity: 2, Flags: ro, Handler: ttlHandler(true), FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "persist", Arity: 2, Flags: w, Handler: cmdPersist, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "type", Arity: 2, Flags: ro, Handler: cmdType, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "keys", Arity: 2, Flags: ro, Handler: cmdKeys})
	t.add(Spec{Name: "dbsize", Arity: 1, Flags: ro, Handler: cmdDBSize})
	t.add(Spec{Name: "flushall", Arity: -1, Flags: w, Handler: cmdFlushAll})
	t.add(Spec{Name: "flushdb", Arity: -1, Flags: w, Handler: cmdFlushAll})
}

func cmdDel(c *Ctx) {
	var n int64
	for _, k := range c.Args[1:] {
		c.Store.Update(string(k), func(tx *store.Tx) {
			if tx.Delete() {
				n++
				tx.Log([]byte("DEL"), k)
			}
		})
	}
	c.W.WriteInt(n)
}

func cmdExists(c *Ctx) {
	var n int64
	for _, k := range c.Args[1:] {
		c.Store.View(string(k), func(e *store.Entry) {
			if e != nil {
				n++
			}
		})
	}
	c.W.WriteInt(n)
}

// expireHandler builds EXPIRE, PEXPIRE, EXPIREAT or PEXPIREAT. ms selects
// milliseconds instead of seconds and abs selects an absolute timestamp.
func expireHandler(ms, abs bool) Handler {
	return func(c *Ctx) {
		name := lowerName(c.Args[0])
		v, ok := parseInt(c.Args[2])
		if !ok {
			c.W.WriteError(errNotInt)
			return
		}
		var nx, xx, gt, lt bool
		for _, o := range c.Args[3:] {
			switch upper(o) {
			case "NX":
				nx = true
			case "XX":
				xx = true
			case "GT":
				gt = true
			case "LT":
				lt = true
			default:
				c.W.WriteError("ERR Unsupported option " + sanitize(o))
				return
			}
		}
		if (nx && (xx || gt || lt)) || (gt && lt) {
			c.W.WriteError("ERR NX and XX, GT or LT options at the same time are not compatible")
			return
		}
		now := c.Store.Now()
		at := v
		if !ms {
			if v > math.MaxInt64/1000 || v < math.MinInt64/1000 {
				c.W.WriteError("ERR invalid expire time in '" + name + "' command")
				return
			}
			at = v * 1000
		}
		if !abs {
			if (at > 0 && at > math.MaxInt64-now) || (at < 0 && at < math.MinInt64+now) {
				c.W.WriteError("ERR invalid expire time in '" + name + "' command")
				return
			}
			at += now
		}
		c.Store.Update(string(c.Args[1]), func(tx *store.Tx) {
			e := tx.Entry()
			if e == nil {
				c.W.WriteInt(0)
				return
			}
			cur := e.ExpireAt
			if (nx && cur != 0) || (xx && cur == 0) ||
				(gt && (cur == 0 || at <= cur)) || (lt && cur != 0 && at >= cur) {
				c.W.WriteInt(0)
				return
			}
			if at <= now {
				tx.Delete()
				tx.Log([]byte("DEL"), c.Args[1])
			} else {
				tx.SetExpire(e, at)
				if tx.Logging() {
					tx.Log([]byte("PEXPIREAT"), c.Args[1], strconv.AppendInt(nil, at, 10))
				}
			}
			c.W.WriteInt(1)
		})
	}
}

func lowerName(b []byte) string {
	out := make([]byte, len(b))
	for i, ch := range b {
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		out[i] = ch
	}
	return string(out)
}

func ttlHandler(ms bool) Handler {
	return func(c *Ctx) {
		now := c.Store.Now()
		c.Store.View(string(c.Args[1]), func(e *store.Entry) {
			switch {
			case e == nil:
				c.W.WriteInt(-2)
			case e.ExpireAt == 0:
				c.W.WriteInt(-1)
			case ms:
				c.W.WriteInt(max(e.ExpireAt-now, 0))
			default:
				c.W.WriteInt(max((e.ExpireAt-now+500)/1000, 0))
			}
		})
	}
}

func cmdPersist(c *Ctx) {
	c.Store.Update(string(c.Args[1]), func(tx *store.Tx) {
		e := tx.Entry()
		if e == nil || e.ExpireAt == 0 {
			c.W.WriteInt(0)
			return
		}
		tx.SetExpire(e, 0)
		tx.Log([]byte("PERSIST"), c.Args[1])
		c.W.WriteInt(1)
	})
}

func cmdType(c *Ctx) {
	c.Store.View(string(c.Args[1]), func(e *store.Entry) {
		if e == nil {
			c.W.WriteSimple("none")
			return
		}
		c.W.WriteSimple(e.Kind.String())
	})
}

func cmdKeys(c *Ctx) {
	pat := string(c.Args[1])
	var keys []string
	if pat == "*" {
		keys = c.Store.Keys(func(string) bool { return true })
	} else {
		keys = c.Store.Keys(func(k string) bool { return glob.Match(pat, k) })
	}
	c.W.WriteArrayHeader(len(keys))
	for _, k := range keys {
		c.W.WriteBulkString(k)
	}
}

func cmdDBSize(c *Ctx) { c.W.WriteInt(int64(c.Store.Len())) }

func cmdFlushAll(c *Ctx) {
	c.Store.FlushAll()
	c.W.WriteSimple("OK")
}
