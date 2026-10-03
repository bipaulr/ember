package command

import (
	"bytes"
	"math"
	"strconv"

	"ember/internal/store"
)

// maxStringLen mirrors the Redis proto-max-bulk-len default.
const maxStringLen = 512 * 1024 * 1024

func registerStrings(t *Table) {
	w, ro := []string{FlagWrite}, []string{FlagReadonly, FlagFast}
	t.add(Spec{Name: "set", Arity: -3, Flags: w, Handler: cmdSet, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "get", Arity: 2, Flags: ro, Handler: cmdGet, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "getdel", Arity: 2, Flags: w, Handler: cmdGetDel, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "mset", Arity: -3, Flags: w, Handler: cmdMSet, FirstKey: 1, LastKey: -1, Step: 2})
	t.add(Spec{Name: "mget", Arity: -2, Flags: ro, Handler: cmdMGet, FirstKey: 1, LastKey: -1, Step: 1})
	t.add(Spec{Name: "incr", Arity: 2, Flags: w, Handler: cmdIncr, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "decr", Arity: 2, Flags: w, Handler: cmdDecr, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "incrby", Arity: 3, Flags: w, Handler: cmdIncrBy, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "decrby", Arity: 3, Flags: w, Handler: cmdDecrBy, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "append", Arity: 3, Flags: w, Handler: cmdAppend, FirstKey: 1, LastKey: 1, Step: 1})
	t.add(Spec{Name: "strlen", Arity: 2, Flags: ro, Handler: cmdStrlen, FirstKey: 1, LastKey: 1, Step: 1})
}

// SET options.
type setOpts struct {
	nx, xx, get, keepTTL bool
	expireAt             int64 // absolute ms, 0 if none
}

func cmdSet(c *Ctx) {
	var o setOpts
	var haveExpire bool
	args := c.Args
	now := c.Store.Now()
	for i := 3; i < len(args); i++ {
		switch upper(args[i]) {
		case "NX":
			o.nx = true
		case "XX":
			o.xx = true
		case "GET":
			o.get = true
		case "KEEPTTL":
			o.keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			opt := upper(args[i])
			if haveExpire || i+1 >= len(args) {
				c.W.WriteError(errSyntax)
				return
			}
			i++
			v, ok := parseInt(args[i])
			if !ok {
				c.W.WriteError(errNotInt)
				return
			}
			at, ok := absoluteExpiry(opt, v, now)
			if !ok {
				c.W.WriteError("ERR invalid expire time in 'set' command")
				return
			}
			o.expireAt, haveExpire = at, true
		default:
			c.W.WriteError(errSyntax)
			return
		}
	}
	if (o.nx && o.xx) || (o.keepTTL && haveExpire) {
		c.W.WriteError(errSyntax)
		return
	}

	key := string(args[1])
	c.Store.Update(key, func(tx *store.Tx) {
		old := tx.Entry()
		if o.get && old != nil && old.Kind != store.KindString {
			c.W.WriteError(errWrongType)
			return
		}
		if (o.nx && old != nil) || (o.xx && old == nil) {
			if o.get {
				writeStrOrNull(c, old)
			} else {
				c.W.WriteNull()
			}
			return
		}
		if o.get {
			writeStrOrNull(c, old)
		}
		ent := &store.Entry{Kind: store.KindString, Str: bytes.Clone(args[2]), ExpireAt: o.expireAt}
		if o.keepTTL && old != nil {
			ent.ExpireAt = old.ExpireAt
		}
		tx.Put(ent)
		if tx.Logging() {
			logSet(tx, args[1], args[2], o)
		}
		if !o.get {
			c.W.WriteSimple("OK")
		}
	})
}

// logSet propagates SET in canonical form: relative expiry becomes absolute
// (PXAT) and the NX/XX/GET modifiers are dropped, since the decision has been
// made. Replay is therefore independent of time and of earlier state.
func logSet(tx *store.Tx, key, val []byte, o setOpts) {
	switch {
	case o.expireAt != 0:
		tx.Log([]byte("SET"), key, val, []byte("PXAT"), strconv.AppendInt(nil, o.expireAt, 10))
	case o.keepTTL:
		tx.Log([]byte("SET"), key, val, []byte("KEEPTTL"))
	default:
		tx.Log([]byte("SET"), key, val)
	}
}

// absoluteExpiry converts an EX/PX/EXAT/PXAT argument to absolute Unix ms.
func absoluteExpiry(opt string, v, now int64) (int64, bool) {
	if v <= 0 {
		return 0, false
	}
	switch opt {
	case "EX", "EXAT":
		if v > math.MaxInt64/1000 {
			return 0, false
		}
		v *= 1000
	}
	if opt == "EX" || opt == "PX" {
		if v > math.MaxInt64-now {
			return 0, false
		}
		v += now
	}
	return v, true
}

func writeStrOrNull(c *Ctx, e *store.Entry) {
	if e == nil {
		c.W.WriteNull()
	} else {
		c.W.WriteBulk(e.Str)
	}
}

func cmdGet(c *Ctx) {
	c.Store.View(string(c.Args[1]), func(e *store.Entry) {
		switch {
		case e == nil:
			c.Client.Misses.Add(1)
			c.W.WriteNull()
		case e.Kind != store.KindString:
			c.W.WriteError(errWrongType)
		default:
			c.Client.Hits.Add(1)
			c.W.WriteBulk(e.Str)
		}
	})
}

func cmdGetDel(c *Ctx) {
	c.Store.Update(string(c.Args[1]), func(tx *store.Tx) {
		e := tx.Entry()
		switch {
		case e == nil:
			c.Client.Misses.Add(1)
			c.W.WriteNull()
		case e.Kind != store.KindString:
			c.W.WriteError(errWrongType)
		default:
			c.Client.Hits.Add(1)
			c.W.WriteBulk(e.Str)
			tx.Delete()
			tx.Log([]byte("DEL"), c.Args[1])
		}
	})
}

func cmdMSet(c *Ctx) {
	if len(c.Args)%2 != 1 {
		c.W.WriteError("ERR wrong number of arguments for 'mset' command")
		return
	}
	// Each pair is applied under its own shard lock and logged as its own SET.
	// MSET is therefore not atomic across keys; see docs/DESIGN.md.
	for i := 1; i < len(c.Args); i += 2 {
		k, v := c.Args[i], c.Args[i+1]
		c.Store.Update(string(k), func(tx *store.Tx) {
			tx.Entry()
			tx.Put(&store.Entry{Kind: store.KindString, Str: bytes.Clone(v)})
			if tx.Logging() {
				tx.Log([]byte("SET"), k, v)
			}
		})
	}
	c.W.WriteSimple("OK")
}

func cmdMGet(c *Ctx) {
	c.W.WriteArrayHeader(len(c.Args) - 1)
	for _, k := range c.Args[1:] {
		c.Store.View(string(k), func(e *store.Entry) {
			if e == nil || e.Kind != store.KindString {
				c.Client.Misses.Add(1)
				c.W.WriteNull()
				return
			}
			c.Client.Hits.Add(1)
			c.W.WriteBulk(e.Str)
		})
	}
}

func cmdIncr(c *Ctx)   { incrBy(c, 1) }
func cmdDecr(c *Ctx)   { incrBy(c, -1) }
func cmdIncrBy(c *Ctx) { incrByArg(c, 1) }
func cmdDecrBy(c *Ctx) { incrByArg(c, -1) }

func incrByArg(c *Ctx, sign int64) {
	d, ok := parseInt(c.Args[2])
	if !ok {
		c.W.WriteError(errNotInt)
		return
	}
	if sign < 0 {
		if d == math.MinInt64 {
			c.W.WriteError(errOverflow)
			return
		}
		d = -d
	}
	incrBy(c, d)
}

func incrBy(c *Ctx, delta int64) {
	c.Store.Update(string(c.Args[1]), func(tx *store.Tx) {
		e := tx.Entry()
		var cur int64
		if e != nil {
			if e.Kind != store.KindString {
				c.W.WriteError(errWrongType)
				return
			}
			v, ok := parseInt(e.Str)
			if !ok {
				c.W.WriteError(errNotInt)
				return
			}
			cur = v
		}
		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			c.W.WriteError(errOverflow)
			return
		}
		cur += delta
		s := strconv.AppendInt(make([]byte, 0, 20), cur, 10)
		if e == nil {
			tx.Put(&store.Entry{Kind: store.KindString, Str: s})
		} else {
			e.Str = s
		}
		if tx.Logging() {
			tx.Log([]byte("INCRBY"), c.Args[1], strconv.AppendInt(nil, delta, 10))
		}
		c.W.WriteInt(cur)
	})
}

func cmdAppend(c *Ctx) {
	c.Store.Update(string(c.Args[1]), func(tx *store.Tx) {
		e := tx.Entry()
		if e == nil {
			tx.Put(&store.Entry{Kind: store.KindString, Str: bytes.Clone(c.Args[2])})
			c.W.WriteInt(int64(len(c.Args[2])))
		} else {
			if e.Kind != store.KindString {
				c.W.WriteError(errWrongType)
				return
			}
			if len(e.Str)+len(c.Args[2]) > maxStringLen {
				c.W.WriteError("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
				return
			}
			e.Str = append(e.Str, c.Args[2]...)
			c.W.WriteInt(int64(len(e.Str)))
		}
		if tx.Logging() {
			tx.Log([]byte("APPEND"), c.Args[1], c.Args[2])
		}
	})
}

func cmdStrlen(c *Ctx) {
	c.Store.View(string(c.Args[1]), func(e *store.Entry) {
		switch {
		case e == nil:
			c.W.WriteInt(0)
		case e.Kind != store.KindString:
			c.W.WriteError(errWrongType)
		default:
			c.W.WriteInt(int64(len(e.Str)))
		}
	})
}
