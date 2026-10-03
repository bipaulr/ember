package command

import "ember/internal/pubsub"

func registerPubSub(t *Table) {
	f := []string{"pubsub", FlagFast}
	t.add(Spec{Name: "subscribe", Arity: -2, Flags: f, Handler: cmdSubscribe})
	t.add(Spec{Name: "unsubscribe", Arity: -1, Flags: f, Handler: cmdUnsubscribe})
	t.add(Spec{Name: "psubscribe", Arity: -2, Flags: f, Handler: cmdPSubscribe})
	t.add(Spec{Name: "punsubscribe", Arity: -1, Flags: f, Handler: cmdPUnsubscribe})
	t.add(Spec{Name: "publish", Arity: 3, Flags: f, Handler: cmdPublish})
	t.add(Spec{Name: "pubsub", Arity: -2, Flags: []string{"pubsub"}, Handler: cmdPubSub})
}

// subAllowed lists the commands a subscribed client may run.
func subAllowed(name string) bool {
	switch name {
	case "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ping", "quit":
		return true
	}
	return false
}

// inSubMode reports whether the client has any active subscription.
func inSubMode(c *Client) bool { return c.Sub != nil && c.Sub.Count() > 0 }

func subErr(name string) string {
	return "ERR Can't execute '" + name + "': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
}

func (c *Ctx) sub() *pubsub.Subscriber {
	if c.Client.Sub == nil {
		c.Client.Sub = c.Client.NewSub()
	}
	return c.Client.Sub
}

func writeSubReply(c *Ctx, kind string, name []byte, count int) {
	c.W.WriteArrayHeader(3)
	c.W.WriteBulkString(kind)
	if name == nil {
		c.W.WriteNull()
	} else {
		c.W.WriteBulk(name)
	}
	c.W.WriteInt(int64(count))
}

func cmdSubscribe(c *Ctx) {
	s := c.sub()
	for _, ch := range c.Args[1:] {
		n := c.Env.Hub.Subscribe(s, string(ch))
		writeSubReply(c, "subscribe", ch, n)
	}
}

func cmdPSubscribe(c *Ctx) {
	s := c.sub()
	for _, p := range c.Args[1:] {
		n := c.Env.Hub.PSubscribe(s, string(p))
		writeSubReply(c, "psubscribe", p, n)
	}
}

func cmdUnsubscribe(c *Ctx) {
	s := c.Client.Sub
	var names []string
	if len(c.Args) > 1 {
		for _, a := range c.Args[1:] {
			names = append(names, string(a))
		}
	} else if s != nil {
		names = s.Channels()
	}
	if len(names) == 0 {
		writeSubReply(c, "unsubscribe", nil, countOf(s))
		return
	}
	for _, ch := range names {
		n := countOf(s)
		if s != nil {
			n = c.Env.Hub.Unsubscribe(s, ch)
		}
		writeSubReply(c, "unsubscribe", []byte(ch), n)
	}
}

func cmdPUnsubscribe(c *Ctx) {
	s := c.Client.Sub
	var names []string
	if len(c.Args) > 1 {
		for _, a := range c.Args[1:] {
			names = append(names, string(a))
		}
	} else if s != nil {
		names = s.Patterns()
	}
	if len(names) == 0 {
		writeSubReply(c, "punsubscribe", nil, countOf(s))
		return
	}
	for _, p := range names {
		n := countOf(s)
		if s != nil {
			n = c.Env.Hub.PUnsubscribe(s, p)
		}
		writeSubReply(c, "punsubscribe", []byte(p), n)
	}
}

func countOf(s *pubsub.Subscriber) int {
	if s == nil {
		return 0
	}
	return s.Count()
}

func cmdPublish(c *Ctx) {
	c.W.WriteInt(int64(c.Env.Hub.Publish(string(c.Args[1]), c.Args[2])))
}

func cmdPubSub(c *Ctx) {
	switch upper(c.Args[1]) {
	case "CHANNELS":
		pat := ""
		if len(c.Args) > 2 {
			pat = string(c.Args[2])
		}
		chs := c.Env.Hub.Channels(pat)
		c.W.WriteArrayHeader(len(chs))
		for _, ch := range chs {
			c.W.WriteBulkString(ch)
		}
	case "NUMSUB":
		c.W.WriteArrayHeader((len(c.Args) - 2) * 2)
		for _, ch := range c.Args[2:] {
			c.W.WriteBulk(ch)
			c.W.WriteInt(int64(c.Env.Hub.NumSub(string(ch))))
		}
	case "NUMPAT":
		c.W.WriteInt(int64(c.Env.Hub.NumPat()))
	default:
		c.W.WriteError("ERR unknown subcommand '" + sanitize(c.Args[1]) + "'")
	}
}
