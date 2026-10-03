package command

func registerConnection(t *Table) {
	t.add(Spec{Name: "ping", Arity: -1, Flags: []string{FlagFast}, Handler: cmdPing})
	t.add(Spec{Name: "echo", Arity: 2, Flags: []string{FlagFast}, Handler: cmdEcho})
	t.add(Spec{Name: "quit", Arity: -1, Flags: []string{FlagFast}, Handler: cmdQuit})
	t.add(Spec{Name: "select", Arity: 2, Flags: []string{FlagFast}, Handler: cmdSelect})
}

func cmdPing(c *Ctx) {
	if inSubMode(c.Client) {
		// RESP2 subscribed clients get [pong, payload] so the reply can be told
		// apart from pushed messages.
		c.W.WriteArrayHeader(2)
		c.W.WriteBulkString("pong")
		if len(c.Args) > 1 {
			c.W.WriteBulk(c.Args[1])
		} else {
			c.W.WriteBulkString("")
		}
		return
	}
	switch len(c.Args) {
	case 1:
		c.W.WriteSimple("PONG")
	case 2:
		c.W.WriteBulk(c.Args[1])
	default:
		c.W.WriteError("ERR wrong number of arguments for 'ping' command")
	}
}

func cmdEcho(c *Ctx) { c.W.WriteBulk(c.Args[1]) }

func cmdQuit(c *Ctx) {
	c.W.WriteSimple("OK")
	c.Client.Closing = true
}

func cmdSelect(c *Ctx) {
	n, ok := parseInt(c.Args[1])
	if !ok {
		c.W.WriteError(errNotInt)
		return
	}
	if n != 0 {
		c.W.WriteError("ERR DB index is out of range")
		return
	}
	c.W.WriteSimple("OK")
}
