// Package command holds the command table and the handler for every command
// Ember supports.
package command

import (
	"strings"
	"sync/atomic"

	"ember/internal/pubsub"
	"ember/internal/resp"
	"ember/internal/store"
)

// Flags describe a command for COMMAND output.
const (
	FlagWrite    = "write"
	FlagReadonly = "readonly"
	FlagFast     = "fast"
	FlagAdmin    = "admin"
)

// Client is the per-connection state visible to handlers.
type Client struct {
	ID   int64
	Addr string
	Name string

	// Closing asks the connection loop to close after flushing replies.
	Closing bool

	// Dirty is set after a write command runs. The connection then commits the
	// AOF before it sends the next bytes to the client.
	Dirty bool

	// Sub is this client's pub/sub inbox, created on its first subscription.
	Sub *pubsub.Subscriber
	// NewSub is supplied by the server. It creates the inbox and starts the
	// goroutine that pushes queued messages to the socket.
	NewSub func() *pubsub.Subscriber

	// Per-client counters, summed by INFO. Keeping them per client avoids a
	// contended atomic on every command.
	Commands atomic.Int64
	Hits     atomic.Int64
	Misses   atomic.Int64
}

// Ctx is passed to every handler. The server reuses one Ctx per connection.
type Ctx struct {
	Args   [][]byte // Args[0] is the command name as the client sent it
	W      *resp.Writer
	Store  *store.Store
	Client *Client
	Env    *Env
}

// Handler executes a command and writes the reply to c.W.
type Handler func(c *Ctx)

// Spec describes one command.
type Spec struct {
	Name string // lower case
	// Arity is the exact argument count including the command name. A negative
	// value means "at least -Arity".
	Arity   int
	Flags   []string
	Handler Handler
	// FirstKey, LastKey, Step follow the COMMAND reply convention.
	FirstKey, LastKey, Step int

	write bool
}

// Table maps upper-case names to specs.
type Table struct {
	m map[string]*Spec
}

// NewTable builds the table with every supported command.
func NewTable() *Table {
	t := &Table{m: make(map[string]*Spec)}
	registerConnection(t)
	registerStrings(t)
	registerKeys(t)
	registerLists(t)
	registerServer(t)
	registerPersist(t)
	registerPubSub(t)
	return t
}

func (t *Table) add(s Spec) {
	for _, f := range s.Flags {
		if f == FlagWrite {
			s.write = true
		}
	}
	t.m[strings.ToUpper(s.Name)] = &s
}

// Lookup finds a spec by case-insensitive name without allocating.
func (t *Table) Lookup(name []byte) *Spec {
	var buf [32]byte
	if len(name) > len(buf) {
		return nil
	}
	for i, ch := range name {
		if ch >= 'a' && ch <= 'z' {
			ch -= 'a' - 'A'
		}
		buf[i] = ch
	}
	return t.m[string(buf[:len(name)])]
}

// Names returns all command specs (unordered).
func (t *Table) Specs() []*Spec {
	out := make([]*Spec, 0, len(t.m))
	for _, s := range t.m {
		out = append(out, s)
	}
	return out
}

// Execute runs the command in c.Args and writes its reply.
func (t *Table) Execute(c *Ctx) {
	c.Client.Commands.Add(1)
	spec := t.Lookup(c.Args[0])
	if spec == nil {
		c.W.WriteError(unknownCommandMsg(c.Args))
		return
	}
	n := len(c.Args)
	if (spec.Arity > 0 && n != spec.Arity) || (spec.Arity < 0 && n < -spec.Arity) {
		c.W.WriteError("ERR wrong number of arguments for '" + spec.Name + "' command")
		return
	}
	if inSubMode(c.Client) && !subAllowed(spec.Name) {
		c.W.WriteError(subErr(spec.Name))
		return
	}
	spec.Handler(c)
	if spec.write {
		c.Client.Dirty = true
	}
}

func unknownCommandMsg(args [][]byte) string {
	var sb strings.Builder
	sb.WriteString("ERR unknown command '")
	sb.WriteString(sanitize(args[0]))
	sb.WriteString("', with args beginning with: ")
	for i := 1; i < len(args) && i <= 3; i++ {
		sb.WriteByte('\'')
		sb.WriteString(sanitize(args[i]))
		sb.WriteString("' ")
	}
	return sb.String()
}

// sanitize keeps error replies on one line, as a RESP error must be.
func sanitize(b []byte) string {
	if len(b) > 128 {
		b = b[:128]
	}
	out := make([]byte, len(b))
	for i, ch := range b {
		if ch == '\r' || ch == '\n' {
			ch = ' '
		}
		out[i] = ch
	}
	return string(out)
}

// Shared error texts, matching Redis.
const (
	errWrongType  = "WRONGTYPE Operation against a key holding the wrong kind of value"
	errNotInt     = "ERR value is not an integer or out of range"
	errSyntax     = "ERR syntax error"
	errOverflow   = "ERR increment or decrement would overflow"
	errNoSuchKey  = "ERR no such key"
	errOutOfRange = "ERR value is out of range, must be positive"
)

func upper(b []byte) string { return strings.ToUpper(string(b)) }
