package command

import (
	"bytes"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"ember/internal/resp"
	"ember/internal/store"
)

// harness runs commands against an in-memory environment with a fake clock.
type harness struct {
	t   *testing.T
	env *Env
	cl  *Client
	now *atomic.Int64
	buf bytes.Buffer
	w   *resp.Writer
	ctx *Ctx
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, now: new(atomic.Int64)}
	h.now.Store(1_000_000)
	st := store.NewWithClock(8, h.now.Load)
	h.env = NewEnv(st)
	h.cl = h.env.NewClient("test")
	h.w = resp.NewWriter(&h.buf)
	h.ctx = &Ctx{W: h.w, Store: st, Client: h.cl, Env: h.env}
	return h
}

// do runs one command and returns the raw RESP reply.
func (h *harness) do(args ...string) string {
	h.t.Helper()
	h.buf.Reset()
	h.ctx.Args = h.ctx.Args[:0]
	for _, a := range args {
		h.ctx.Args = append(h.ctx.Args, []byte(a))
	}
	h.env.Table.Execute(h.ctx)
	if err := h.w.Flush(); err != nil {
		h.t.Fatal(err)
	}
	return h.buf.String()
}

func (h *harness) advance(ms int64) { h.now.Add(ms) }

type step struct {
	cmd  string
	want string
}

func runSteps(t *testing.T, steps []step) {
	t.Helper()
	h := newHarness(t)
	for _, s := range steps {
		got := h.do(strings.Fields(s.cmd)...)
		if got != s.want {
			t.Fatalf("%s\n got: %q\nwant: %q", s.cmd, got, s.want)
		}
	}
}

const (
	ok      = "+OK\r\n"
	nilBulk = "$-1\r\n"
	wrong   = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	notInt  = "-ERR value is not an integer or out of range\r\n"
	syntax  = "-ERR syntax error\r\n"
)

func bulk(s string) string { return "$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n" }
func integer(n int) string { return ":" + strconv.Itoa(n) + "\r\n" }

func TestConnectionCommands(t *testing.T) {
	runSteps(t, []step{
		{"PING", "+PONG\r\n"},
		{"ping", "+PONG\r\n"},
		{"PING hello", bulk("hello")},
		{"PING a b", "-ERR wrong number of arguments for 'ping' command\r\n"},
		{"ECHO hi", bulk("hi")},
		{"ECHO", "-ERR wrong number of arguments for 'echo' command\r\n"},
		{"SELECT 0", ok},
		{"SELECT 1", "-ERR DB index is out of range\r\n"},
		{"SELECT x", notInt},
		{"QUIT", ok},
	})
}

func TestUnknownCommand(t *testing.T) {
	runSteps(t, []step{
		{"FOO a b", "-ERR unknown command 'FOO', with args beginning with: 'a' 'b' \r\n"},
		{"foo", "-ERR unknown command 'foo', with args beginning with: \r\n"},
	})
}

func TestArity(t *testing.T) {
	runSteps(t, []step{
		{"GET", "-ERR wrong number of arguments for 'get' command\r\n"},
		{"GET a b", "-ERR wrong number of arguments for 'get' command\r\n"},
		{"SET a", "-ERR wrong number of arguments for 'set' command\r\n"},
		{"MSET a", "-ERR wrong number of arguments for 'mset' command\r\n"},
		{"MSET a 1 b", "-ERR wrong number of arguments for 'mset' command\r\n"},
		{"DEL", "-ERR wrong number of arguments for 'del' command\r\n"},
		{"LPUSH a", "-ERR wrong number of arguments for 'lpush' command\r\n"},
		{"LRANGE a 0", "-ERR wrong number of arguments for 'lrange' command\r\n"},
		{"LPOP a 1 2", "-ERR wrong number of arguments for 'lpop' command\r\n"},
	})
}

func TestStrings(t *testing.T) {
	runSteps(t, []step{
		{"GET k", nilBulk},
		{"SET k v", ok},
		{"GET k", bulk("v")},
		{"SET k v2 NX", nilBulk},
		{"GET k", bulk("v")},
		{"SET other x XX", nilBulk},
		{"SET k v3 XX", ok},
		{"GET k", bulk("v3")},
		{"SET k v4 GET", bulk("v3")},
		{"SET fresh a GET", nilBulk},
		{"SET fresh b NX GET", bulk("a")},
		{"GET fresh", bulk("a")},
		{"SET k v NX XX", syntax},
		{"SET k v EX", syntax},
		{"SET k v EX 10 PX 10", syntax},
		{"SET k v EX abc", notInt},
		{"SET k v EX 0", "-ERR invalid expire time in 'set' command\r\n"},
		{"SET k v EX -5", "-ERR invalid expire time in 'set' command\r\n"},
		{"SET k v BOGUS", syntax},
		{"STRLEN k", integer(2)},
		{"STRLEN nope", integer(0)},
		{"APPEND k xyz", integer(5)},
		{"APPEND new abc", integer(3)},
		{"GET k", bulk("v4xyz")},
		{"MSET a 1 b 2 c 3", ok},
		{"MGET a b nope c", "*4\r\n" + bulk("1") + bulk("2") + nilBulk + bulk("3")},
		{"GETDEL a", bulk("1")},
		{"GETDEL a", nilBulk},
		{"EXISTS a b nope b", integer(2)},
	})
}

func TestIncrDecr(t *testing.T) {
	runSteps(t, []step{
		{"INCR n", integer(1)},
		{"INCR n", integer(2)},
		{"DECR n", integer(1)},
		{"INCRBY n 10", integer(11)},
		{"DECRBY n 20", ":-9\r\n"},
		{"INCRBY n x", notInt},
		{"DECRBY n x", notInt},
		{"SET s abc", ok},
		{"INCR s", notInt},
		{"SET s 007", ok},
		{"INCR s", notInt},
		{"SET big 9223372036854775807", ok},
		{"INCR big", "-ERR increment or decrement would overflow\r\n"},
		{"SET small -9223372036854775808", ok},
		{"DECR small", "-ERR increment or decrement would overflow\r\n"},
		{"DECRBY n -9223372036854775808", "-ERR decrement would overflow\r\n"},
		{"LPUSH l a", integer(1)},
		{"INCR l", wrong},
		{"APPEND l x", wrong},
		{"GET l", wrong},
		{"STRLEN l", wrong},
		{"GETDEL l", wrong},
		{"SET l x GET", wrong},
	})
}

func TestKeepsTTLOnIncr(t *testing.T) {
	h := newHarness(t)
	h.do("SET", "n", "1", "EX", "100")
	h.do("INCR", "n")
	if got := h.do("TTL", "n"); got != integer(100) {
		t.Fatalf("TTL after INCR: %q", got)
	}
	h.do("SET", "n", "5")
	if got := h.do("TTL", "n"); got != integer(-1) {
		t.Fatalf("SET should clear TTL: %q", got)
	}
	h.do("EXPIRE", "n", "50")
	h.do("SET", "n", "6", "KEEPTTL")
	if got := h.do("TTL", "n"); got != integer(50) {
		t.Fatalf("KEEPTTL: %q", got)
	}
	if got := h.do("SET", "n", "1", "KEEPTTL", "EX", "5"); got != syntax {
		t.Fatalf("KEEPTTL+EX: %q", got)
	}
}

func TestKeys(t *testing.T) {
	runSteps(t, []step{
		{"SET a 1", ok},
		{"LPUSH l x", integer(1)},
		{"TYPE a", "+string\r\n"},
		{"TYPE l", "+list\r\n"},
		{"TYPE zz", "+none\r\n"},
		{"DBSIZE", integer(2)},
		{"DEL a l nope", integer(2)},
		{"DEL a", integer(0)},
		{"DBSIZE", integer(0)},
		{"MSET user:1 a user:2 b other c", ok},
		{"KEYS nomatch*", "*0\r\n"},
		{"FLUSHALL", ok},
		{"DBSIZE", integer(0)},
		{"FLUSHDB", ok},
	})
}

func TestKeysGlob(t *testing.T) {
	h := newHarness(t)
	h.do("MSET", "user:1", "a", "user:2", "b", "other", "c")
	got := h.do("KEYS", "user:*")
	if !strings.HasPrefix(got, "*2\r\n") || !strings.Contains(got, "user:1") || !strings.Contains(got, "user:2") {
		t.Fatalf("KEYS user:*: %q", got)
	}
	if got := h.do("KEYS", "*"); !strings.HasPrefix(got, "*3\r\n") {
		t.Fatalf("KEYS *: %q", got)
	}
	if got := h.do("KEYS", "user:?"); !strings.HasPrefix(got, "*2\r\n") {
		t.Fatalf("KEYS user:?: %q", got)
	}
}

func TestExpire(t *testing.T) {
	h := newHarness(t)
	check := func(cmd string, want string) {
		t.Helper()
		if got := h.do(strings.Fields(cmd)...); got != want {
			t.Fatalf("%s: got %q want %q", cmd, got, want)
		}
	}
	check("SET k v", ok)
	check("TTL k", integer(-1))
	check("PTTL k", integer(-1))
	check("TTL nope", integer(-2))
	check("EXPIRE k 10", integer(1))
	check("TTL k", integer(10))
	check("PTTL k", integer(10000))
	check("EXPIRE nope 10", integer(0))
	check("EXPIRE k x", notInt)
	check("PEXPIRE k 1500", integer(1))
	check("PTTL k", integer(1500))
	check("TTL k", integer(2)) // rounds like Redis
	check("PERSIST k", integer(1))
	check("PERSIST k", integer(0))
	check("PERSIST nope", integer(0))
	check("TTL k", integer(-1))

	// absolute forms
	check("PEXPIREAT k 1005000", integer(1))
	check("PTTL k", integer(5000))
	check("EXPIREAT k 1010", integer(1))
	check("PTTL k", integer(10000))

	// options
	check("EXPIRE k 20 NX", integer(0))
	check("EXPIRE k 20 XX", integer(1))
	check("EXPIRE k 5 GT", integer(0))
	check("EXPIRE k 50 GT", integer(1))
	check("EXPIRE k 100 LT", integer(0))
	check("EXPIRE k 10 LT", integer(1))
	check("EXPIRE k 10 NX XX", "-ERR NX and XX, GT or LT options at the same time are not compatible\r\n")
	check("EXPIRE k 10 BOGUS", "-ERR Unsupported option BOGUS\r\n")
	check("EXPIRE k 9223372036854775807", "-ERR invalid expire time in 'expire' command\r\n")

	// lazy expiry on access
	h.advance(9_999)
	check("GET k", bulk("v"))
	h.advance(2)
	check("GET k", nilBulk)
	check("EXISTS k", integer(0))
	check("TTL k", integer(-2))

	// non-positive expiry deletes
	check("SET d v", ok)
	check("EXPIRE d 0", integer(1))
	check("EXISTS d", integer(0))
	check("SET d v", ok)
	check("PEXPIREAT d 1", integer(1))
	check("GET d", nilBulk)
}

func TestSetExpiryOptions(t *testing.T) {
	h := newHarness(t)
	h.do("SET", "a", "v", "EX", "10")
	if got := h.do("PTTL", "a"); got != integer(10000) {
		t.Fatal(got)
	}
	h.do("SET", "a", "v", "PX", "250")
	if got := h.do("PTTL", "a"); got != integer(250) {
		t.Fatal(got)
	}
	h.do("SET", "a", "v", "PXAT", "1005000")
	if got := h.do("PTTL", "a"); got != integer(5000) {
		t.Fatal(got)
	}
	h.do("SET", "a", "v", "EXAT", "1020")
	if got := h.do("PTTL", "a"); got != integer(20000) {
		t.Fatal(got)
	}
	if got := h.do("SET", "a", "v", "EX", "9223372036854775807"); !strings.Contains(got, "invalid expire time") {
		t.Fatal(got)
	}
	if got := h.do("SET", "a", "v", "PX", "9223372036854775807"); !strings.Contains(got, "invalid expire time") {
		t.Fatal(got)
	}
	h.advance(25_000)
	if got := h.do("GET", "a"); got != nilBulk {
		t.Fatal(got)
	}
}

func TestLists(t *testing.T) {
	runSteps(t, []step{
		{"LPUSH l a b c", integer(3)},
		{"RPUSH l x y", integer(5)},
		{"LRANGE l 0 -1", "*5\r\n" + bulk("c") + bulk("b") + bulk("a") + bulk("x") + bulk("y")},
		{"LRANGE l 1 2", "*2\r\n" + bulk("b") + bulk("a")},
		{"LRANGE l -2 -1", "*2\r\n" + bulk("x") + bulk("y")},
		{"LRANGE l -100 100", "*5\r\n" + bulk("c") + bulk("b") + bulk("a") + bulk("x") + bulk("y")},
		{"LRANGE l 3 1", "*0\r\n"},
		{"LRANGE l 10 20", "*0\r\n"},
		{"LRANGE l 0 -10", "*0\r\n"},
		{"LRANGE nope 0 -1", "*0\r\n"},
		{"LRANGE l a 1", notInt},
		{"LLEN l", integer(5)},
		{"LLEN nope", integer(0)},
		{"LPOP l", bulk("c")},
		{"RPOP l", bulk("y")},
		{"LPOP l 2", "*2\r\n" + bulk("b") + bulk("a")},
		{"RPOP l 10", "*1\r\n" + bulk("x")},
		{"EXISTS l", integer(0)},
		{"LPOP l", nilBulk},
		{"LPOP l 2", "*-1\r\n"},
		{"RPOP l", nilBulk},
		{"LPOP l -1", "-ERR value is out of range, must be positive\r\n"},
		{"LPOP l x", "-ERR value is out of range, must be positive\r\n"},
		{"SET s v", ok},
		{"LPUSH s a", wrong},
		{"RPUSH s a", wrong},
		{"LPOP s", wrong},
		{"LLEN s", wrong},
		{"LRANGE s 0 -1", wrong},
	})
}

func TestListDequeGrowth(t *testing.T) {
	h := newHarness(t)
	for i := 0; i < 100; i++ {
		h.do("LPUSH", "l", strconv.Itoa(i))
		h.do("RPUSH", "l", strconv.Itoa(-i))
	}
	if got := h.do("LLEN", "l"); got != integer(200) {
		t.Fatal(got)
	}
	if got := h.do("LPOP", "l"); got != bulk("99") {
		t.Fatal(got)
	}
	if got := h.do("RPOP", "l"); got != bulk("-99") {
		t.Fatal(got)
	}
}

func TestServerCommands(t *testing.T) {
	h := newHarness(t)
	h.env.Port = 6380
	h.env.MaxClients = 100
	h.do("SET", "a", "1", "EX", "100")
	h.do("GET", "a")
	h.do("GET", "missing")

	info := h.do("INFO")
	for _, want := range []string{"# Server", "# Clients", "# Stats", "# Keyspace", "cluster_enabled:0", "tcp_port:6380",
		"connected_clients:1", "maxclients:100", "keyspace_hits:1", "keyspace_misses:1",
		"total_commands_processed:", "db0:keys=1,expires=1", "aof_enabled:0"} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO missing %q", want)
		}
	}
	if got := h.do("INFO", "keyspace"); strings.Contains(got, "# Server") || !strings.Contains(got, "db0:keys=1") {
		t.Errorf("INFO keyspace: %q", got)
	}
	if got := h.do("INFO", "bogus"); got != "$0\r\n\r\n" {
		t.Errorf("INFO bogus: %q", got)
	}
	if got := h.do("INFO", "all"); !strings.Contains(got, "# Stats") {
		t.Errorf("INFO all: %q", got)
	}

	if got := h.do("COMMAND", "DOCS"); got != "*0\r\n" {
		t.Errorf("COMMAND DOCS: %q", got)
	}
	if got := h.do("COMMAND", "COUNT"); got != integer(len(h.env.Table.m)) {
		t.Errorf("COMMAND COUNT: %q", got)
	}
	if got := h.do("COMMAND"); !strings.HasPrefix(got, "*"+strconv.Itoa(len(h.env.Table.m))+"\r\n") {
		t.Errorf("COMMAND: %.40q", got)
	}
	if got := h.do("COMMAND", "INFO", "get", "nope"); !strings.Contains(got, "$3\r\nget\r\n:2\r\n") || !strings.HasSuffix(got, "*-1\r\n") {
		t.Errorf("COMMAND INFO: %q", got)
	}
	if got := h.do("COMMAND", "LIST"); !strings.Contains(got, "$3\r\nget\r\n") {
		t.Errorf("COMMAND LIST: %q", got)
	}
	if got := h.do("COMMAND", "BOGUS"); !strings.HasPrefix(got, "-ERR unknown subcommand") {
		t.Errorf("COMMAND BOGUS: %q", got)
	}

	if got := h.do("CONFIG", "GET", "save"); got != "*2\r\n"+bulk("save")+bulk("") {
		t.Errorf("CONFIG GET save: %q", got)
	}
	if got := h.do("CONFIG", "GET", "app*"); !strings.Contains(got, "appendonly") {
		t.Errorf("CONFIG GET app*: %q", got)
	}
	if got := h.do("CONFIG", "GET"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("CONFIG GET no arg: %q", got)
	}
	if got := h.do("CONFIG", "SET", "a", "b"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("CONFIG SET: %q", got)
	}

	if got := h.do("CLIENT", "ID"); got != integer(int(h.cl.ID)) {
		t.Errorf("CLIENT ID: %q", got)
	}
	if got := h.do("CLIENT", "GETNAME"); got != nilBulk {
		t.Errorf("CLIENT GETNAME: %q", got)
	}
	if got := h.do("CLIENT", "SETNAME", "me"); got != ok {
		t.Errorf("CLIENT SETNAME: %q", got)
	}
	if got := h.do("CLIENT", "GETNAME"); got != bulk("me") {
		t.Errorf("CLIENT GETNAME: %q", got)
	}
	if got := h.do("CLIENT", "SETNAME"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("CLIENT SETNAME arity: %q", got)
	}
	if got := h.do("CLIENT", "SETINFO", "LIB-NAME", "x"); got != ok {
		t.Errorf("CLIENT SETINFO: %q", got)
	}
	if got := h.do("CLIENT", "BOGUS"); !strings.HasPrefix(got, "-ERR unknown subcommand") {
		t.Errorf("CLIENT BOGUS: %q", got)
	}

	if got := h.do("HELLO"); !strings.Contains(got, "ember") {
		t.Errorf("HELLO: %q", got)
	}
	if got := h.do("HELLO", "2"); !strings.Contains(got, "proto") {
		t.Errorf("HELLO 2: %q", got)
	}
	if got := h.do("HELLO", "3"); !strings.HasPrefix(got, "-NOPROTO") {
		t.Errorf("HELLO 3: %q", got)
	}
	if got := h.do("HELLO", "x"); !strings.HasPrefix(got, "-ERR") {
		t.Errorf("HELLO x: %q", got)
	}
	if got := h.do("BGREWRITEAOF"); got != "-ERR AOF is not enabled\r\n" {
		t.Errorf("BGREWRITEAOF: %q", got)
	}
}

func TestStatsFoldOnRemove(t *testing.T) {
	h := newHarness(t)
	h.do("PING")
	h.do("PING")
	h.env.RemoveClient(h.cl)
	st := h.env.Stats()
	if st.Clients != 0 || st.Commands != 2 {
		t.Fatalf("%+v", st)
	}
}

func TestParseInt(t *testing.T) {
	good := map[string]int64{"0": 0, "1": 1, "-1": -1, "123": 123, "9223372036854775807": 1<<63 - 1, "-9223372036854775808": -1 << 63}
	for s, want := range good {
		if got, ok := parseInt([]byte(s)); !ok || got != want {
			t.Errorf("parseInt(%q) = %d, %v", s, got, ok)
		}
	}
	for _, s := range []string{"", "-", "+1", "01", "-0", "1x", " 1", "1 ", "9223372036854775808", "-9223372036854775809", "99999999999999999999", "123456789012345678901"} {
		if _, ok := parseInt([]byte(s)); ok {
			t.Errorf("parseInt(%q) should fail", s)
		}
	}
}

func TestSanitize(t *testing.T) {
	if got := sanitize([]byte("a\r\nb")); got != "a  b" {
		t.Fatal(got)
	}
	if got := sanitize(bytes.Repeat([]byte("x"), 500)); len(got) != 128 {
		t.Fatal(len(got))
	}
}

func TestUnknownCommandListsArgumentsLikeRedis(t *testing.T) {
	h := newHarness(t)
	if got := h.do("FOO", "a", "b", "c", "d", "e"); got != "-ERR unknown command 'FOO', with args beginning with: 'a' 'b' 'c' 'd' 'e' \r\n" {
		t.Fatalf("short args are all listed: %q", got)
	}
	long := strings.Repeat("x", 200)
	got := h.do("FOO", long, "second", "third")
	// The first argument is cut to 128 bytes, which already fills the budget.
	want := "-ERR unknown command 'FOO', with args beginning with: '" + strings.Repeat("x", 128) + "' \r\n"
	if got != want {
		t.Fatalf("got %q", got)
	}
}

func TestDecrByMinimumOverflow(t *testing.T) {
	h := newHarness(t)
	if got := h.do("DECRBY", "n", "-9223372036854775808"); got != "-ERR decrement would overflow\r\n" {
		t.Fatal(got)
	}
}
