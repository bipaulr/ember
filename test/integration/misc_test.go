package integration

import (
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRemainingCommandsViaGoRedis(t *testing.T) {
	c, _ := inProc(t)

	c.Set(bg, "k", "v", 0)
	at := time.Now().Add(time.Hour)
	if ok, err := c.ExpireAt(bg, "k", at).Result(); err != nil || !ok {
		t.Fatalf("EXPIREAT: %v %v", ok, err)
	}
	if d, _ := c.TTL(bg, "k").Result(); d < 59*time.Minute || d > time.Hour {
		t.Fatalf("TTL after EXPIREAT: %v", d)
	}
	if ok, _ := c.PExpireAt(bg, "k", time.Now().Add(2*time.Hour)).Result(); !ok {
		t.Fatal("PEXPIREAT")
	}
	if d, _ := c.TTL(bg, "k").Result(); d < 119*time.Minute {
		t.Fatalf("TTL after PEXPIREAT: %v", d)
	}
	if ok, _ := c.ExpireNX(bg, "k", time.Minute).Result(); ok {
		t.Fatal("EXPIRE NX should fail on a key that has a TTL")
	}
	if ok, _ := c.ExpireGT(bg, "k", 3*time.Hour).Result(); !ok {
		t.Fatal("EXPIRE GT")
	}
	if err := c.Do(bg, "FLUSHDB").Err(); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.DBSize(bg).Result(); n != 0 {
		t.Fatal(n)
	}

	// CONFIG GET, as redis-benchmark and tools use it
	cfg, err := c.ConfigGet(bg, "save").Result()
	if err != nil || cfg["save"] != "" {
		t.Fatalf("CONFIG GET save: %v %v", cfg, err)
	}
	if err := c.ConfigSet(bg, "maxmemory", "1").Err(); err == nil {
		t.Fatal("CONFIG SET is not supported and must say so")
	}

	// CLIENT and HELLO
	if id, err := c.ClientID(bg).Result(); err != nil || id <= 0 {
		t.Fatalf("CLIENT ID: %v %v", id, err)
	}
	if err := c.Do(bg, "CLIENT", "SETNAME", "tester").Err(); err != nil {
		t.Fatal(err)
	}
	if name, _ := c.Do(bg, "CLIENT", "GETNAME").Text(); name != "tester" {
		t.Fatalf("CLIENT GETNAME %q", name)
	}
	err = c.Do(bg, "HELLO", "3").Err()
	if err == nil || !strings.HasPrefix(err.Error(), "NOPROTO") {
		t.Fatalf("HELLO 3 should be refused, got %v", err)
	}
	if err := c.Do(bg, "HELLO", "2").Err(); err != nil {
		t.Fatalf("HELLO 2: %v", err)
	}
	if _, err := c.Do(bg, "COMMAND", "COUNT").Int(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Do(bg, "COMMAND", "INFO", "get").Result(); err != nil {
		t.Fatal(err)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	c, _ := inProc(t)
	conn, err := net.Dial("tcp", c.Options().Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(conn, "QUIT\r\n"); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 16)
	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		t.Fatalf("got %q", buf[:n])
	}
	if _, err := conn.Read(buf); !errors.Is(err, io.EOF) {
		t.Fatalf("connection should be closed after QUIT, got %v", err)
	}
}

func TestInlineCommandsLikeTelnet(t *testing.T) {
	c, _ := inProc(t)
	conn, err := net.Dial("tcp", c.Options().Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(conn, "SET greeting \"hello world\"\r\nGET greeting\r\n"); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n$11\r\nhello world\r\n"
	buf := make([]byte, len(want))
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != want {
		t.Fatalf("got %q, %v", buf, err)
	}
}

func TestProtocolErrorOverTheWire(t *testing.T) {
	c, _ := inProc(t)
	// A bad connection must not affect a good one.
	conn, err := net.Dial("tcp", c.Options().Addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	_, _ = io.WriteString(conn, "*1\r\n$abc\r\n")
	buf := make([]byte, 64)
	n, _ := conn.Read(buf)
	if !strings.HasPrefix(string(buf[:n]), "-ERR Protocol error") {
		t.Fatalf("got %q", buf[:n])
	}
	if err := c.Ping(bg).Err(); err != nil {
		t.Fatalf("other clients must be unaffected: %v", err)
	}
	if _, err := c.Get(bg, "nope").Result(); !errors.Is(err, redis.Nil) {
		t.Fatal(err)
	}
}
