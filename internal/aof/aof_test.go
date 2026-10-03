package aof

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"ember/internal/command"
	"ember/internal/store"
)

// rig is a store, command environment and AOF wired together the way main does.
type rig struct {
	t    *testing.T
	st   *store.Store
	env  *command.Env
	a    *AOF
	run  func(args [][]byte) error
	path string
}

func newRig(t *testing.T, path string, policy Policy, minRewrite int64) *rig {
	t.Helper()
	st := store.New(8)
	env := command.NewEnv(st)
	st.SetLoading(true)
	res, err := Replay(path, env.Replayer())
	st.SetLoading(false)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	_ = res
	a, err := Open(Options{Path: path, Policy: policy, Shards: st.NumShards(), RewriteMinSize: minRewrite})
	if err != nil {
		t.Fatal(err)
	}
	a.SetSource(st)
	st.SetPropagator(a.Append)
	env.AOF = a
	return &rig{t: t, st: st, env: env, a: a, run: env.Replayer(), path: path}
}

// exec runs one command through run like a client would, then commits it.
func (r *rig) exec(run func([][]byte) error, args []string) error {
	b := make([][]byte, len(args))
	for i, a := range args {
		b[i] = []byte(a)
	}
	if err := run(b); err != nil {
		return fmt.Errorf("%v: %w", args, err)
	}
	return r.a.Commit()
}

// do executes a command on the rig's own (single goroutine) executor.
func (r *rig) do(args ...string) {
	r.t.Helper()
	if err := r.exec(r.run, args); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) close() {
	r.t.Helper()
	if err := r.a.Close(); err != nil {
		r.t.Fatal(err)
	}
}

// dump returns a canonical, sorted description of a store's contents.
func dump(st *store.Store) string {
	var lines []string
	for i := 0; i < st.NumShards(); i++ {
		st.SnapshotShard(i, nil, func(args [][]byte) {
			parts := make([]string, len(args))
			for j, a := range args {
				parts[j] = string(a)
			}
			lines = append(lines, strings.Join(parts, " "))
		})
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// loadFresh replays path into a brand new store.
func loadFresh(t *testing.T, path string) (*store.Store, ReplayResult) {
	t.Helper()
	st := store.New(8)
	env := command.NewEnv(st)
	st.SetLoading(true)
	res, err := Replay(path, env.Replayer())
	st.SetLoading(false)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	return st, res
}

func tmpAOF(t *testing.T) string { return filepath.Join(t.TempDir(), "test.aof") }

func TestParsePolicy(t *testing.T) {
	for in, want := range map[string]Policy{"always": Always, "everysec": EverySec, "": EverySec, "no": No} {
		got, err := ParsePolicy(in)
		if err != nil || got != want {
			t.Errorf("ParsePolicy(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParsePolicy("sometimes"); err == nil {
		t.Error("expected error")
	}
	if Always.String() != "always" || EverySec.String() != "everysec" || No.String() != "no" {
		t.Error("String")
	}
}

func TestRoundTrip(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	r.do("SET", "s", "hello")
	r.do("SET", "ttl", "v", "EX", "1000")
	r.do("INCR", "n")
	r.do("INCRBY", "n", "41")
	r.do("APPEND", "s", " world")
	r.do("MSET", "a", "1", "b", "2")
	r.do("DEL", "a")
	r.do("LPUSH", "l", "x", "y")
	r.do("RPUSH", "l", "z")
	r.do("LPOP", "l")
	r.do("SET", "gone", "1")
	r.do("GETDEL", "gone")
	r.do("SET", "p", "1", "EX", "500")
	r.do("PERSIST", "p")
	r.do("EXPIRE", "s", "2000")
	want := dump(r.st)
	r.close()

	st, res := loadFresh(t, path)
	if got := dump(st); got != want {
		t.Fatalf("state differs after replay\n got:\n%s\nwant:\n%s", got, want)
	}
	if res.Truncated || res.Commands == 0 {
		t.Fatalf("%+v", res)
	}
}

// TestRelativeExpiryIsLoggedAbsolute checks the on-disk form directly.
func TestRelativeExpiryIsLoggedAbsolute(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	r.do("SET", "k", "v")
	r.do("EXPIRE", "k", "100")
	r.do("SET", "j", "v", "EX", "100")
	r.do("PEXPIRE", "k", "5000")
	r.close()
	data, _ := os.ReadFile(path)
	s := string(data)
	for _, bad := range []string{"\r\nEXPIRE\r\n", "\r\nPEXPIRE\r\n", "\r\nEX\r\n", "\r\nPX\r\n"} {
		if strings.Contains(strings.ToUpper(s), bad) {
			t.Fatalf("relative form %q found in log:\n%q", bad, s)
		}
	}
	if !strings.Contains(s, "PEXPIREAT") || !strings.Contains(s, "PXAT") {
		t.Fatalf("absolute forms missing:\n%q", s)
	}
}

// TestExpiryAcrossDowntime: a key whose deadline passes while the server is
// down must be gone after restart, and one that has not must keep its deadline.
func TestExpiryAcrossDowntime(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	r.do("SET", "short", "v", "PX", "150")
	r.do("SET", "long", "v", "EX", "1000")
	r.do("SET", "counter", "1", "PX", "150")
	r.do("INCR", "counter") // non-idempotent command on a key that expires during downtime
	r.close()
	time.Sleep(300 * time.Millisecond)

	r2 := newRig(t, path, EverySec, -1)
	defer r2.close()
	var h1, h2, h3 bool
	r2.st.View("short", func(e *store.Entry) { h1 = e != nil })
	r2.st.View("long", func(e *store.Entry) { h2 = e != nil && e.ExpireAt > r2.st.Now() })
	r2.st.View("counter", func(e *store.Entry) { h3 = e != nil })
	if h1 || !h2 || h3 {
		t.Fatalf("short alive=%v long ok=%v counter alive=%v", h1, h2, h3)
	}
}

func TestReplayMissingFile(t *testing.T) {
	res, err := Replay(filepath.Join(t.TempDir(), "nope.aof"), func([][]byte) error { return nil })
	if err != nil || res.Commands != 0 {
		t.Fatalf("%+v %v", res, err)
	}
}

// TestTruncatedTail cuts the file at every possible byte inside the last
// record and expects replay to recover everything before it.
func TestTruncatedTail(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	r.do("SET", "a", "1")
	r.do("SET", "b", "2")
	r.close()
	good, _ := os.ReadFile(path)
	r = newRig(t, path, EverySec, -1)
	r.do("SET", "last", "value-that-gets-cut")
	r.close()
	full, _ := os.ReadFile(path)

	for cut := len(good) + 1; cut < len(full); cut++ {
		p := filepath.Join(t.TempDir(), "cut.aof")
		if err := os.WriteFile(p, full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		st, res := loadFresh(t, p)
		if !res.Truncated || res.DiscardedBytes != int64(cut-len(good)) {
			t.Fatalf("cut=%d: %+v", cut, res)
		}
		if st.Len() != 2 {
			t.Fatalf("cut=%d: len=%d", cut, st.Len())
		}
		after, _ := os.ReadFile(p)
		if !bytes.Equal(after, good) {
			t.Fatalf("cut=%d: file not repaired to the valid prefix", cut)
		}
	}
	// Uncut file loads fully and is untouched.
	st, res := loadFresh(t, path)
	if res.Truncated || st.Len() != 3 {
		t.Fatalf("%+v len=%d", res, st.Len())
	}
}

func TestTruncatedTailThenAppendsContinue(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	r.do("SET", "a", "1")
	r.close()
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.WriteString("*3\r\n$3\r\nSET\r\n$1\r\nb") // torn write
	_ = f.Close()

	r = newRig(t, path, EverySec, -1) // repairs on open
	r.do("SET", "c", "3")
	r.close()
	st, res := loadFresh(t, path)
	if res.Truncated || st.Len() != 2 {
		t.Fatalf("%+v len=%d", res, st.Len())
	}
}

func TestCorruptionFailsFast(t *testing.T) {
	cases := map[string]string{
		"garbage mid file":    "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*1\r\n:5\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n",
		"bad bulk terminator": "*2\r\n$3\r\nGET\r\n$1\r\nkXX*1\r\n$4\r\nPING\r\n",
		"unknown command":     "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n*1\r\n$7\r\nNOTACMD\r\n",
		"command that errs":   "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$3\r\nabc\r\n*2\r\n$4\r\nINCR\r\n$1\r\na\r\n",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			p := tmpAOF(t)
			if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
			st := store.New(4)
			env := command.NewEnv(st)
			_, err := Replay(p, env.Replayer())
			var ce *CorruptError
			if !errors.As(err, &ce) {
				t.Fatalf("expected CorruptError, got %v", err)
			}
			if !strings.Contains(ce.Error(), "corrupt") || ce.Unwrap() == nil {
				t.Fatal("poor error message")
			}
			after, _ := os.ReadFile(p)
			if string(after) != content {
				t.Fatal("corrupt file must be left untouched")
			}
		})
	}
}

func TestRewriteCompacts(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	for i := 0; i < 2000; i++ {
		r.do("INCR", "counter")
	}
	for i := 0; i < 500; i++ {
		r.do("SET", "overwritten", fmt.Sprint(i))
	}
	r.do("SET", "temp", "x")
	r.do("DEL", "temp")
	r.do("SET", "ttl", "v", "EX", "1000")
	for i := 0; i < 300; i++ {
		r.do("RPUSH", "list", fmt.Sprint(i))
	}
	for i := 0; i < 100; i++ {
		r.do("LPOP", "list")
	}
	before := r.a.Info().Size
	if err := r.a.Rewrite(); err != nil {
		t.Fatal(err)
	}
	info := r.a.Info()
	if info.Size >= before/10 || info.Rewrites != 1 || info.BaseSize != info.Size {
		t.Fatalf("not compacted: before=%d after=%+v", before, info)
	}
	fi, _ := os.Stat(path)
	if fi.Size() != info.Size {
		t.Fatalf("file size %d != tracked %d", fi.Size(), info.Size)
	}
	// Writes after the rewrite go to the new file.
	r.do("INCR", "counter")
	want := dump(r.st)
	r.close()
	st, _ := loadFresh(t, path)
	if got := dump(st); got != want {
		t.Fatalf("state differs after rewrite+replay\n got:\n%s\nwant:\n%s", got, want)
	}
	if _, err := os.Stat(tempPath(path)); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}

// TestRewriteUnderConcurrentWrites is the consistency proof for the rewrite:
// writers use non-idempotent commands (INCR, RPUSH, APPEND) while rewrites run
// back to back. If a single write were lost or applied twice, the replayed
// state would differ from the live one.
func TestRewriteUnderConcurrentWrites(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	const writers, iters = 8, 1500
	var wg sync.WaitGroup
	stop := make(chan struct{})
	var rewrites int
	rwDone := make(chan struct{})
	go func() {
		defer close(rwDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := r.a.Rewrite(); err != nil {
				t.Errorf("rewrite: %v", err)
				return
			}
			rewrites++
		}
	}()
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			run := r.env.Replayer()
			do := func(args ...string) {
				if err := r.exec(run, args); err != nil {
					t.Error(err)
				}
			}
			for i := 0; i < iters; i++ {
				do("INCR", fmt.Sprintf("counter:%d", g%3)) // shared keys
				do("RPUSH", fmt.Sprintf("list:%d", g%3), fmt.Sprint(g, ":", i))
				do("APPEND", fmt.Sprintf("app:%d", g%2), "x")
				do("SET", fmt.Sprintf("k:%d", i%40), fmt.Sprint(g))
				if i%10 == 0 {
					do("SET", fmt.Sprintf("own:%d:%d", g, i), "v", "EX", "1000")
				}
				if i%25 == 0 {
					do("DEL", fmt.Sprintf("own:%d:%d", g, i-25))
				}
				if i%50 == 0 {
					do("LPOP", fmt.Sprintf("list:%d", g%3))
				}
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	<-rwDone
	if rewrites == 0 {
		t.Fatal("no rewrite ran during the test")
	}
	want := dump(r.st)
	r.close()
	st, _ := loadFresh(t, path)
	if got := dump(st); got != want {
		t.Fatalf("live and replayed state differ after %d concurrent rewrites", rewrites)
	}
	// Exact totals prove nothing was lost or doubled.
	var total int
	for k := 0; k < 3; k++ {
		st.View(fmt.Sprintf("counter:%d", k), func(e *store.Entry) {
			var n int
			_, _ = fmt.Sscan(string(e.Str), &n)
			total += n
		})
	}
	if total != writers*iters {
		t.Fatalf("INCR total %d, want %d", total, writers*iters)
	}
	t.Logf("%d rewrites ran while %d writes were applied", rewrites, writers*iters*4)
}

func TestFlushAllAbortsRewrite(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	for i := 0; i < 20000; i++ {
		r.do("SET", fmt.Sprint("key", i), "value")
	}
	if err := r.a.StartRewrite(); err != nil {
		t.Fatal(err)
	}
	r.do("FLUSHALL")
	r.do("SET", "after", "1")
	for r.a.Info().RewriteInProgress {
		time.Sleep(time.Millisecond)
	}
	// Whether or not the rewrite was aborted, the final file must replay to the live state.
	want := dump(r.st)
	r.close()
	st, _ := loadFresh(t, path)
	if got := dump(st); got != want || st.Len() != 1 {
		t.Fatalf("state wrong after FLUSHALL during rewrite: len=%d", st.Len())
	}
}

func TestRewriteInProgressAndErrors(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	for i := 0; i < 50000; i++ {
		r.do("SET", fmt.Sprint("key", i), "value")
	}
	if err := r.a.StartRewrite(); err != nil {
		t.Fatal(err)
	}
	if err := r.a.StartRewrite(); !errors.Is(err, command.ErrRewriteInProgress) {
		// The first rewrite may already be done on a fast machine.
		if err != nil {
			t.Fatalf("unexpected: %v", err)
		}
	}
	for r.a.Info().RewriteInProgress {
		time.Sleep(time.Millisecond)
	}
	r.close()
	if err := r.a.StartRewrite(); err == nil {
		t.Fatal("rewrite after close should fail")
	}
	a2, _ := Open(Options{Path: path, RewriteMinSize: -1})
	defer func() { _ = a2.Close() }()
	if err := a2.StartRewrite(); err == nil {
		t.Fatal("rewrite without source should fail")
	}
}

func TestBGRewriteCommand(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	defer r.close()
	r.do("SET", "a", "1")
	r.do("BGREWRITEAOF")
	for r.a.Info().RewriteInProgress {
		time.Sleep(time.Millisecond)
	}
	if r.a.Info().Rewrites != 1 {
		t.Fatal("BGREWRITEAOF did not rewrite")
	}
}

func TestAutoRewrite(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, 2000) // tiny threshold
	for i := 0; i < 400; i++ {
		r.do("INCR", "c")
	}
	deadline := time.Now().Add(5 * time.Second)
	for r.a.Info().Rewrites == 0 {
		if time.Now().After(deadline) {
			t.Fatal("automatic rewrite never triggered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	want := dump(r.st)
	r.close()
	st, _ := loadFresh(t, path)
	if dump(st) != want {
		t.Fatal("state differs after auto rewrite")
	}
}

func TestAlwaysPolicyGroupCommit(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, Always, -1)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			run := r.env.Replayer()
			for i := 0; i < 100; i++ {
				// After Commit returns the record must be in the file already.
				if err := r.exec(run, []string{"SET", fmt.Sprintf("g%d:%d", g, i), "v"}); err != nil {
					t.Error(err)
				}
			}
		}(g)
	}
	wg.Wait()
	// Read the file without closing the AOF: everything must be there.
	st, res := loadFresh(t, copyOf(t, path))
	if st.Len() != 1600 || res.Truncated {
		t.Fatalf("len=%d %+v", st.Len(), res)
	}
	r.close()
}

func copyOf(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(t.TempDir(), "copy.aof")
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPoliciesWriteThroughOnCommit(t *testing.T) {
	for _, pol := range []Policy{EverySec, No} {
		path := tmpAOF(t)
		r := newRig(t, path, pol, -1)
		r.do("SET", "a", "1")
		// Without closing: committed data is already visible to other readers.
		st, _ := loadFresh(t, copyOf(t, path))
		if st.Len() != 1 {
			t.Fatalf("%v: committed write not in file", pol)
		}
		r.close()
	}
}

func TestBackgroundFlushOfUnattachedRecords(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	defer r.close()
	// Append directly, as active expiry does, with nobody calling Commit.
	r.a.Append(0, [][]byte{[]byte("SET"), []byte("bg"), []byte("1")})
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, _ := loadFresh(t, copyOf(t, path))
		if st.Len() == 1 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("housekeeper never wrote the pending record")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestCloseIsIdempotentAndIgnoresLateAppends(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, EverySec, -1)
	r.do("SET", "a", "1")
	r.close()
	if err := r.a.Close(); err != nil {
		t.Fatal(err)
	}
	r.a.Append(0, [][]byte{[]byte("SET"), []byte("late"), []byte("1")})
	st, _ := loadFresh(t, path)
	if st.Len() != 1 {
		t.Fatal("append after close reached the file")
	}
}

func TestStaleTempFileRemoved(t *testing.T) {
	path := tmpAOF(t)
	_ = os.WriteFile(tempPath(path), []byte("junk"), 0o644)
	r := newRig(t, path, EverySec, -1)
	defer r.close()
	if _, err := os.Stat(tempPath(path)); !os.IsNotExist(err) {
		t.Fatal("stale temp file not removed")
	}
}

func TestInfoReflectsState(t *testing.T) {
	path := tmpAOF(t)
	r := newRig(t, path, Always, -1)
	defer r.close()
	r.do("SET", "a", "1")
	info := r.a.Info()
	if info.Path != path || info.Policy != "always" || info.Size == 0 || r.a.Seq() == 0 {
		t.Fatalf("%+v", info)
	}
}
