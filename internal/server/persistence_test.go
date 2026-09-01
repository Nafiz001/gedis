package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Persistence is only meaningful across a restart, so these tests start a
// server, write to it, stop it, and start a new one over the same directory --
// which is the scenario the feature exists for and the only one that proves it.

// restartHarness runs successive servers over a shared directory.
type restartHarness struct {
	t      *testing.T
	dir    string
	cfg    func(*Config)
	srv    *Server
	h      *harness
	stopFn func()
}

func newRestartHarness(t *testing.T, tweak func(*Config)) *restartHarness {
	t.Helper()
	return &restartHarness{t: t, dir: t.TempDir(), cfg: tweak}
}

// start brings a server up over the shared directory.
func (rh *restartHarness) start() *conn {
	rh.t.Helper()

	clk := &testClock{}
	clk.ms.Store(1_700_000_000_000)

	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	cfg.Databases = 4
	cfg.Shards = 8
	cfg.Dir = rh.dir
	cfg.Clock = clk.now
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	// Automatic snapshots are disabled so each test controls exactly when a
	// save happens; a timer firing mid-test would make them non-deterministic.
	cfg.SavePoints = nil
	if rh.cfg != nil {
		rh.cfg(&cfg)
	}

	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		rh.t.Fatalf("listen: %v", err)
	}
	addr := srv.Addr().String()

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer close(done)
		_ = srv.Serve(ctx)
	}()

	rh.srv = srv
	rh.h = &harness{srv: srv, addr: addr, clk: clk}
	rh.stopFn = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			rh.t.Error("server did not shut down")
		}
	}
	rh.t.Cleanup(func() {
		if rh.stopFn != nil {
			rh.stopFn()
			rh.stopFn = nil
		}
	})
	return rh.h.dial(rh.t)
}

// stop shuts the current server down, flushing whatever it owes to disk.
func (rh *restartHarness) stop() {
	rh.t.Helper()
	if rh.stopFn != nil {
		rh.stopFn()
		rh.stopFn = nil
	}
}

// restart is a stop followed by a start over the same directory.
func (rh *restartHarness) restart() *conn {
	rh.t.Helper()
	rh.stop()
	return rh.start()
}

func TestAOFSurvivesRestart(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })

	c := rh.start()
	c.do("SET", "greeting", "hello")
	c.do("RPUSH", "jobs", "a", "b", "c")
	c.do("HSET", "user", "name", "nafiz", "city", "dhaka")
	c.do("SADD", "tags", "go", "redis")
	c.do("ZADD", "board", "10", "x", "20", "y")
	c.do("INCRBY", "counter", "41")
	c.do("INCR", "counter")

	c = rh.restart()

	runCmdTests(t, c, []cmdTest{
		{[]string{"GET", "greeting"}, "hello"},
		{[]string{"LRANGE", "jobs", "0", "-1"}, "[a b c]"},
		{[]string{"HGET", "user", "name"}, "nafiz"},
		{[]string{"HLEN", "user"}, "(integer) 2"},
		{[]string{"SCARD", "tags"}, "(integer) 2"},
		{[]string{"SISMEMBER", "tags", "go"}, "(integer) 1"},
		{[]string{"ZSCORE", "board", "y"}, "20"},
		{[]string{"ZCARD", "board"}, "(integer) 2"},
		{[]string{"GET", "counter"}, "42"},
		{[]string{"DBSIZE"}, "(integer) 6"},
	})
}

// Deletes and overwrites must survive too, or a replay reconstructs data the
// user removed.
func TestAOFReplaysDeletions(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })

	c := rh.start()
	c.do("SET", "keep", "v1")
	c.do("SET", "gone", "v")
	c.do("DEL", "gone")
	c.do("SET", "keep", "v2")
	c.do("RPUSH", "list", "a", "b")
	c.do("LPOP", "list")

	c = rh.restart()

	runCmdTests(t, c, []cmdTest{
		{[]string{"GET", "keep"}, "v2"},
		{[]string{"EXISTS", "gone"}, "(integer) 0"},
		{[]string{"LRANGE", "list", "0", "-1"}, "[b]"},
		{[]string{"DBSIZE"}, "(integer) 2"},
	})
}

// A relative TTL must be recorded as an absolute deadline, or every restart
// silently renews the key.
func TestAOFRewritesRelativeExpiry(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })

	c := rh.start()
	c.do("SET", "session", "token", "EX", "100")
	c.do("SET", "plain", "v")
	c.do("EXPIRE", "plain", "200")

	// Advance the clock so a naive replay would restart both countdowns.
	rh.h.clk.advance(50_000)
	c = rh.restart()
	rh.h.clk.ms.Store(1_700_000_000_000 + 50_000)

	// 50 of the 100 seconds are gone, so about 50 should remain.
	if got := c.do("TTL", "session"); got != "(integer) 50" {
		t.Errorf("TTL after restart: got %s, want 50 -- the deadline was renewed", got)
	}
	if got := c.do("TTL", "plain"); got != "(integer) 150" {
		t.Errorf("TTL after restart: got %s, want 150", got)
	}

	// The log itself must contain the absolute form, not the relative one.
	data, err := os.ReadFile(filepath.Join(rh.dir, "gedis.aof"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "PXAT") {
		t.Error("the log does not contain an absolute SET deadline")
	}
	if !strings.Contains(string(data), "PEXPIREAT") {
		t.Error("the log does not contain an absolute EXPIRE")
	}
}

// A key whose deadline passed while the server was down must not come back.
func TestAOFDropsExpiredKeysOnReplay(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })

	c := rh.start()
	c.do("SET", "shortlived", "v", "PX", "1000")
	c.do("SET", "permanent", "v")

	rh.h.clk.advance(5000)
	c = rh.restart()
	rh.h.clk.ms.Store(1_700_000_000_000 + 5000)

	if got := c.do("GET", "shortlived"); got != "(nil)" {
		t.Errorf("an expired key came back after restart: %s", got)
	}
	if got := c.do("GET", "permanent"); got != "v" {
		t.Errorf("permanent key: got %s", got)
	}
}

// SPOP picks at random, so it must be logged as the removal it performed.
func TestAOFRewritesRandomCommands(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })

	c := rh.start()
	c.do("SADD", "s", "a", "b", "c", "d", "e")
	c.send("SPOP", "s", "2")
	popped := c.read().Array
	if len(popped) != 2 {
		t.Fatalf("SPOP returned %d members", len(popped))
	}
	remaining := c.do("SCARD", "s")

	c = rh.restart()

	if got := c.do("SCARD", "s"); got != remaining {
		t.Errorf("set size after restart: got %s, want %s", got, remaining)
	}
	// The exact members that were popped must be the ones still missing.
	for _, m := range popped {
		if got := c.do("SISMEMBER", "s", string(m.Str)); got != "(integer) 0" {
			t.Errorf("popped member %q came back after restart", m.Str)
		}
	}

	data, err := os.ReadFile(filepath.Join(rh.dir, "gedis.aof"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "SPOP") {
		t.Error("SPOP was logged verbatim; replay would remove different members")
	}
	if !strings.Contains(string(data), "SREM") {
		t.Error("SPOP was not rewritten to SREM")
	}
}

// A crash mid-write leaves a partial command. Starting must succeed, keeping
// everything before the tear.
func TestAOFTruncatedTailIsTolerated(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })

	c := rh.start()
	c.do("SET", "a", "1")
	c.do("SET", "b", "2")
	c.do("SET", "c", "3")
	rh.stop()

	// Chop the file mid-command, the way a power cut would.
	path := filepath.Join(rh.dir, "gedis.aof")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data[:len(data)-8], 0o644); err != nil {
		t.Fatal(err)
	}

	c = rh.start()
	// The first two writes were complete and must be intact.
	if got := c.do("GET", "a"); got != "1" {
		t.Errorf("a: got %s, want 1", got)
	}
	if got := c.do("GET", "b"); got != "2" {
		t.Errorf("b: got %s, want 2", got)
	}
	// And the server must be fully usable afterwards.
	if got := c.do("SET", "d", "4"); got != "OK" {
		t.Errorf("server unusable after a truncated log: %s", got)
	}
}

// A rewrite must compact the log without changing what it reconstructs.
func TestAOFRewriteCompacts(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })

	c := rh.start()
	// A counter incremented many times leaves one integer but a long history.
	for i := 0; i < 500; i++ {
		c.do("INCR", "counter")
	}
	c.do("SET", "other", "v", "EX", "1000")
	c.do("RPUSH", "list", "a", "b", "c")

	before := rh.srv.aof.Size()

	if got := c.do("BGREWRITEAOF"); got != "Background append only file rewriting started" {
		t.Fatalf("BGREWRITEAOF: %s", got)
	}
	// The rewrite runs on its own goroutine; wait for it to finish.
	deadline := time.Now().Add(5 * time.Second)
	for rh.srv.rewriting.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if rh.srv.rewriting.Load() {
		t.Fatal("the rewrite did not finish within 5s")
	}

	after := rh.srv.aof.Size()
	if after >= before {
		t.Errorf("the rewrite did not shrink the log: %d -> %d", before, after)
	}

	// Writes that arrive after the rewrite must still be recorded.
	c.do("SET", "afterwards", "yes")

	c = rh.restart()
	runCmdTests(t, c, []cmdTest{
		{[]string{"GET", "counter"}, "500"},
		{[]string{"GET", "other"}, "v"},
		{[]string{"LRANGE", "list", "0", "-1"}, "[a b c]"},
		{[]string{"GET", "afterwards"}, "yes"},
	})
	if got := c.do("TTL", "other"); got == "(integer) -1" {
		t.Error("the rewrite lost the key's deadline")
	}
}

func TestSnapshotSaveAndLoad(t *testing.T) {
	rh := newRestartHarness(t, nil) // snapshots, not the log

	c := rh.start()
	c.do("SET", "greeting", "hello")
	c.do("RPUSH", "jobs", "a", "b", "c")
	c.do("HSET", "user", "name", "nafiz")
	c.do("SADD", "tags", "go", "redis")
	c.do("ZADD", "board", "1.5", "x", "inf", "y", "-inf", "z")
	c.do("SET", "expiring", "v", "EX", "1000")
	c.do("SELECT", "2")
	c.do("SET", "other-db", "v")
	c.do("SELECT", "0")

	if got := c.do("SAVE"); got != "OK" {
		t.Fatalf("SAVE: %s", got)
	}

	c = rh.restart()

	runCmdTests(t, c, []cmdTest{
		{[]string{"GET", "greeting"}, "hello"},
		{[]string{"LRANGE", "jobs", "0", "-1"}, "[a b c]"},
		{[]string{"HGET", "user", "name"}, "nafiz"},
		{[]string{"SCARD", "tags"}, "(integer) 2"},
		// Infinite scores must round-trip exactly.
		{[]string{"ZSCORE", "board", "x"}, "1.5"},
		{[]string{"ZSCORE", "board", "y"}, "inf"},
		{[]string{"ZSCORE", "board", "z"}, "-inf"},
		{[]string{"TTL", "expiring"}, "(integer) 1000"},
	})

	// Databases other than 0 must survive as well.
	c.do("SELECT", "2")
	if got := c.do("GET", "other-db"); got != "v" {
		t.Errorf("db2 key: got %s", got)
	}
}

// Binary payloads must survive the binary format.
func TestSnapshotBinarySafety(t *testing.T) {
	rh := newRestartHarness(t, nil)

	c := rh.start()
	payload := "line1\r\nline2\x00\xff\xfe end"
	c.do("SET", "binary", payload)
	c.do("HSET", "h", "field\x00with\x00nulls", payload)
	c.do("SADD", "s", payload)
	c.do("SAVE")

	c = rh.restart()

	if got := c.do("GET", "binary"); got != payload {
		t.Errorf("binary string did not round-trip: %q", got)
	}
	if got := c.do("HGET", "h", "field\x00with\x00nulls"); got != payload {
		t.Errorf("binary hash field did not round-trip: %q", got)
	}
	if got := c.do("SISMEMBER", "s", payload); got != "(integer) 1" {
		t.Errorf("binary set member did not round-trip: %s", got)
	}
}

// A corrupted snapshot must be refused, not partially applied.
func TestSnapshotDetectsCorruption(t *testing.T) {
	rh := newRestartHarness(t, nil)

	c := rh.start()
	for i := 0; i < 50; i++ {
		c.do("SET", fmt.Sprintf("key%d", i), "value")
	}
	c.do("SAVE")
	rh.stop()

	// Flip a byte in the middle of the payload.
	path := filepath.Join(rh.dir, "gedis.rdb")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// Starting must fail loudly rather than load half the keys.
	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	cfg.Dir = rh.dir
	cfg.SavePoints = nil
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	err = srv.Serve(context.Background())
	srv.Close()

	if err == nil {
		t.Fatal("a corrupted snapshot was accepted")
	}
	if !strings.Contains(err.Error(), "checksum") && !strings.Contains(err.Error(), "snapshot") {
		t.Errorf("unhelpful error for a corrupt snapshot: %v", err)
	}
}

func TestLastSaveAndBGSave(t *testing.T) {
	rh := newRestartHarness(t, nil)
	c := rh.start()

	c.send("LASTSAVE")
	first := c.read()
	if first.Int == 0 {
		t.Error("LASTSAVE returned 0 on a fresh server")
	}

	c.do("SET", "k", "v")
	if got := c.do("BGSAVE"); got != "Background saving started" {
		t.Errorf("BGSAVE: %s", got)
	}
	deadline := time.Now().Add(5 * time.Second)
	for rh.srv.saving.Load() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(rh.dir, "gedis.rdb")); err != nil {
		t.Errorf("BGSAVE did not write a snapshot: %v", err)
	}
}

func TestPersistenceInfo(t *testing.T) {
	rh := newRestartHarness(t, func(cfg *Config) { cfg.AppendOnly = true })
	c := rh.start()
	c.do("SET", "k", "v")

	c.send("INFO", "persistence")
	info := string(c.read().Str)
	for _, want := range []string{
		"# Persistence", "loading:0", "aof_enabled:1",
		"aof_current_size:", "rdb_changes_since_last_save:",
	} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO persistence is missing %q\n%s", want, info)
		}
	}
}
