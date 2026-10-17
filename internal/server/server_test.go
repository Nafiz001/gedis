package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Nafiz001/gedis/internal/resp"
)

// These are end-to-end tests: they start a real listener, connect over TCP, and
// speak RESP. Testing through the socket rather than by calling handlers
// directly is what catches the mistakes that matter here -- reply framing, the
// exact error strings clients switch on, and pipelining behaviour.

// testClock drives key expiration without sleeping.
type testClock struct{ ms atomic.Int64 }

func (c *testClock) now() int64       { return c.ms.Load() }
func (c *testClock) advance(ms int64) { c.ms.Add(ms) }

type harness struct {
	srv  *Server
	addr string
	clk  *testClock
}

// startServer brings up a server on an ephemeral port and tears it down when
// the test ends.
func startServer(t *testing.T, tweak func(*Config)) *harness {
	t.Helper()

	clk := &testClock{}
	clk.ms.Store(1_700_000_000_000)

	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0" // port 0: let the OS pick, so tests can run in parallel
	cfg.Databases = 4
	cfg.Shards = 8
	cfg.Clock = clk.now
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	if tweak != nil {
		tweak(&cfg)
	}

	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatalf("listen: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not shut down within 5s")
		}
	})

	return &harness{srv: srv, addr: srv.Addr().String(), clk: clk}
}

// conn is a minimal RESP client for the tests.
type conn struct {
	t  *testing.T
	nc net.Conn
	r  *resp.Reader
	w  *resp.Writer
}

func (h *harness) dial(t *testing.T) *conn {
	t.Helper()
	nc, err := net.DialTimeout("tcp", h.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = nc.Close() })
	_ = nc.SetDeadline(time.Now().Add(15 * time.Second))
	return &conn{t: t, nc: nc, r: resp.NewReader(nc), w: resp.NewWriter(nc)}
}

// send writes a command without reading the reply, for pipelining tests.
func (c *conn) send(args ...string) {
	c.t.Helper()
	if err := c.w.WriteCommand(args...); err != nil {
		c.t.Fatalf("write %v: %v", args, err)
	}
	if err := c.w.Flush(); err != nil {
		c.t.Fatalf("flush %v: %v", args, err)
	}
}

func (c *conn) read() resp.Value {
	c.t.Helper()
	v, err := c.r.ReadValue()
	if err != nil {
		c.t.Fatalf("read reply: %v", err)
	}
	return v
}

// do sends a command and returns the reply rendered the way redis-cli prints
// it, which makes the expectations in the tables below readable.
func (c *conn) do(args ...string) string {
	c.t.Helper()
	c.send(args...)
	return c.read().String()
}

// cmdTest is one command and the reply it must produce.
type cmdTest struct {
	cmd  []string
	want string
}

func runCmdTests(t *testing.T, c *conn, tests []cmdTest) {
	t.Helper()
	for i, tc := range tests {
		got := c.do(tc.cmd...)
		if got != tc.want {
			t.Errorf("step %d: %s\n  got:  %s\n  want: %s",
				i, strings.Join(tc.cmd, " "), got, tc.want)
		}
	}
}

func TestConnectionCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"PING"}, "PONG"},
		{[]string{"PING", "hello"}, "hello"},
		{[]string{"ECHO", "hello world"}, "hello world"},
		{[]string{"ECHO"}, "(error) ERR wrong number of arguments for 'echo' command"},
		{[]string{"SELECT", "1"}, "OK"},
		{[]string{"SELECT", "99"}, "(error) ERR DB index is out of range"},
		{[]string{"SELECT", "abc"}, "(error) ERR value is not an integer or out of range"},
		{[]string{"CLIENT", "SETNAME", "tester"}, "OK"},
		{[]string{"CLIENT", "GETNAME"}, "tester"},
		{[]string{"CLIENT", "SETNAME", "bad name"}, "(error) ERR Client names cannot contain spaces, newlines or special characters."},
		{[]string{"HELLO", "3"}, "(error) NOPROTO unsupported protocol version"},
	})

	// COMMAND DOCS is what redis-cli sends on connect; it must not error.
	if got := c.do("COMMAND", "DOCS"); got != "[]" {
		t.Errorf("COMMAND DOCS: got %s", got)
	}
	if got := c.do("COMMAND", "COUNT"); !strings.HasPrefix(got, "(integer)") {
		t.Errorf("COMMAND COUNT: got %s", got)
	}
}

func TestStringCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"GET", "missing"}, "(nil)"},
		{[]string{"SET", "k", "v"}, "OK"},
		{[]string{"GET", "k"}, "v"},
		{[]string{"STRLEN", "k"}, "(integer) 1"},
		{[]string{"STRLEN", "missing"}, "(integer) 0"},
		{[]string{"APPEND", "k", "alue"}, "(integer) 5"},
		{[]string{"GET", "k"}, "value"},
		{[]string{"APPEND", "fresh", "new"}, "(integer) 3"},
		{[]string{"GETSET", "k", "replaced"}, "value"},
		{[]string{"GETSET", "brandnew", "x"}, "(nil)"},
		{[]string{"GETDEL", "k"}, "replaced"},
		{[]string{"EXISTS", "k"}, "(integer) 0"},
		{[]string{"GETDEL", "k"}, "(nil)"},

		// Multi-key.
		{[]string{"MSET", "a", "1", "b", "2", "c", "3"}, "OK"},
		{[]string{"MGET", "a", "b", "c", "nope"}, "[1 2 3 (nil)]"},
		{[]string{"MSET", "a", "1", "b"}, "(error) ERR wrong number of arguments for 'mset' command"},
		{[]string{"MSETNX", "a", "9", "z", "9"}, "(integer) 0"},
		{[]string{"GET", "a"}, "1"},
		{[]string{"MSETNX", "y", "9", "z", "9"}, "(integer) 1"},

		// Ranges.
		{[]string{"SET", "s", "Hello World"}, "OK"},
		{[]string{"GETRANGE", "s", "0", "4"}, "Hello"},
		{[]string{"GETRANGE", "s", "-5", "-1"}, "World"},
		{[]string{"GETRANGE", "s", "0", "-1"}, "Hello World"},
		{[]string{"GETRANGE", "s", "100", "200"}, ""},
		{[]string{"GETRANGE", "missing", "0", "-1"}, ""},
		{[]string{"SETRANGE", "s", "6", "Redis"}, "(integer) 11"},
		{[]string{"GET", "s"}, "Hello Redis"},
		{[]string{"SETRANGE", "s", "-1", "x"}, "(error) ERR offset is out of range"},
		// Writing past the end zero-pads the gap.
		{[]string{"SETRANGE", "pad", "3", "x"}, "(integer) 4"},
		{[]string{"GET", "pad"}, "\x00\x00\x00x"},
		// An empty value must not create a key.
		{[]string{"SETRANGE", "nocreate", "0", ""}, "(integer) 0"},
		{[]string{"EXISTS", "nocreate"}, "(integer) 0"},
	})
}

func TestCounterCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"INCR", "n"}, "(integer) 1"}, // creates the key at 0 then adds
		{[]string{"INCR", "n"}, "(integer) 2"},
		{[]string{"INCRBY", "n", "10"}, "(integer) 12"},
		{[]string{"DECR", "n"}, "(integer) 11"},
		{[]string{"DECRBY", "n", "11"}, "(integer) 0"},
		{[]string{"INCRBY", "n", "notanumber"}, "(error) ERR value is not an integer or out of range"},

		// Non-numeric contents.
		{[]string{"SET", "word", "abc"}, "OK"},
		{[]string{"INCR", "word"}, "(error) ERR value is not an integer or out of range"},
		// A float is not an integer, even though it parses as a number.
		{[]string{"SET", "f", "3.0"}, "OK"},
		{[]string{"INCR", "f"}, "(error) ERR value is not an integer or out of range"},
		// Leading whitespace is rejected too.
		{[]string{"SET", "sp", " 1"}, "OK"},
		{[]string{"INCR", "sp"}, "(error) ERR value is not an integer or out of range"},

		// Overflow must be reported, not silently wrapped.
		{[]string{"SET", "big", "9223372036854775807"}, "OK"},
		{[]string{"INCR", "big"}, "(error) ERR increment or decrement would overflow"},
		{[]string{"GET", "big"}, "9223372036854775807"},
		{[]string{"SET", "small", "-9223372036854775808"}, "OK"},
		{[]string{"DECR", "small"}, "(error) ERR increment or decrement would overflow"},

		// Floats.
		{[]string{"SET", "fl", "10.5"}, "OK"},
		{[]string{"INCRBYFLOAT", "fl", "0.1"}, "10.6"},
		{[]string{"INCRBYFLOAT", "fl", "-5"}, "5.6"},
		{[]string{"INCRBYFLOAT", "newfloat", "3.25"}, "3.25"},
		{[]string{"INCRBYFLOAT", "fl", "notafloat"}, "(error) ERR value is not a valid float"},
		{[]string{"INCRBYFLOAT", "word", "1"}, "(error) ERR value is not a valid float"},
	})
}

func TestSetOptions(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		// NX creates only when absent.
		{[]string{"SET", "k", "first", "NX"}, "OK"},
		{[]string{"SET", "k", "second", "NX"}, "(nil)"},
		{[]string{"GET", "k"}, "first"},

		// XX updates only when present.
		{[]string{"SET", "k", "third", "XX"}, "OK"},
		{[]string{"GET", "k"}, "third"},
		{[]string{"SET", "absent", "v", "XX"}, "(nil)"},
		{[]string{"EXISTS", "absent"}, "(integer) 0"},

		// GET returns the previous value.
		{[]string{"SET", "k", "fourth", "GET"}, "third"},
		{[]string{"SET", "unset", "v", "GET"}, "(nil)"},

		// Expiry modifiers.
		{[]string{"SET", "e", "v", "EX", "100"}, "OK"},
		{[]string{"TTL", "e"}, "(integer) 100"},
		{[]string{"SET", "e", "v2", "PX", "50000"}, "OK"},
		{[]string{"TTL", "e"}, "(integer) 50"},
		// A plain SET clears the TTL...
		{[]string{"SET", "e", "v3"}, "OK"},
		{[]string{"TTL", "e"}, "(integer) -1"},
		// ...but KEEPTTL preserves it.
		{[]string{"SET", "e", "v", "EX", "100"}, "OK"},
		{[]string{"SET", "e", "v4", "KEEPTTL"}, "OK"},
		{[]string{"TTL", "e"}, "(integer) 100"},

		// Bad syntax.
		{[]string{"SET", "k", "v", "NX", "XX"}, "(error) ERR syntax error"},
		{[]string{"SET", "k", "v", "EX"}, "(error) ERR syntax error"},
		{[]string{"SET", "k", "v", "EX", "0"}, "(error) ERR invalid expire time in 'set' command"},
		{[]string{"SET", "k", "v", "EX", "-1"}, "(error) ERR invalid expire time in 'set' command"},
		{[]string{"SET", "k", "v", "EX", "abc"}, "(error) ERR value is not an integer or out of range"},
		{[]string{"SET", "k", "v", "EX", "100", "KEEPTTL"}, "(error) ERR syntax error"},
		{[]string{"SET", "k", "v", "BOGUS"}, "(error) ERR syntax error"},
		// An overflowing TTL must be rejected, not wrapped into the past.
		{[]string{"SET", "k", "v", "EX", "9223372036854775807"}, "(error) ERR invalid expire time in 'set' command"},

		// SETEX / PSETEX.
		{[]string{"SETEX", "sx", "60", "v"}, "OK"},
		{[]string{"TTL", "sx"}, "(integer) 60"},
		{[]string{"SETEX", "sx", "0", "v"}, "(error) ERR invalid expire time in 'setex' command"},
		{[]string{"PSETEX", "px", "60000", "v"}, "OK"},
		{[]string{"TTL", "px"}, "(integer) 60"},
	})
}

func TestExpiration(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"SET", "k", "v"}, "OK"},
		{[]string{"TTL", "k"}, "(integer) -1"},
		{[]string{"TTL", "missing"}, "(integer) -2"},
		{[]string{"PTTL", "missing"}, "(integer) -2"},
		{[]string{"EXPIRE", "missing", "10"}, "(integer) 0"},

		{[]string{"EXPIRE", "k", "100"}, "(integer) 1"},
		{[]string{"TTL", "k"}, "(integer) 100"},
		{[]string{"PTTL", "k"}, "(integer) 100000"},
		{[]string{"PERSIST", "k"}, "(integer) 1"},
		{[]string{"TTL", "k"}, "(integer) -1"},
		{[]string{"PERSIST", "k"}, "(integer) 0"},

		// Conditional forms.
		{[]string{"EXPIRE", "k", "100", "XX"}, "(integer) 0"}, // no TTL yet
		{[]string{"EXPIRE", "k", "100", "NX"}, "(integer) 1"},
		{[]string{"EXPIRE", "k", "200", "NX"}, "(integer) 0"}, // already has one
		{[]string{"EXPIRE", "k", "200", "GT"}, "(integer) 1"},
		{[]string{"TTL", "k"}, "(integer) 200"},
		{[]string{"EXPIRE", "k", "100", "GT"}, "(integer) 0"},
		{[]string{"EXPIRE", "k", "100", "LT"}, "(integer) 1"},
		{[]string{"TTL", "k"}, "(integer) 100"},
		{[]string{"EXPIRE", "k", "10", "NX", "XX"}, "(error) ERR NX and XX, GT or LT options at the same time are not compatible"},

		// A non-positive EXPIRE deletes the key outright.
		{[]string{"SET", "doomed", "v"}, "OK"},
		{[]string{"EXPIRE", "doomed", "-1"}, "(integer) 1"},
		{[]string{"EXISTS", "doomed"}, "(integer) 0"},
	})

	// Now actually cross a deadline.
	c.do("SET", "ttlkey", "v", "PX", "1000")
	if got := c.do("GET", "ttlkey"); got != "v" {
		t.Fatalf("before expiry: got %s", got)
	}
	h.clk.advance(1001)
	if got := c.do("GET", "ttlkey"); got != "(nil)" {
		t.Errorf("after expiry: got %s, want (nil)", got)
	}
	if got := c.do("EXISTS", "ttlkey"); got != "(integer) 0" {
		t.Errorf("EXISTS after expiry: got %s", got)
	}
	if got := c.do("TTL", "ttlkey"); got != "(integer) -2" {
		t.Errorf("TTL after expiry: got %s", got)
	}
}

// GETEX reads a value and changes its TTL in one step.
func TestGetEx(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"SET", "k", "v", "EX", "100"}, "OK"},
		{[]string{"GETEX", "k"}, "v"},
		{[]string{"TTL", "k"}, "(integer) 100"}, // no options: TTL untouched
		{[]string{"GETEX", "k", "EX", "200"}, "v"},
		{[]string{"TTL", "k"}, "(integer) 200"},
		{[]string{"GETEX", "k", "PERSIST"}, "v"},
		{[]string{"TTL", "k"}, "(integer) -1"},
		{[]string{"GETEX", "missing"}, "(nil)"},
	})
}

func TestKeyspaceCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"SET", "a", "1"}, "OK"},
		{[]string{"SET", "b", "2"}, "OK"},
		{[]string{"EXISTS", "a", "b", "missing"}, "(integer) 2"},
		// EXISTS counts a repeated key more than once.
		{[]string{"EXISTS", "a", "a", "a"}, "(integer) 3"},
		{[]string{"TYPE", "a"}, "string"},
		{[]string{"TYPE", "missing"}, "none"},
		{[]string{"DEL", "a", "missing"}, "(integer) 1"},
		{[]string{"UNLINK", "b"}, "(integer) 1"},
		{[]string{"DBSIZE"}, "(integer) 0"},

		// RENAME.
		{[]string{"SET", "src", "v"}, "OK"},
		{[]string{"RENAME", "src", "dst"}, "OK"},
		{[]string{"GET", "dst"}, "v"},
		{[]string{"RENAME", "gone", "x"}, "(error) ERR no such key"},
		{[]string{"SET", "other", "o"}, "OK"},
		{[]string{"RENAMENX", "dst", "other"}, "(integer) 0"},
		{[]string{"RENAMENX", "dst", "fresh"}, "(integer) 1"},
		{[]string{"GET", "fresh"}, "v"},

		// COPY.
		{[]string{"COPY", "fresh", "clone"}, "(integer) 1"},
		{[]string{"GET", "clone"}, "v"},
		{[]string{"COPY", "fresh", "clone"}, "(integer) 0"}, // exists, no REPLACE
		{[]string{"COPY", "fresh", "clone", "REPLACE"}, "(integer) 1"},
		{[]string{"COPY", "missing", "x"}, "(integer) 0"},
		{[]string{"COPY", "fresh", "fresh"}, "(error) ERR source and destination objects are the same"},
	})

	// A copy must not share storage with its source.
	c.do("SET", "orig", "abc")
	c.do("COPY", "orig", "dup")
	c.do("APPEND", "orig", "XYZ")
	if got := c.do("GET", "dup"); got != "abc" {
		t.Errorf("COPY aliased the value: dup is %q after appending to orig", got)
	}
}

func TestKeysAndScan(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	for i := 0; i < 30; i++ {
		c.do("SET", fmt.Sprintf("user:%d", i), "v")
	}
	for i := 0; i < 10; i++ {
		c.do("SET", fmt.Sprintf("post:%d", i), "v")
	}

	if got := c.do("DBSIZE"); got != "(integer) 40" {
		t.Errorf("DBSIZE: got %s", got)
	}

	c.send("KEYS", "user:*")
	if n := len(c.read().Array); n != 30 {
		t.Errorf("KEYS user:*: got %d keys, want 30", n)
	}
	c.send("KEYS", "*")
	if n := len(c.read().Array); n != 40 {
		t.Errorf("KEYS *: got %d keys, want 40", n)
	}

	// A full SCAN must terminate and cover every key exactly once.
	seen := map[string]int{}
	cursor := "0"
	for i := 0; ; i++ {
		if i > 500 {
			t.Fatal("SCAN did not terminate")
		}
		c.send("SCAN", cursor, "COUNT", "5")
		v := c.read()
		if len(v.Array) != 2 {
			t.Fatalf("SCAN reply shape: %s", v.String())
		}
		cursor = string(v.Array[0].Str)
		for _, k := range v.Array[1].Array {
			seen[string(k.Str)]++
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 40 {
		t.Errorf("SCAN covered %d keys, want 40", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("SCAN returned %q %d times", k, n)
		}
	}

	if got := c.do("SCAN", "notanumber"); got != "(error) ERR invalid cursor" {
		t.Errorf("SCAN bad cursor: got %s", got)
	}

	// RANDOMKEY must return something that actually exists.
	c.send("RANDOMKEY")
	key := string(c.read().Str)
	if got := c.do("EXISTS", key); got != "(integer) 1" {
		t.Errorf("RANDOMKEY returned %q which does not exist", key)
	}

	c.do("FLUSHDB")
	if got := c.do("DBSIZE"); got != "(integer) 0" {
		t.Errorf("DBSIZE after FLUSHDB: got %s", got)
	}
	if got := c.do("RANDOMKEY"); got != "(nil)" {
		t.Errorf("RANDOMKEY on empty db: got %s", got)
	}
}

func TestErrorHandling(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"NOSUCHCOMMAND"}, `(error) ERR unknown command 'NOSUCHCOMMAND', with args beginning with: `},
		{[]string{"NOSUCHCOMMAND", "a", "b"}, `(error) ERR unknown command 'NOSUCHCOMMAND', with args beginning with: "a", "b"`},
		{[]string{"GET"}, "(error) ERR wrong number of arguments for 'get' command"},
		{[]string{"GET", "a", "b"}, "(error) ERR wrong number of arguments for 'get' command"},
		{[]string{"SET", "k"}, "(error) ERR wrong number of arguments for 'set' command"},
		{[]string{"DEL"}, "(error) ERR wrong number of arguments for 'del' command"},
	})

	// Command names are case-insensitive.
	if got := c.do("pInG"); got != "PONG" {
		t.Errorf("mixed-case command: got %s", got)
	}

	// The connection stays usable after every one of those errors.
	if got := c.do("PING"); got != "PONG" {
		t.Errorf("connection unusable after errors: %s", got)
	}
}

// A malformed frame must be reported and the connection closed, since the
// stream cannot be resynchronised afterwards.
func TestProtocolErrorClosesConnection(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	if _, err := c.nc.Write([]byte("*3\r\n$3\r\nSET\r\n+notabulk\r\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	v, err := c.r.ReadValue()
	if err != nil {
		t.Fatalf("expected an error reply, got read error: %v", err)
	}
	if v.Type != resp.TypeError || !strings.Contains(v.String(), "Protocol error") {
		t.Errorf("got %s, want a protocol error", v.String())
	}
	// The server must then hang up.
	_ = c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadValue(); err == nil {
		t.Error("connection stayed open after a protocol error")
	}
}

func TestInlineCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	// The telnet-style form, which is how you debug a server with netcat.
	steps := []struct{ send, want string }{
		{"PING\r\n", "PONG"},
		{"SET greeting \"hello world\"\r\n", "OK"},
		{"GET greeting\r\n", "hello world"},
		{"DEL greeting\r\n", "(integer) 1"},
	}
	for _, s := range steps {
		if _, err := c.nc.Write([]byte(s.send)); err != nil {
			t.Fatalf("write %q: %v", s.send, err)
		}
		if got := c.read().String(); got != s.want {
			t.Errorf("%q: got %s, want %s", s.send, got, s.want)
		}
	}
}

// Pipelining: many commands in one write, replies in order, one batch.
func TestPipelining(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	const n = 500
	for i := 0; i < n; i++ {
		if err := c.w.WriteCommand("SET", fmt.Sprintf("key:%d", i), fmt.Sprintf("val:%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if got := c.read().String(); got != "OK" {
			t.Fatalf("reply %d: got %s, want OK", i, got)
		}
	}

	// Replies must come back in request order, not just in the right count.
	for i := 0; i < n; i++ {
		if err := c.w.WriteCommand("GET", fmt.Sprintf("key:%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.w.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		want := fmt.Sprintf("val:%d", i)
		if got := c.read().String(); got != want {
			t.Fatalf("reply %d out of order: got %s, want %s", i, got, want)
		}
	}
}

// Each SELECT-able database is a separate keyspace.
func TestDatabaseIsolation(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	c.do("SET", "k", "db0")
	c.do("SELECT", "1")
	if got := c.do("GET", "k"); got != "(nil)" {
		t.Errorf("db1 sees db0 key: %s", got)
	}
	c.do("SET", "k", "db1")
	c.do("SELECT", "0")
	if got := c.do("GET", "k"); got != "db0" {
		t.Errorf("db0 value changed: %s", got)
	}

	// FLUSHDB clears only the current database; FLUSHALL clears every one.
	c.do("FLUSHDB")
	c.do("SELECT", "1")
	if got := c.do("GET", "k"); got != "db1" {
		t.Errorf("FLUSHDB cleared the wrong database: %s", got)
	}
	c.do("FLUSHALL")
	if got := c.do("DBSIZE"); got != "(integer) 0" {
		t.Errorf("FLUSHALL left db1 populated: %s", got)
	}

	// A second connection starts on db0, not on the first client's selection.
	c2 := h.dial(t)
	c2.do("SET", "fresh", "v")
	if got := c2.do("DBSIZE"); got != "(integer) 1" {
		t.Errorf("new connection did not start on db0: %s", got)
	}
}

func TestAuth(t *testing.T) {
	h := startServer(t, func(cfg *Config) { cfg.Password = "correct horse" })
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"GET", "k"}, "(error) NOAUTH Authentication required."},
		{[]string{"AUTH", "wrong"}, "(error) WRONGPASS invalid username-password pair or user is disabled."},
		{[]string{"GET", "k"}, "(error) NOAUTH Authentication required."},
		{[]string{"AUTH", "correct horse"}, "OK"},
		{[]string{"SET", "k", "v"}, "OK"},
		{[]string{"GET", "k"}, "v"},
		// The username form is accepted for the implicit default user.
		{[]string{"AUTH", "default", "correct horse"}, "OK"},
		{[]string{"AUTH", "other", "correct horse"}, "(error) WRONGPASS invalid username-password pair or user is disabled."},
	})

	// RESET drops the authentication again.
	if got := c.do("RESET"); got != "RESET" {
		t.Errorf("RESET: got %s", got)
	}
	if got := c.do("GET", "k"); got != "(error) NOAUTH Authentication required." {
		t.Errorf("still authenticated after RESET: %s", got)
	}

	// With no password configured, AUTH is an error rather than a no-op.
	h2 := startServer(t, nil)
	c2 := h2.dial(t)
	if got := c2.do("AUTH", "anything"); !strings.Contains(got, "no password is set") {
		t.Errorf("AUTH without requirepass: got %s", got)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	if got := c.do("QUIT"); got != "OK" {
		t.Fatalf("QUIT: got %s", got)
	}
	_ = c.nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.r.ReadValue(); err == nil {
		t.Error("connection stayed open after QUIT")
	}
}

func TestInfoAndServerCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SET", "k", "v")

	c.send("INFO")
	info := string(c.read().Str)
	for _, want := range []string{
		"# Server", "gedis_version:", "# Clients", "connected_clients:",
		"# Memory", "used_memory:", "# Stats", "total_commands_processed:",
		"# Keyspace", "db0:keys=1",
	} {
		if !strings.Contains(info, want) {
			t.Errorf("INFO is missing %q", want)
		}
	}

	c.send("INFO", "clients")
	section := string(c.read().Str)
	if !strings.Contains(section, "# Clients") || strings.Contains(section, "# Memory") {
		t.Errorf("INFO clients returned the wrong sections:\n%s", section)
	}

	c.send("TIME")
	if tv := c.read(); len(tv.Array) != 2 {
		t.Errorf("TIME: got %s", tv.String())
	}

	c.send("CONFIG", "GET", "maxmemory")
	cfgv := c.read()
	if len(cfgv.Array) != 2 || string(cfgv.Array[0].Str) != "maxmemory" {
		t.Errorf("CONFIG GET maxmemory: got %s", cfgv.String())
	}
	// CONFIG GET takes glob patterns.
	c.send("CONFIG", "GET", "max*")
	if n := len(c.read().Array); n < 4 {
		t.Errorf("CONFIG GET max*: got %d elements, want at least 4", n)
	}

	if got := c.do("DEBUG", "NOTASUBCOMMAND"); !strings.HasPrefix(got, "(error)") {
		t.Errorf("unknown DEBUG subcommand: got %s", got)
	}
}

// Many clients hammering the server at once must all get correct replies.
// Run with -race to make the shared-state check meaningful.
func TestConcurrentClients(t *testing.T) {
	h := startServer(t, nil)

	const (
		clients = 16
		ops     = 100
	)
	var wg sync.WaitGroup
	errs := make(chan error, clients)

	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nc, err := net.DialTimeout("tcp", h.addr, 5*time.Second)
			if err != nil {
				errs <- err
				return
			}
			defer nc.Close()
			_ = nc.SetDeadline(time.Now().Add(30 * time.Second))
			r, w := resp.NewReader(nc), resp.NewWriter(nc)

			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("c%d:k%d", id, i)
				// Each client owns its own keys, and also increments one key
				// shared by every client.
				if err := w.WriteCommand("SET", key, "v"); err != nil {
					errs <- err
					return
				}
				if err := w.WriteCommand("GET", key); err != nil {
					errs <- err
					return
				}
				if err := w.WriteCommand("INCR", "shared"); err != nil {
					errs <- err
					return
				}
				if err := w.Flush(); err != nil {
					errs <- err
					return
				}
				for _, want := range []string{"OK", "v"} {
					v, err := r.ReadValue()
					if err != nil {
						errs <- err
						return
					}
					if v.String() != want {
						errs <- fmt.Errorf("client %d op %d: got %s, want %s", id, i, v.String(), want)
						return
					}
				}
				if _, err := r.ReadValue(); err != nil { // the INCR reply
					errs <- err
					return
				}
			}
		}(id)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every INCR must be accounted for: no lost updates across 16 connections.
	c := h.dial(t)
	if got := c.do("GET", "shared"); got != fmt.Sprint(clients*ops) {
		t.Errorf("lost updates: shared counter is %s, want %d", got, clients*ops)
	}
	if got := c.do("DBSIZE"); got != fmt.Sprintf("(integer) %d", clients*ops+1) {
		t.Errorf("DBSIZE: got %s, want %d", got, clients*ops+1)
	}
}

func TestMaxClients(t *testing.T) {
	h := startServer(t, func(cfg *Config) { cfg.MaxClients = 2 })

	a := h.dial(t)
	b := h.dial(t)
	if got := a.do("PING"); got != "PONG" {
		t.Fatalf("client 1: %s", got)
	}
	if got := b.do("PING"); got != "PONG" {
		t.Fatalf("client 2: %s", got)
	}

	// The third connection is accepted, told why, and closed -- rather than
	// dropped silently.
	nc, err := net.DialTimeout("tcp", h.addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer nc.Close()
	_ = nc.SetDeadline(time.Now().Add(5 * time.Second))

	v, err := resp.NewReader(nc).ReadValue()
	if err != nil {
		t.Fatalf("expected a rejection message: %v", err)
	}
	if v.Type != resp.TypeError || !strings.Contains(v.String(), "max number of clients") {
		t.Errorf("got %s, want a max-clients error", v.String())
	}
}

// Shutdown must close the listener and disconnect clients without hanging.
func TestGracefulShutdown(t *testing.T) {
	clk := &testClock{}
	clk.ms.Store(1_700_000_000_000)

	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	cfg.Databases = 2
	cfg.Clock = clk.now
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		t.Fatal(err)
	}
	addr := srv.Addr().String()

	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background()) }()

	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	r, w := resp.NewReader(nc), resp.NewWriter(nc)
	_ = w.WriteCommand("PING")
	_ = w.Flush()
	if v, err := r.ReadValue(); err != nil || v.String() != "PONG" {
		t.Fatalf("PING before shutdown: %v %v", v, err)
	}

	srv.Close()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return within 5s of Close")
	}

	// The port must be free again.
	if _, err := net.DialTimeout("tcp", addr, 1*time.Second); err == nil {
		t.Error("still accepting connections after shutdown")
	}
}

// A wrong-typed operation must produce WRONGTYPE. Until the container types
// land, the store is seeded directly to create a non-string value.
func TestWrongTypeError(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	// A value that is not a []byte stands in for a future list or hash.
	h.srv.DB(0).Set("notastring", struct{ items []string }{}, 0)

	for _, cmd := range [][]string{
		{"GET", "notastring"},
		{"APPEND", "notastring", "x"},
		{"INCR", "notastring"},
		{"STRLEN", "notastring"},
		{"GETDEL", "notastring"},
		{"GETRANGE", "notastring", "0", "-1"},
	} {
		got := c.do(cmd...)
		if !strings.HasPrefix(got, "(error) WRONGTYPE") {
			t.Errorf("%s: got %s, want WRONGTYPE", strings.Join(cmd, " "), got)
		}
	}

	// TYPE reports the unknown value rather than failing.
	if got := c.do("TYPE", "notastring"); got != "none" {
		t.Errorf("TYPE: got %s", got)
	}
	// MGET degrades to nil instead of failing the whole batch.
	if got := c.do("MGET", "notastring"); got != "[(nil)]" {
		t.Errorf("MGET with a wrong-typed key: got %s", got)
	}
}

func BenchmarkServerGetSet(b *testing.B) {
	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg)
	if err := srv.Listen(); err != nil {
		b.Fatal(err)
	}
	go func() { _ = srv.Serve(context.Background()) }()
	defer srv.Close()

	nc, err := net.Dial("tcp", srv.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()
	r, w := resp.NewReader(nc), resp.NewWriter(nc)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = w.WriteCommand("SET", "benchkey", "benchvalue")
		_ = w.Flush()
		if _, err := r.ReadValue(); err != nil {
			b.Fatal(err)
		}
	}
}
