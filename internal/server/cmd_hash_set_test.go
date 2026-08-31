package server

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// sortedReply parses an array reply rendered by resp.Value.String() and sorts
// its elements, so tests can assert on collections whose order is undefined.
func sortedReply(s string) string {
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	if s == "" {
		return "[]"
	}
	parts := strings.Split(s, " ")
	sort.Strings(parts)
	return "[" + strings.Join(parts, " ") + "]"
}

func TestHashCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"HGET", "missing", "f"}, "(nil)"},
		{[]string{"HLEN", "missing"}, "(integer) 0"},
		{[]string{"HEXISTS", "missing", "f"}, "(integer) 0"},
		{[]string{"HGETALL", "missing"}, "[]"},
		{[]string{"HKEYS", "missing"}, "[]"},
		{[]string{"HVALS", "missing"}, "[]"},
		{[]string{"HDEL", "missing", "f"}, "(integer) 0"},
		{[]string{"HSTRLEN", "missing", "f"}, "(integer) 0"},

		{[]string{"HSET", "u", "name", "Nafiz"}, "(integer) 1"},
		{[]string{"HSET", "u", "city", "Dhaka", "role", "backend"}, "(integer) 2"},
		// Overwriting an existing field counts as 0 new fields.
		{[]string{"HSET", "u", "name", "Nafiz Ahmed"}, "(integer) 0"},
		{[]string{"HGET", "u", "name"}, "Nafiz Ahmed"},
		{[]string{"HGET", "u", "nosuchfield"}, "(nil)"},
		{[]string{"HLEN", "u"}, "(integer) 3"},
		{[]string{"HEXISTS", "u", "city"}, "(integer) 1"},
		{[]string{"HEXISTS", "u", "nosuchfield"}, "(integer) 0"},
		{[]string{"HSTRLEN", "u", "city"}, "(integer) 5"},
		{[]string{"HSTRLEN", "u", "nosuchfield"}, "(integer) 0"},

		{[]string{"HMGET", "u", "name", "city", "nosuchfield"}, "[Nafiz Ahmed Dhaka (nil)]"},
		{[]string{"HMGET", "missing", "a", "b"}, "[(nil) (nil)]"},
		{[]string{"HMSET", "m", "a", "1", "b", "2"}, "OK"},
		{[]string{"HGET", "m", "b"}, "2"},
		{[]string{"HSET", "bad", "onlyfield"}, "(error) ERR wrong number of arguments for 'hset' command"},

		{[]string{"HSETNX", "u", "name", "Someone"}, "(integer) 0"},
		{[]string{"HGET", "u", "name"}, "Nafiz Ahmed"},
		{[]string{"HSETNX", "u", "country", "BD"}, "(integer) 1"},
		{[]string{"HGET", "u", "country"}, "BD"},

		{[]string{"HDEL", "u", "country", "nosuchfield"}, "(integer) 1"},
		{[]string{"HLEN", "u"}, "(integer) 3"},

		{[]string{"TYPE", "u"}, "hash"},
	})

	// Order of HKEYS/HVALS/HGETALL is undefined, so compare sorted.
	if got := sortedReply(c.do("HKEYS", "u")); got != "[city name role]" {
		t.Errorf("HKEYS: got %s", got)
	}
	if got := sortedReply(c.do("HVALS", "u")); got != "[Ahmed Dhaka Nafiz backend]" {
		t.Errorf("HVALS: got %s", got)
	}

	// A hash whose last field is removed must disappear entirely.
	c.do("HSET", "solo", "f", "v")
	c.do("HDEL", "solo", "f")
	if got := c.do("EXISTS", "solo"); got != "(integer) 0" {
		t.Errorf("emptied hash still exists: %s", got)
	}
}

func TestHashCounters(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		// A missing hash and a missing field both start at zero.
		{[]string{"HINCRBY", "h", "n", "5"}, "(integer) 5"},
		{[]string{"HINCRBY", "h", "n", "-2"}, "(integer) 3"},
		{[]string{"HINCRBY", "h", "other", "1"}, "(integer) 1"},
		{[]string{"HGET", "h", "n"}, "3"},

		{[]string{"HINCRBYFLOAT", "h", "f", "3.5"}, "3.5"},
		{[]string{"HINCRBYFLOAT", "h", "f", "0.25"}, "3.75"},
		{[]string{"HINCRBYFLOAT", "h", "f", "-4"}, "-0.25"},

		// Error text verified against Redis 7: hashes get their own wording.
		{[]string{"HSET", "h", "word", "abc"}, "(integer) 1"},
		{[]string{"HINCRBY", "h", "word", "1"}, "(error) ERR hash value is not an integer"},
		{[]string{"HINCRBYFLOAT", "h", "word", "1"}, "(error) ERR hash value is not a float"},
		{[]string{"HINCRBY", "h", "n", "notanumber"}, "(error) ERR value is not an integer or out of range"},

		{[]string{"HSET", "h", "big", "9223372036854775807"}, "(integer) 1"},
		{[]string{"HINCRBY", "h", "big", "1"}, "(error) ERR increment or decrement would overflow"},
		{[]string{"HGET", "h", "big"}, "9223372036854775807"},
	})
}

func TestHashRandAndScan(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	// Verified against Redis 7: no count gives nil, a count gives an empty array.
	if got := c.do("HRANDFIELD", "missing"); got != "(nil)" {
		t.Errorf("HRANDFIELD missing: got %s", got)
	}
	if got := c.do("HRANDFIELD", "missing", "3"); got != "[]" {
		t.Errorf("HRANDFIELD missing 3: got %s", got)
	}

	for i := 0; i < 10; i++ {
		c.do("HSET", "h", fmt.Sprintf("f%d", i), fmt.Sprintf("v%d", i))
	}

	// A positive count never repeats and never exceeds the hash size.
	c.send("HRANDFIELD", "h", "5")
	fields := c.read().Array
	if len(fields) != 5 {
		t.Errorf("HRANDFIELD h 5: got %d fields", len(fields))
	}
	seen := map[string]bool{}
	for _, f := range fields {
		if seen[string(f.Str)] {
			t.Errorf("HRANDFIELD with a positive count repeated %q", f.Str)
		}
		seen[string(f.Str)] = true
	}
	c.send("HRANDFIELD", "h", "100")
	if n := len(c.read().Array); n != 10 {
		t.Errorf("HRANDFIELD beyond the size: got %d, want 10", n)
	}
	// A negative count returns exactly that many and may repeat.
	c.send("HRANDFIELD", "h", "-20")
	if n := len(c.read().Array); n != 20 {
		t.Errorf("HRANDFIELD h -20: got %d, want 20", n)
	}
	c.send("HRANDFIELD", "h", "3", "WITHVALUES")
	if n := len(c.read().Array); n != 6 {
		t.Errorf("HRANDFIELD WITHVALUES: got %d elements, want 6", n)
	}

	// A full HSCAN must cover every field exactly once and terminate.
	found := map[string]string{}
	cursor := "0"
	for i := 0; ; i++ {
		if i > 100 {
			t.Fatal("HSCAN did not terminate")
		}
		c.send("HSCAN", "h", cursor, "COUNT", "3")
		v := c.read()
		cursor = string(v.Array[0].Str)
		pairs := v.Array[1].Array
		for j := 0; j+1 < len(pairs); j += 2 {
			found[string(pairs[j].Str)] = string(pairs[j+1].Str)
		}
		if cursor == "0" {
			break
		}
	}
	if len(found) != 10 {
		t.Errorf("HSCAN covered %d fields, want 10", len(found))
	}
	if found["f3"] != "v3" {
		t.Errorf("HSCAN returned wrong value for f3: %q", found["f3"])
	}

	c.send("HSCAN", "h", "0", "MATCH", "f1*", "COUNT", "100")
	v := c.read()
	if n := len(v.Array[1].Array); n != 2 { // f1 and its value
		t.Errorf("HSCAN MATCH f1*: got %d elements, want 2", n)
	}
}

func TestSetCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"SCARD", "missing"}, "(integer) 0"},
		{[]string{"SISMEMBER", "missing", "a"}, "(integer) 0"},
		{[]string{"SMEMBERS", "missing"}, "[]"},
		{[]string{"SREM", "missing", "a"}, "(integer) 0"},

		{[]string{"SADD", "s", "a"}, "(integer) 1"},
		{[]string{"SADD", "s", "b", "c"}, "(integer) 2"},
		{[]string{"SADD", "s", "a", "d"}, "(integer) 1"}, // 'a' is already there
		{[]string{"SCARD", "s"}, "(integer) 4"},
		{[]string{"SISMEMBER", "s", "a"}, "(integer) 1"},
		{[]string{"SISMEMBER", "s", "zzz"}, "(integer) 0"},
		{[]string{"SMISMEMBER", "s", "a", "zzz", "b"}, "[(integer) 1 (integer) 0 (integer) 1]"},
		{[]string{"SREM", "s", "a", "zzz"}, "(integer) 1"},
		{[]string{"SCARD", "s"}, "(integer) 3"},
		{[]string{"TYPE", "s"}, "set"},
	})

	if got := sortedReply(c.do("SMEMBERS", "s")); got != "[b c d]" {
		t.Errorf("SMEMBERS: got %s", got)
	}

	// Removing the last member deletes the key.
	c.do("SADD", "solo", "only")
	c.do("SREM", "solo", "only")
	if got := c.do("EXISTS", "solo"); got != "(integer) 0" {
		t.Errorf("emptied set still exists: %s", got)
	}
}

func TestSetOperations(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SADD", "a", "1", "2", "3", "4")
	c.do("SADD", "b", "3", "4", "5")
	c.do("SADD", "c", "4", "5", "6")

	tests := []struct {
		cmd  []string
		want string
	}{
		{[]string{"SINTER", "a", "b"}, "[3 4]"},
		{[]string{"SINTER", "a", "b", "c"}, "[4]"},
		{[]string{"SINTER", "a", "missing"}, "[]"},
		{[]string{"SINTER", "a"}, "[1 2 3 4]"},
		{[]string{"SUNION", "a", "b"}, "[1 2 3 4 5]"},
		{[]string{"SUNION", "a", "missing"}, "[1 2 3 4]"},
		{[]string{"SDIFF", "a", "b"}, "[1 2]"},
		{[]string{"SDIFF", "a", "b", "c"}, "[1 2]"},
		{[]string{"SDIFF", "b", "a"}, "[5]"},
		{[]string{"SDIFF", "missing", "a"}, "[]"},
	}
	for _, tc := range tests {
		if got := sortedReply(c.do(tc.cmd...)); got != tc.want {
			t.Errorf("%s: got %s, want %s", strings.Join(tc.cmd, " "), got, tc.want)
		}
	}

	// The STORE variants report the size and write the destination.
	runCmdTests(t, c, []cmdTest{
		{[]string{"SINTERSTORE", "dst", "a", "b"}, "(integer) 2"},
		{[]string{"SCARD", "dst"}, "(integer) 2"},
		{[]string{"SUNIONSTORE", "dst", "a", "b"}, "(integer) 5"},
		{[]string{"SCARD", "dst"}, "(integer) 5"},
		{[]string{"SDIFFSTORE", "dst", "a", "b"}, "(integer) 2"},
		{[]string{"SCARD", "dst"}, "(integer) 2"},
		// An empty result deletes the destination rather than storing an
		// empty set. Verified against Redis 7.
		{[]string{"SINTERSTORE", "dst", "a", "missing"}, "(integer) 0"},
		{[]string{"EXISTS", "dst"}, "(integer) 0"},

		{[]string{"SINTERCARD", "2", "a", "b"}, "(integer) 2"},
		{[]string{"SINTERCARD", "2", "a", "b", "LIMIT", "1"}, "(integer) 1"},
		{[]string{"SINTERCARD", "0", "a"}, "(error) ERR numkeys should be greater than 0"},
		{[]string{"SINTERCARD", "5", "a"}, "(error) ERR Number of keys can't be greater than number of args"},
	})

	// Source sets must be untouched by the operations above.
	if got := sortedReply(c.do("SMEMBERS", "a")); got != "[1 2 3 4]" {
		t.Errorf("set operations mutated their input: %s", got)
	}
}

func TestSetPopMoveAndScan(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	// Verified against Redis 7.
	runCmdTests(t, c, []cmdTest{
		{[]string{"SPOP", "missing"}, "(nil)"},
		{[]string{"SPOP", "missing", "2"}, "[]"},
		{[]string{"SRANDMEMBER", "missing"}, "(nil)"},
		{[]string{"SRANDMEMBER", "missing", "2"}, "[]"},
		{[]string{"SPOP", "missing", "-1"}, "(error) ERR value is out of range, must be positive"},
	})

	c.do("SADD", "s", "a", "b", "c", "d", "e")
	c.send("SPOP", "s", "2")
	if n := len(c.read().Array); n != 2 {
		t.Errorf("SPOP s 2: got %d members", n)
	}
	if got := c.do("SCARD", "s"); got != "(integer) 3" {
		t.Errorf("SPOP did not remove members: %s", got)
	}
	// Popping more than remain returns what is left and deletes the key.
	c.send("SPOP", "s", "10")
	if n := len(c.read().Array); n != 3 {
		t.Errorf("SPOP s 10: got %d members, want 3", n)
	}
	if got := c.do("EXISTS", "s"); got != "(integer) 0" {
		t.Errorf("drained set still exists: %s", got)
	}

	// SRANDMEMBER never removes anything.
	c.do("SADD", "r", "a", "b", "c")
	c.send("SRANDMEMBER", "r", "-5")
	if n := len(c.read().Array); n != 5 {
		t.Errorf("SRANDMEMBER r -5: got %d, want 5", n)
	}
	if got := c.do("SCARD", "r"); got != "(integer) 3" {
		t.Errorf("SRANDMEMBER removed members: %s", got)
	}

	runCmdTests(t, c, []cmdTest{
		{[]string{"SADD", "src", "x", "y"}, "(integer) 2"},
		{[]string{"SMOVE", "src", "dst", "x"}, "(integer) 1"},
		{[]string{"SISMEMBER", "src", "x"}, "(integer) 0"},
		{[]string{"SISMEMBER", "dst", "x"}, "(integer) 1"},
		{[]string{"SMOVE", "src", "dst", "nosuchmember"}, "(integer) 0"},
		{[]string{"SMOVE", "missing", "dst", "x"}, "(integer) 0"},
		// Emptying the source deletes it.
		{[]string{"SMOVE", "src", "dst", "y"}, "(integer) 1"},
		{[]string{"EXISTS", "src"}, "(integer) 0"},
	})

	// A full SSCAN covers everything exactly once.
	for i := 0; i < 20; i++ {
		c.do("SADD", "big", fmt.Sprintf("m%d", i))
	}
	seen := map[string]int{}
	cursor := "0"
	for i := 0; ; i++ {
		if i > 100 {
			t.Fatal("SSCAN did not terminate")
		}
		c.send("SSCAN", "big", cursor, "COUNT", "4")
		v := c.read()
		cursor = string(v.Array[0].Str)
		for _, m := range v.Array[1].Array {
			seen[string(m.Str)]++
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 20 {
		t.Errorf("SSCAN covered %d members, want 20", len(seen))
	}
	for m, n := range seen {
		if n != 1 {
			t.Errorf("SSCAN returned %q %d times", m, n)
		}
	}
}

// Each aggregate type must reject commands belonging to the others.
func TestCrossTypeErrors(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SET", "str", "v")
	c.do("RPUSH", "list", "v")
	c.do("HSET", "hash", "f", "v")
	c.do("SADD", "set", "m")

	wrong := [][]string{
		// Hash commands on other types.
		{"HGET", "str", "f"}, {"HGET", "list", "f"}, {"HGET", "set", "f"},
		{"HSET", "list", "f", "v"}, {"HDEL", "set", "f"}, {"HLEN", "str"},
		{"HGETALL", "list"}, {"HINCRBY", "set", "f", "1"}, {"HKEYS", "str"},
		// Set commands on other types.
		{"SADD", "hash", "m"}, {"SREM", "list", "m"}, {"SCARD", "str"},
		{"SMEMBERS", "hash"}, {"SISMEMBER", "list", "m"}, {"SPOP", "str"},
		{"SINTER", "set", "hash"}, {"SMOVE", "set", "str", "m"},
		{"SMOVE", "str", "set", "m"},
		// List and string commands on a hash or set.
		{"LPUSH", "hash", "v"}, {"LRANGE", "set", "0", "-1"},
		{"GET", "hash"}, {"INCR", "set"},
	}
	for _, cmd := range wrong {
		if got := c.do(cmd...); !strings.HasPrefix(got, "(error) WRONGTYPE") {
			t.Errorf("%s: got %s, want WRONGTYPE", strings.Join(cmd, " "), got)
		}
	}

	// TYPE must report each correctly.
	for key, want := range map[string]string{
		"str": "string", "list": "list", "hash": "hash", "set": "set",
	} {
		if got := c.do("TYPE", key); got != want {
			t.Errorf("TYPE %s: got %s, want %s", key, got, want)
		}
	}
}
