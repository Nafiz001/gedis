package server

import (
	"strings"
	"testing"
)

func TestListPushPopCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"LLEN", "missing"}, "(integer) 0"},
		{[]string{"LPOP", "missing"}, "(nil)"},
		{[]string{"RPOP", "missing"}, "(nil)"},
		{[]string{"LRANGE", "missing", "0", "-1"}, "[]"},

		{[]string{"RPUSH", "q", "b"}, "(integer) 1"},
		{[]string{"RPUSH", "q", "c", "d"}, "(integer) 3"},
		{[]string{"LPUSH", "q", "a"}, "(integer) 4"},
		{[]string{"LRANGE", "q", "0", "-1"}, "[a b c d]"},
		{[]string{"LLEN", "q"}, "(integer) 4"},

		// Multiple values to LPUSH go on one at a time, so they end up
		// reversed relative to the argument order.
		{[]string{"DEL", "r"}, "(integer) 0"},
		{[]string{"LPUSH", "r", "1", "2", "3"}, "(integer) 3"},
		{[]string{"LRANGE", "r", "0", "-1"}, "[3 2 1]"},

		{[]string{"LPOP", "q"}, "a"},
		{[]string{"RPOP", "q"}, "d"},
		{[]string{"LRANGE", "q", "0", "-1"}, "[b c]"},

		// The count form always replies with an array.
		{[]string{"LPOP", "q", "2"}, "[b c]"},
		{[]string{"EXISTS", "q"}, "(integer) 0"}, // emptied, so the key is gone
		{[]string{"LPOP", "q", "2"}, "(nil)"},
		{[]string{"LPOP", "r", "0"}, "[]"},
		{[]string{"LPOP", "r", "-1"}, "(error) ERR value is out of range, must be positive"},

		// PUSHX only appends to a list that already exists.
		{[]string{"LPUSHX", "nothere", "v"}, "(integer) 0"},
		{[]string{"RPUSHX", "nothere", "v"}, "(integer) 0"},
		{[]string{"EXISTS", "nothere"}, "(integer) 0"},
		{[]string{"RPUSHX", "r", "0"}, "(integer) 4"},
	})
}

// An aggregate key must disappear once its last element is removed.
func TestListEmptyKeyIsDeleted(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	for _, drain := range [][]string{
		{"LPOP", "k"},
		{"RPOP", "k"},
		{"LREM", "k", "0", "only"},
		{"LTRIM", "k", "5", "10"},
	} {
		c.do("DEL", "k")
		c.do("RPUSH", "k", "only")
		if got := c.do("EXISTS", "k"); got != "(integer) 1" {
			t.Fatalf("setup failed: %s", got)
		}
		c.do(drain...)
		if got := c.do("EXISTS", "k"); got != "(integer) 0" {
			t.Errorf("after %s: EXISTS is %s, want 0", strings.Join(drain, " "), got)
		}
		if got := c.do("TYPE", "k"); got != "none" {
			t.Errorf("after %s: TYPE is %s, want none", strings.Join(drain, " "), got)
		}
	}
}

func TestListIndexRangeCommands(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("RPUSH", "l", "a", "b", "c", "d", "e")

	runCmdTests(t, c, []cmdTest{
		{[]string{"LINDEX", "l", "0"}, "a"},
		{[]string{"LINDEX", "l", "4"}, "e"},
		{[]string{"LINDEX", "l", "-1"}, "e"},
		{[]string{"LINDEX", "l", "-5"}, "a"},
		{[]string{"LINDEX", "l", "5"}, "(nil)"},
		{[]string{"LINDEX", "l", "-6"}, "(nil)"},
		{[]string{"LINDEX", "missing", "0"}, "(nil)"},
		{[]string{"LINDEX", "l", "notanumber"}, "(error) ERR value is not an integer or out of range"},

		{[]string{"LRANGE", "l", "0", "-1"}, "[a b c d e]"},
		{[]string{"LRANGE", "l", "1", "3"}, "[b c d]"},
		{[]string{"LRANGE", "l", "-3", "-1"}, "[c d e]"},
		{[]string{"LRANGE", "l", "0", "100"}, "[a b c d e]"},
		{[]string{"LRANGE", "l", "3", "1"}, "[]"},
		{[]string{"LRANGE", "l", "10", "20"}, "[]"},

		{[]string{"LSET", "l", "0", "A"}, "OK"},
		{[]string{"LSET", "l", "-1", "E"}, "OK"},
		{[]string{"LRANGE", "l", "0", "-1"}, "[A b c d E]"},
		{[]string{"LSET", "l", "99", "x"}, "(error) ERR index out of range"},
		{[]string{"LSET", "missing", "0", "x"}, "(error) ERR no such key"},
	})
}

func TestListInsertRemoveTrim(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"RPUSH", "l", "a", "c"}, "(integer) 2"},
		{[]string{"LINSERT", "l", "BEFORE", "c", "b"}, "(integer) 3"},
		{[]string{"LRANGE", "l", "0", "-1"}, "[a b c]"},
		{[]string{"LINSERT", "l", "AFTER", "c", "d"}, "(integer) 4"},
		{[]string{"LRANGE", "l", "0", "-1"}, "[a b c d]"},
		{[]string{"LINSERT", "l", "BEFORE", "nosuchpivot", "x"}, "(integer) -1"},
		{[]string{"LINSERT", "missing", "BEFORE", "a", "x"}, "(integer) 0"},
		{[]string{"LINSERT", "l", "SIDEWAYS", "a", "x"}, "(error) ERR syntax error"},

		// LREM, with the counts verified against real Redis.
		{[]string{"DEL", "r"}, "(integer) 0"},
		{[]string{"RPUSH", "r", "a", "b", "a", "c", "a"}, "(integer) 5"},
		{[]string{"LREM", "r", "2", "a"}, "(integer) 2"},
		{[]string{"LRANGE", "r", "0", "-1"}, "[b c a]"},
		{[]string{"LREM", "r", "0", "a"}, "(integer) 1"},
		{[]string{"LRANGE", "r", "0", "-1"}, "[b c]"},
		{[]string{"LREM", "r", "0", "nothere"}, "(integer) 0"},
		{[]string{"LREM", "missing", "0", "a"}, "(integer) 0"},

		{[]string{"DEL", "t"}, "(integer) 0"},
		{[]string{"RPUSH", "t", "a", "b", "c", "d", "e"}, "(integer) 5"},
		{[]string{"LTRIM", "t", "1", "3"}, "OK"},
		{[]string{"LRANGE", "t", "0", "-1"}, "[b c d]"},
		{[]string{"LTRIM", "t", "0", "-1"}, "OK"},
		{[]string{"LRANGE", "t", "0", "-1"}, "[b c d]"},
		{[]string{"LTRIM", "missing", "0", "1"}, "OK"},
	})

	// LREM from the tail.
	c.do("DEL", "tail")
	c.do("RPUSH", "tail", "a", "b", "a", "c", "a")
	if got := c.do("LREM", "tail", "-2", "a"); got != "(integer) 2" {
		t.Errorf("LREM -2: got %s", got)
	}
	if got := c.do("LRANGE", "tail", "0", "-1"); got != "[a b c]" {
		t.Errorf("after LREM -2: got %s, want [a b c]", got)
	}
}

func TestListPos(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("RPUSH", "l", "a", "b", "c", "b", "d", "b")

	runCmdTests(t, c, []cmdTest{
		{[]string{"LPOS", "l", "b"}, "(integer) 1"},
		{[]string{"LPOS", "l", "nothere"}, "(nil)"},
		{[]string{"LPOS", "missing", "b"}, "(nil)"},
		{[]string{"LPOS", "l", "b", "RANK", "2"}, "(integer) 3"},
		{[]string{"LPOS", "l", "b", "RANK", "-1"}, "(integer) 5"},
		{[]string{"LPOS", "l", "b", "COUNT", "0"}, "[(integer) 1 (integer) 3 (integer) 5]"},
		{[]string{"LPOS", "l", "b", "COUNT", "2"}, "[(integer) 1 (integer) 3]"},
		{[]string{"LPOS", "l", "b", "RANK", "2", "COUNT", "0"}, "[(integer) 3 (integer) 5]"},
		{[]string{"LPOS", "l", "b", "COUNT", "0", "MAXLEN", "2"}, "[(integer) 1]"},
		{[]string{"LPOS", "l", "b", "RANK", "0"}, "(error) ERR RANK can't be zero. Use 1 to start searching from the first matching element in the head of the list or a negative rank to start searching from the tail. A RANK of zero is invalid."},
		{[]string{"LPOS", "l", "b", "COUNT", "-1"}, "(error) ERR COUNT can't be negative"},
	})
}

func TestListMove(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"RPUSH", "src", "a", "b", "c"}, "(integer) 3"},
		{[]string{"RPOPLPUSH", "src", "dst"}, "c"},
		{[]string{"LRANGE", "src", "0", "-1"}, "[a b]"},
		{[]string{"LRANGE", "dst", "0", "-1"}, "[c]"},

		{[]string{"LMOVE", "src", "dst", "LEFT", "RIGHT"}, "a"},
		{[]string{"LRANGE", "src", "0", "-1"}, "[b]"},
		{[]string{"LRANGE", "dst", "0", "-1"}, "[c a]"},

		// Draining the source must delete its key.
		{[]string{"LMOVE", "src", "dst", "LEFT", "LEFT"}, "b"},
		{[]string{"EXISTS", "src"}, "(integer) 0"},
		{[]string{"LRANGE", "dst", "0", "-1"}, "[b c a]"},

		{[]string{"LMOVE", "missing", "dst", "LEFT", "LEFT"}, "(nil)"},
		{[]string{"RPOPLPUSH", "missing", "dst"}, "(nil)"},
		{[]string{"LMOVE", "dst", "other", "SIDEWAYS", "LEFT"}, "(error) ERR syntax error"},
	})

	// Rotating a list onto itself must not lose or duplicate the element.
	c.do("DEL", "rot")
	c.do("RPUSH", "rot", "1", "2", "3")
	if got := c.do("RPOPLPUSH", "rot", "rot"); got != "3" {
		t.Errorf("self-rotate returned %s", got)
	}
	if got := c.do("LRANGE", "rot", "0", "-1"); got != "[3 1 2]" {
		t.Errorf("self-rotate: got %s, want [3 1 2]", got)
	}
	if got := c.do("LLEN", "rot"); got != "(integer) 3" {
		t.Errorf("self-rotate changed the length: %s", got)
	}
}

// Every list command must reject a key holding a string, and the string
// commands must reject a list.
func TestListWrongType(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SET", "str", "v")
	c.do("RPUSH", "list", "v")

	listCommandsOnAString := [][]string{
		{"LPUSH", "str", "v"},
		{"RPUSH", "str", "v"},
		{"LPUSHX", "str", "v"},
		{"LPOP", "str"},
		{"RPOP", "str"},
		{"LLEN", "str"},
		{"LRANGE", "str", "0", "-1"},
		{"LINDEX", "str", "0"},
		{"LSET", "str", "0", "v"},
		{"LINSERT", "str", "BEFORE", "a", "b"},
		{"LREM", "str", "0", "v"},
		{"LTRIM", "str", "0", "1"},
		{"LPOS", "str", "v"},
		{"LMOVE", "str", "list", "LEFT", "LEFT"},
		{"LMOVE", "list", "str", "LEFT", "LEFT"},
	}
	for _, cmd := range listCommandsOnAString {
		if got := c.do(cmd...); !strings.HasPrefix(got, "(error) WRONGTYPE") {
			t.Errorf("%s: got %s, want WRONGTYPE", strings.Join(cmd, " "), got)
		}
	}

	stringCommandsOnAList := [][]string{
		{"GET", "list"},
		{"APPEND", "list", "x"},
		{"INCR", "list"},
		{"STRLEN", "list"},
		{"GETRANGE", "list", "0", "-1"},
	}
	for _, cmd := range stringCommandsOnAList {
		if got := c.do(cmd...); !strings.HasPrefix(got, "(error) WRONGTYPE") {
			t.Errorf("%s: got %s, want WRONGTYPE", strings.Join(cmd, " "), got)
		}
	}

	// A failed WRONGTYPE must not have modified anything.
	if got := c.do("GET", "str"); got != "v" {
		t.Errorf("string was modified: %s", got)
	}
	if got := c.do("LRANGE", "list", "0", "-1"); got != "[v]" {
		t.Errorf("list was modified: %s", got)
	}
}

func TestListTypeAndExpiry(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	c.do("RPUSH", "l", "a")
	if got := c.do("TYPE", "l"); got != "list" {
		t.Errorf("TYPE: got %s, want list", got)
	}

	// A list obeys TTLs like any other key.
	c.do("EXPIRE", "l", "100")
	if got := c.do("TTL", "l"); got != "(integer) 100" {
		t.Errorf("TTL: got %s", got)
	}
	// Pushing must not clear the deadline.
	c.do("RPUSH", "l", "b")
	if got := c.do("TTL", "l"); got != "(integer) 100" {
		t.Errorf("TTL after RPUSH: got %s, want 100", got)
	}

	h.clk.advance(100_001)
	if got := c.do("LLEN", "l"); got != "(integer) 0" {
		t.Errorf("after expiry: got %s", got)
	}
	if got := c.do("EXISTS", "l"); got != "(integer) 0" {
		t.Errorf("EXISTS after expiry: got %s", got)
	}
}
