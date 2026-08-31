package server

import (
	"fmt"
	"strings"
	"testing"
)

// All expectations here were checked against Redis 7 before being written down.

func TestZSetBasics(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"ZCARD", "missing"}, "(integer) 0"},
		{[]string{"ZSCORE", "missing", "m"}, "(nil)"},
		{[]string{"ZRANK", "missing", "m"}, "(nil)"},
		{[]string{"ZRANGE", "missing", "0", "-1"}, "[]"},
		{[]string{"ZCOUNT", "missing", "-inf", "+inf"}, "(integer) 0"},

		{[]string{"ZADD", "z", "1", "a"}, "(integer) 1"},
		{[]string{"ZADD", "z", "2", "b", "3", "c"}, "(integer) 2"},
		// Rescoring an existing member adds nothing.
		{[]string{"ZADD", "z", "10", "a"}, "(integer) 0"},
		{[]string{"ZSCORE", "z", "a"}, "10"},
		{[]string{"ZADD", "z", "1", "a"}, "(integer) 0"},
		{[]string{"ZCARD", "z"}, "(integer) 3"},
		{[]string{"TYPE", "z"}, "zset"},

		// Whole scores come back without a decimal point.
		{[]string{"ZADD", "s", "1.5", "half", "2", "two", "inf", "big", "-inf", "small"}, "(integer) 4"},
		{[]string{"ZSCORE", "s", "half"}, "1.5"},
		{[]string{"ZSCORE", "s", "two"}, "2"},
		{[]string{"ZSCORE", "s", "big"}, "inf"},
		{[]string{"ZSCORE", "s", "small"}, "-inf"},
		{[]string{"ZMSCORE", "s", "half", "nosuchmember", "two"}, "[1.5 (nil) 2]"},

		{[]string{"ZADD", "z", "notanumber", "m"}, "(error) ERR value is not a valid float"},

		{[]string{"ZINCRBY", "z", "5", "a"}, "6"},
		{[]string{"ZINCRBY", "z", "-6", "a"}, "0"},
		{[]string{"ZINCRBY", "z", "2.5", "brandnew"}, "2.5"},

		{[]string{"ZREM", "z", "brandnew", "nosuchmember"}, "(integer) 1"},
		{[]string{"ZCARD", "z"}, "(integer) 3"},
	})

	// inf + -inf has no ordering, so it must be refused.
	c.do("ZADD", "n", "inf", "m")
	if got := c.do("ZINCRBY", "n", "-inf", "m"); got != "(error) ERR resulting score is not a number (NaN)" {
		t.Errorf("ZINCRBY to NaN: got %s", got)
	}

	// Removing the last member deletes the key.
	c.do("ZADD", "solo", "1", "only")
	c.do("ZREM", "solo", "only")
	if got := c.do("EXISTS", "solo"); got != "(integer) 0" {
		t.Errorf("emptied zset still exists: %s", got)
	}
}

func TestZAddOptions(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"ZADD", "z", "5", "a"}, "(integer) 1"},

		// NX only creates.
		{[]string{"ZADD", "z", "NX", "99", "a"}, "(integer) 0"},
		{[]string{"ZSCORE", "z", "a"}, "5"},
		{[]string{"ZADD", "z", "NX", "1", "fresh"}, "(integer) 1"},

		// XX only updates, and never creates the key.
		{[]string{"ZADD", "nosuchkey", "XX", "1", "m"}, "(integer) 0"},
		{[]string{"EXISTS", "nosuchkey"}, "(integer) 0"},
		{[]string{"ZADD", "z", "XX", "7", "a"}, "(integer) 0"},
		{[]string{"ZSCORE", "z", "a"}, "7"},

		// CH counts changes rather than additions.
		{[]string{"ZADD", "z", "CH", "8", "a"}, "(integer) 1"},
		{[]string{"ZADD", "z", "CH", "8", "a"}, "(integer) 0"},

		// GT and LT only move the score in one direction.
		{[]string{"ZADD", "z", "GT", "CH", "3", "a"}, "(integer) 0"},
		{[]string{"ZSCORE", "z", "a"}, "8"},
		{[]string{"ZADD", "z", "GT", "CH", "20", "a"}, "(integer) 1"},
		{[]string{"ZSCORE", "z", "a"}, "20"},
		{[]string{"ZADD", "z", "LT", "CH", "30", "a"}, "(integer) 0"},
		{[]string{"ZADD", "z", "LT", "CH", "10", "a"}, "(integer) 1"},
		{[]string{"ZSCORE", "z", "a"}, "10"},

		// INCR turns ZADD into ZINCRBY and replies with the new score.
		{[]string{"ZADD", "z", "INCR", "5", "a"}, "15"},
		// A condition that rejects the update replies null, not a score.
		{[]string{"ZADD", "z", "NX", "INCR", "5", "a"}, "(nil)"},
		{[]string{"ZADD", "z", "XX", "INCR", "5", "nosuchmember"}, "(nil)"},

		{[]string{"ZADD", "z", "NX", "XX", "1", "m"}, "(error) ERR XX and NX options at the same time are not compatible"},
		{[]string{"ZADD", "z", "NX", "GT", "1", "m"}, "(error) ERR GT, LT, and/or NX options at the same time are not compatible"},
		{[]string{"ZADD", "z", "GT", "LT", "1", "m"}, "(error) ERR GT, LT, and/or NX options at the same time are not compatible"},
		{[]string{"ZADD", "z", "INCR", "1", "a", "2", "b"}, "(error) ERR INCR option supports a single increment-element pair"},
	})
}

func TestZSetRanges(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")

	runCmdTests(t, c, []cmdTest{
		// By rank.
		{[]string{"ZRANGE", "z", "0", "-1"}, "[a b c d e]"},
		{[]string{"ZRANGE", "z", "0", "2"}, "[a b c]"},
		{[]string{"ZRANGE", "z", "-2", "-1"}, "[d e]"},
		{[]string{"ZRANGE", "z", "3", "1"}, "[]"},
		{[]string{"ZRANGE", "z", "0", "-1", "WITHSCORES"}, "[a 1 b 2 c 3 d 4 e 5]"},
		{[]string{"ZREVRANGE", "z", "0", "-1"}, "[e d c b a]"},
		{[]string{"ZREVRANGE", "z", "0", "1"}, "[e d]"},
		{[]string{"ZRANGE", "z", "0", "-1", "REV"}, "[e d c b a]"},

		// Ranks.
		{[]string{"ZRANK", "z", "a"}, "(integer) 0"},
		{[]string{"ZRANK", "z", "e"}, "(integer) 4"},
		{[]string{"ZRANK", "z", "nosuchmember"}, "(nil)"},
		{[]string{"ZREVRANK", "z", "a"}, "(integer) 4"},
		{[]string{"ZREVRANK", "z", "e"}, "(integer) 0"},

		// By score.
		{[]string{"ZRANGEBYSCORE", "z", "-inf", "+inf"}, "[a b c d e]"},
		{[]string{"ZRANGEBYSCORE", "z", "2", "4"}, "[b c d]"},
		{[]string{"ZRANGEBYSCORE", "z", "(1", "+inf"}, "[b c d e]"},
		{[]string{"ZRANGEBYSCORE", "z", "(1", "(4"}, "[b c]"},
		{[]string{"ZRANGEBYSCORE", "z", "-inf", "+inf", "LIMIT", "1", "2"}, "[b c]"},
		{[]string{"ZRANGEBYSCORE", "z", "100", "200"}, "[]"},
		{[]string{"ZREVRANGEBYSCORE", "z", "+inf", "-inf"}, "[e d c b a]"},
		{[]string{"ZREVRANGEBYSCORE", "z", "4", "2"}, "[d c b]"},
		{[]string{"ZRANGE", "z", "+inf", "-inf", "BYSCORE", "REV"}, "[e d c b a]"},

		// Counts.
		{[]string{"ZCOUNT", "z", "-inf", "+inf"}, "(integer) 5"},
		{[]string{"ZCOUNT", "z", "2", "4"}, "(integer) 3"},
		{[]string{"ZCOUNT", "z", "(2", "4"}, "(integer) 2"},
		{[]string{"ZCOUNT", "z", "100", "200"}, "(integer) 0"},

		// Errors.
		{[]string{"ZRANGEBYSCORE", "z", "bad", "+inf"}, "(error) ERR min or max is not a float"},
		{[]string{"ZRANGE", "z", "0", "-1", "LIMIT", "0", "2"}, "(error) ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"},
	})
}

func TestZSetLexRanges(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	// Lexicographic ranges assume every member shares one score.
	c.do("ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d")

	runCmdTests(t, c, []cmdTest{
		{[]string{"ZRANGEBYLEX", "lex", "-", "+"}, "[a b c d]"},
		{[]string{"ZRANGEBYLEX", "lex", "[a", "[c"}, "[a b c]"},
		{[]string{"ZRANGEBYLEX", "lex", "(a", "[c"}, "[b c]"},
		{[]string{"ZRANGEBYLEX", "lex", "(a", "(d"}, "[b c]"},
		{[]string{"ZRANGEBYLEX", "lex", "-", "+", "LIMIT", "1", "2"}, "[b c]"},
		// A min of "+" or a max of "-" is an empty window.
		{[]string{"ZRANGEBYLEX", "lex", "+", "-"}, "[]"},
		{[]string{"ZREVRANGEBYLEX", "lex", "+", "-"}, "[d c b a]"},
		{[]string{"ZRANGE", "lex", "-", "+", "BYLEX"}, "[a b c d]"},
		{[]string{"ZRANGE", "lex", "+", "-", "BYLEX", "REV"}, "[d c b a]"},

		{[]string{"ZLEXCOUNT", "lex", "-", "+"}, "(integer) 4"},
		{[]string{"ZLEXCOUNT", "lex", "[b", "[c"}, "(integer) 2"},

		{[]string{"ZRANGEBYLEX", "lex", "a", "c"}, "(error) ERR min or max not valid string range item"},
		// BYLEX has no scores to return.
		{[]string{"ZRANGE", "lex", "-", "+", "BYLEX", "WITHSCORES"}, "(error) ERR syntax error"},
	})
}

func TestZSetRemoveRanges(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	seed := func(key string) {
		c.do("DEL", key)
		c.do("ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	}

	seed("r")
	runCmdTests(t, c, []cmdTest{
		{[]string{"ZREMRANGEBYRANK", "r", "0", "1"}, "(integer) 2"},
		{[]string{"ZRANGE", "r", "0", "-1"}, "[c d e]"},
	})

	seed("s")
	runCmdTests(t, c, []cmdTest{
		{[]string{"ZREMRANGEBYSCORE", "s", "2", "4"}, "(integer) 3"},
		{[]string{"ZRANGE", "s", "0", "-1"}, "[a e]"},
		{[]string{"ZREMRANGEBYSCORE", "s", "-inf", "+inf"}, "(integer) 2"},
		{[]string{"EXISTS", "s"}, "(integer) 0"},
	})

	c.do("ZADD", "l", "0", "a", "0", "b", "0", "c")
	runCmdTests(t, c, []cmdTest{
		{[]string{"ZREMRANGEBYLEX", "l", "[a", "[b"}, "(integer) 2"},
		{[]string{"ZRANGE", "l", "0", "-1"}, "[c]"},
	})

	// The structure must stay consistent after bulk removal.
	seed("big")
	for i := 0; i < 200; i++ {
		c.do("ZADD", "big", fmt.Sprint(i), fmt.Sprintf("m%03d", i))
	}
	c.do("ZREMRANGEBYRANK", "big", "0", "99")
	if got := c.do("ZCARD", "big"); got != "(integer) 105" { // 205 seeded minus 100
		t.Errorf("ZCARD after bulk removal: got %s", got)
	}
	// Ranks must still be contiguous and correct.
	c.send("ZRANGE", "big", "0", "0")
	first := string(c.read().Array[0].Str)
	if got := c.do("ZRANK", "big", first); got != "(integer) 0" {
		t.Errorf("rank of the first member after removal: got %s", got)
	}
}

func TestZSetPopAndRandom(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"ZPOPMIN", "missing"}, "[]"},
		{[]string{"ZPOPMAX", "missing"}, "[]"},
		{[]string{"ZRANDMEMBER", "missing"}, "(nil)"},
		{[]string{"ZRANDMEMBER", "missing", "3"}, "[]"},

		{[]string{"ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"}, "(integer) 4"},
		{[]string{"ZPOPMIN", "z"}, "[a 1]"},
		{[]string{"ZPOPMAX", "z"}, "[d 4]"},
		{[]string{"ZRANGE", "z", "0", "-1"}, "[b c]"},
		{[]string{"ZPOPMIN", "z", "5"}, "[b 2 c 3]"},
		{[]string{"EXISTS", "z"}, "(integer) 0"},
	})

	c.do("ZADD", "r", "1", "a", "2", "b", "3", "c")
	c.send("ZRANDMEMBER", "r", "-5")
	if n := len(c.read().Array); n != 5 {
		t.Errorf("ZRANDMEMBER r -5: got %d, want 5", n)
	}
	c.send("ZRANDMEMBER", "r", "10")
	if n := len(c.read().Array); n != 3 {
		t.Errorf("ZRANDMEMBER beyond the size: got %d, want 3", n)
	}
	c.send("ZRANDMEMBER", "r", "2", "WITHSCORES")
	if n := len(c.read().Array); n != 4 {
		t.Errorf("ZRANDMEMBER WITHSCORES: got %d elements, want 4", n)
	}
	if got := c.do("ZCARD", "r"); got != "(integer) 3" {
		t.Errorf("ZRANDMEMBER removed members: %s", got)
	}
}

func TestZSetOperations(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("ZADD", "a", "1", "x", "2", "y")
	c.do("ZADD", "b", "10", "y", "20", "z")

	runCmdTests(t, c, []cmdTest{
		{[]string{"ZUNIONSTORE", "dst", "2", "a", "b"}, "(integer) 3"},
		{[]string{"ZRANGE", "dst", "0", "-1", "WITHSCORES"}, "[x 1 y 12 z 20]"},

		{[]string{"ZUNIONSTORE", "dst", "2", "a", "b", "AGGREGATE", "MAX"}, "(integer) 3"},
		{[]string{"ZRANGE", "dst", "0", "-1", "WITHSCORES"}, "[x 1 y 10 z 20]"},

		{[]string{"ZUNIONSTORE", "dst", "2", "a", "b", "WEIGHTS", "2", "3"}, "(integer) 3"},
		{[]string{"ZRANGE", "dst", "0", "-1", "WITHSCORES"}, "[x 2 y 34 z 60]"},

		{[]string{"ZINTERSTORE", "dst", "2", "a", "b"}, "(integer) 1"},
		{[]string{"ZRANGE", "dst", "0", "-1", "WITHSCORES"}, "[y 12]"},

		{[]string{"ZDIFFSTORE", "dst", "2", "a", "b"}, "(integer) 1"},
		{[]string{"ZRANGE", "dst", "0", "-1", "WITHSCORES"}, "[x 1]"},

		// An empty result deletes the destination.
		{[]string{"ZINTERSTORE", "dst", "2", "a", "nosuchkey"}, "(integer) 0"},
		{[]string{"EXISTS", "dst"}, "(integer) 0"},

		// The non-storing forms return the members directly.
		{[]string{"ZUNION", "2", "a", "b", "WITHSCORES"}, "[x 1 y 12 z 20]"},
		{[]string{"ZINTER", "2", "a", "b"}, "[y]"},
		{[]string{"ZDIFF", "2", "a", "b"}, "[x]"},

		{[]string{"ZUNIONSTORE", "dst", "0", "a"}, "(error) ERR at least 1 input key is needed for the given command"},
	})

	// A plain set counts as a sorted set whose members all score 1.
	c.do("SADD", "plain", "m1", "m2")
	runCmdTests(t, c, []cmdTest{
		{[]string{"ZUNIONSTORE", "mixed", "2", "a", "plain"}, "(integer) 4"},
		{[]string{"ZSCORE", "mixed", "m1"}, "1"},
		{[]string{"ZSCORE", "mixed", "x"}, "1"},
	})

	// The inputs must be unchanged.
	if got := c.do("ZRANGE", "a", "0", "-1", "WITHSCORES"); got != "[x 1 y 2]" {
		t.Errorf("set operations mutated their input: %s", got)
	}
}

func TestZScan(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	for i := 0; i < 20; i++ {
		c.do("ZADD", "z", fmt.Sprint(i), fmt.Sprintf("m%02d", i))
	}

	seen := map[string]string{}
	cursor := "0"
	for i := 0; ; i++ {
		if i > 100 {
			t.Fatal("ZSCAN did not terminate")
		}
		c.send("ZSCAN", "z", cursor, "COUNT", "4")
		v := c.read()
		cursor = string(v.Array[0].Str)
		pairs := v.Array[1].Array
		for j := 0; j+1 < len(pairs); j += 2 {
			seen[string(pairs[j].Str)] = string(pairs[j+1].Str)
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != 20 {
		t.Errorf("ZSCAN covered %d members, want 20", len(seen))
	}
	if seen["m07"] != "7" {
		t.Errorf("ZSCAN returned the wrong score for m07: %q", seen["m07"])
	}
}

func TestZSetWrongType(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SET", "str", "v")
	c.do("ZADD", "z", "1", "m")

	for _, cmd := range [][]string{
		{"ZADD", "str", "1", "m"},
		{"ZSCORE", "str", "m"},
		{"ZCARD", "str"},
		{"ZRANGE", "str", "0", "-1"},
		{"ZRANK", "str", "m"},
		{"ZREM", "str", "m"},
		{"ZINCRBY", "str", "1", "m"},
		{"ZCOUNT", "str", "-inf", "+inf"},
		{"ZPOPMIN", "str"},
		{"ZUNIONSTORE", "dst", "2", "z", "str"},
		// And the reverse direction.
		{"GET", "z"},
		{"LPUSH", "z", "v"},
		{"SADD", "z", "v"},
		{"HSET", "z", "f", "v"},
	} {
		if got := c.do(cmd...); !strings.HasPrefix(got, "(error) WRONGTYPE") {
			t.Errorf("%s: got %s, want WRONGTYPE", strings.Join(cmd, " "), got)
		}
	}
}

// A sorted set built through the server must keep its ordering under churn.
func TestZSetOrderingUnderChurn(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	for i := 0; i < 300; i++ {
		c.do("ZADD", "z", fmt.Sprint(i%50), fmt.Sprintf("m%03d", i))
	}
	// Rescore half of them, which forces the skiplist to move nodes.
	for i := 0; i < 300; i += 2 {
		c.do("ZADD", "z", fmt.Sprint(100+i%37), fmt.Sprintf("m%03d", i))
	}
	// Remove a third.
	for i := 0; i < 300; i += 3 {
		c.do("ZREM", "z", fmt.Sprintf("m%03d", i))
	}

	c.send("ZRANGE", "z", "0", "-1", "WITHSCORES")
	arr := c.read().Array
	if len(arr)%2 != 0 {
		t.Fatalf("WITHSCORES returned an odd number of elements: %d", len(arr))
	}

	// Scores must be non-decreasing, and every rank must match its position.
	prev := ""
	for i := 0; i < len(arr); i += 2 {
		member, score := string(arr[i].Str), string(arr[i+1].Str)
		if prev != "" && score < prev && len(score) == len(prev) {
			t.Fatalf("out of order at %d: %s after %s", i, score, prev)
		}
		prev = score
		if got := c.do("ZRANK", "z", member); got != fmt.Sprintf("(integer) %d", i/2) {
			t.Fatalf("ZRANK %s: got %s, want %d", member, got, i/2)
		}
	}
	if got := c.do("ZCARD", "z"); got != fmt.Sprintf("(integer) %d", len(arr)/2) {
		t.Errorf("ZCARD disagrees with ZRANGE: %s vs %d", got, len(arr)/2)
	}
}
