package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestMultiExec(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	runCmdTests(t, c, []cmdTest{
		{[]string{"EXEC"}, "(error) ERR EXEC without MULTI"},
		{[]string{"DISCARD"}, "(error) ERR DISCARD without MULTI"},

		{[]string{"MULTI"}, "OK"},
		{[]string{"SET", "a", "1"}, "QUEUED"},
		{[]string{"INCR", "a"}, "QUEUED"},
		{[]string{"GET", "a"}, "QUEUED"},
		{[]string{"MULTI"}, "(error) ERR MULTI calls can not be nested"},
	})

	// Nothing has been applied yet: the queue is still a queue.
	if got := c.do("EXEC"); got != "[OK (integer) 2 2]" {
		t.Errorf("EXEC: got %s", got)
	}
	if got := c.do("GET", "a"); got != "2" {
		t.Errorf("after EXEC: got %s", got)
	}
}

func TestMultiQueuesRatherThanRuns(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	other := h.dial(t)

	c.do("MULTI")
	c.do("SET", "pending", "value")

	// Another client must not see the queued write before EXEC.
	if got := other.do("EXISTS", "pending"); got != "(integer) 0" {
		t.Errorf("a queued write was visible before EXEC: %s", got)
	}

	c.do("EXEC")
	if got := other.do("GET", "pending"); got != "value" {
		t.Errorf("after EXEC: got %s", got)
	}
}

func TestDiscard(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	c.do("SET", "k", "original")
	c.do("MULTI")
	c.do("SET", "k", "changed")
	c.do("DEL", "k")

	if got := c.do("DISCARD"); got != "OK" {
		t.Errorf("DISCARD: %s", got)
	}
	if got := c.do("GET", "k"); got != "original" {
		t.Errorf("DISCARD applied the queue: got %s", got)
	}
	// The transaction is over, so EXEC is an error again.
	if got := c.do("EXEC"); got != "(error) ERR EXEC without MULTI" {
		t.Errorf("EXEC after DISCARD: %s", got)
	}
}

// A command that cannot even be queued poisons the transaction, because
// running the rest would apply a partial version of what was asked for.
func TestExecAbortsOnQueueError(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	c.do("MULTI")
	c.do("SET", "a", "1")
	if got := c.do("NOSUCHCOMMAND"); !strings.HasPrefix(got, "(error) ERR unknown command") {
		t.Errorf("queueing an unknown command: %s", got)
	}
	if got := c.do("GET"); got != "(error) ERR wrong number of arguments for 'get' command" {
		t.Errorf("queueing a bad arity: %s", got)
	}

	if got := c.do("EXEC"); got != "(error) EXECABORT Transaction discarded because of previous errors." {
		t.Errorf("EXEC after a queue error: %s", got)
	}
	if got := c.do("EXISTS", "a"); got != "(integer) 0" {
		t.Errorf("an aborted transaction still applied a command: %s", got)
	}
}

// An error at run time is reported in place and does not stop the rest: by
// then the client has already been told each command was accepted.
func TestExecRuntimeErrorsDoNotAbort(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SET", "str", "notanumber")

	c.do("MULTI")
	c.do("SET", "before", "1")
	c.do("INCR", "str") // fails at run time
	c.do("SET", "after", "1")

	c.send("EXEC")
	results := c.read()
	if len(results.Array) != 3 {
		t.Fatalf("EXEC returned %d results, want 3", len(results.Array))
	}
	if results.Array[1].Type != 0x2D { // '-' error
		t.Errorf("the failing command should have reported an error: %s", results.Array[1].String())
	}
	// Both surrounding commands must have applied.
	if got := c.do("GET", "before"); got != "1" {
		t.Errorf("command before the error: %s", got)
	}
	if got := c.do("GET", "after"); got != "1" {
		t.Errorf("command after the error: %s", got)
	}
}

func TestWatchAbortsOnChange(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	other := h.dial(t)

	c.do("SET", "watched", "1")
	c.do("WATCH", "watched")

	// Another client moves the key out from under the transaction.
	other.do("SET", "watched", "2")

	c.do("MULTI")
	c.do("SET", "result", "should-not-apply")
	// A null array means "aborted, retry", which is how a client tells that
	// apart from "ran and produced nothing".
	if got := c.do("EXEC"); got != "(nil)" {
		t.Errorf("EXEC after a watched key changed: got %s, want a null array", got)
	}
	if got := c.do("EXISTS", "result"); got != "(integer) 0" {
		t.Errorf("an aborted transaction applied its queue: %s", got)
	}
}

func TestWatchSucceedsWhenUntouched(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	other := h.dial(t)

	c.do("SET", "watched", "1")
	c.do("WATCH", "watched")
	// A write to an unrelated key must not abort anything.
	other.do("SET", "unrelated", "changed")

	c.do("MULTI")
	c.do("INCR", "watched")
	if got := c.do("EXEC"); got != "[(integer) 2]" {
		t.Errorf("EXEC: got %s", got)
	}
}

// Deleting or expiring a watched key counts as changing it.
func TestWatchNoticesEveryKindOfChange(t *testing.T) {
	h := startServer(t, nil)

	for _, change := range [][]string{
		{"SET", "w", "other"},
		{"DEL", "w"},
		{"EXPIRE", "w", "100"},
		{"APPEND", "w", "x"},
		{"RENAME", "w", "renamed"},
	} {
		t.Run(strings.Join(change, "_"), func(t *testing.T) {
			c := h.dial(t)
			other := h.dial(t)
			c.do("FLUSHALL")
			c.do("SET", "w", "v")

			c.do("WATCH", "w")
			other.do(change...)

			c.do("MULTI")
			c.do("SET", "applied", "yes")
			if got := c.do("EXEC"); got != "(nil)" {
				t.Errorf("EXEC after %s: got %s, want an abort", strings.Join(change, " "), got)
			}
		})
	}
}

// FLUSHDB removes keys without going through the per-key write path, so
// watchers have to be invalidated explicitly.
func TestWatchNoticesFlush(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	other := h.dial(t)

	c.do("SET", "w", "v")
	c.do("WATCH", "w")
	other.do("FLUSHDB")

	c.do("MULTI")
	c.do("SET", "applied", "yes")
	if got := c.do("EXEC"); got != "(nil)" {
		t.Errorf("EXEC after FLUSHDB: got %s, want an abort", got)
	}
}

func TestUnwatch(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	other := h.dial(t)

	c.do("SET", "w", "1")
	c.do("WATCH", "w")
	other.do("SET", "w", "2")

	// UNWATCH clears both the watches and the dirty flag they set.
	if got := c.do("UNWATCH"); got != "OK" {
		t.Errorf("UNWATCH: %s", got)
	}
	c.do("MULTI")
	c.do("SET", "applied", "yes")
	if got := c.do("EXEC"); got != "[OK]" {
		t.Errorf("EXEC after UNWATCH: got %s, want success", got)
	}

	// EXEC also releases watches, so the next transaction starts clean.
	c.do("WATCH", "w")
	c.do("MULTI")
	if got := c.do("EXEC"); got != "[]" {
		t.Errorf("empty EXEC: %s", got)
	}
	other.do("SET", "w", "3")
	c.do("MULTI")
	c.do("SET", "second", "yes")
	if got := c.do("EXEC"); got != "[OK]" {
		t.Errorf("EXEC did not release the previous watch: %s", got)
	}
}

func TestWatchInsideMultiRejected(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)

	c.do("MULTI")
	if got := c.do("WATCH", "k"); got != "(error) ERR WATCH inside MULTI is not allowed" {
		t.Errorf("WATCH inside MULTI: %s", got)
	}
}

// EXEC must run with the keyspace to itself. Without that, a concurrent client
// could interleave and the transaction would not be one.
func TestExecIsAtomic(t *testing.T) {
	h := startServer(t, nil)

	const (
		writers = 8
		rounds  = 50
	)
	c := h.dial(t)
	c.do("SET", "a", "0")
	c.do("SET", "b", "0")

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := h.dial(t)
			for i := 0; i < rounds; i++ {
				// Two counters incremented together: any observer must never
				// catch them disagreeing.
				conn.do("MULTI")
				conn.do("INCR", "a")
				conn.do("INCR", "b")
				conn.do("EXEC")
			}
		}()
	}

	// While that runs, keep checking the two counters agree.
	wg.Add(1)
	go func() {
		defer wg.Done()
		observer := h.dial(t)
		for i := 0; i < 200; i++ {
			observer.do("MULTI")
			observer.do("GET", "a")
			observer.do("GET", "b")
			got := observer.do("EXEC")
			parts := strings.Fields(strings.Trim(got, "[]"))
			if len(parts) == 2 && parts[0] != parts[1] {
				t.Errorf("observed a torn transaction: a=%s b=%s", parts[0], parts[1])
				return
			}
		}
	}()

	wg.Wait()

	want := fmt.Sprintf("%d", writers*rounds)
	if got := c.do("GET", "a"); got != want {
		t.Errorf("counter a: got %s, want %s", got, want)
	}
	if got := c.do("GET", "b"); got != want {
		t.Errorf("counter b: got %s, want %s", got, want)
	}
}

// The classic optimistic-locking loop: WATCH, read, compute, EXEC, retry on
// abort. It has to converge with several clients competing.
func TestWatchRetryLoopConverges(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SET", "balance", "0")

	const (
		clients    = 4
		perClient  = 25
		increment  = 3
		maxRetries = 200
	)

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := h.dial(t)
			for j := 0; j < perClient; j++ {
				for attempt := 0; attempt < maxRetries; attempt++ {
					conn.do("WATCH", "balance")
					current := conn.do("GET", "balance")
					var value int
					fmt.Sscanf(current, "%d", &value)

					conn.do("MULTI")
					conn.do("SET", "balance", fmt.Sprint(value+increment))
					if conn.do("EXEC") != "(nil)" {
						break // committed
					}
					// Aborted: someone else got there first, so read again.
				}
			}
		}()
	}
	wg.Wait()

	want := fmt.Sprintf("%d", clients*perClient*increment)
	if got := c.do("GET", "balance"); got != want {
		t.Errorf("balance: got %s, want %s -- an update was lost", got, want)
	}
}
