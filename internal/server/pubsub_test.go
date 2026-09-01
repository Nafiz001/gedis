package server

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// expectPush reads one pub/sub frame and renders it for comparison.
func (c *conn) expectPush(t *testing.T) string {
	t.Helper()
	return c.read().String()
}

func TestSubscribeAndPublish(t *testing.T) {
	h := startServer(t, nil)
	sub := h.dial(t)
	pub := h.dial(t)

	// The confirmation carries the running subscription count.
	if got := sub.do("SUBSCRIBE", "news"); got != "[subscribe news (integer) 1]" {
		t.Fatalf("SUBSCRIBE: %s", got)
	}

	if got := pub.do("PUBLISH", "news", "hello"); got != "(integer) 1" {
		t.Errorf("PUBLISH: got %s, want 1 subscriber", got)
	}
	if got := sub.expectPush(t); got != "[message news hello]" {
		t.Errorf("delivery: got %s", got)
	}

	// Publishing to a channel nobody listens on reaches nobody.
	if got := pub.do("PUBLISH", "quiet", "nobody-there"); got != "(integer) 0" {
		t.Errorf("PUBLISH to an empty channel: got %s", got)
	}

	// Multiple channels in one command produce one confirmation each.
	sub.send("SUBSCRIBE", "sports", "weather")
	if got := sub.read().String(); got != "[subscribe sports (integer) 2]" {
		t.Errorf("second subscribe: %s", got)
	}
	if got := sub.read().String(); got != "[subscribe weather (integer) 3]" {
		t.Errorf("third subscribe: %s", got)
	}

	pub.do("PUBLISH", "sports", "goal")
	if got := sub.expectPush(t); got != "[message sports goal]" {
		t.Errorf("delivery on the second channel: %s", got)
	}
}

func TestMultipleSubscribers(t *testing.T) {
	h := startServer(t, nil)
	pub := h.dial(t)

	subs := make([]*conn, 3)
	for i := range subs {
		subs[i] = h.dial(t)
		subs[i].do("SUBSCRIBE", "broadcast")
	}

	if got := pub.do("PUBLISH", "broadcast", "to-everyone"); got != "(integer) 3" {
		t.Errorf("PUBLISH: got %s, want 3", got)
	}
	for i, s := range subs {
		if got := s.expectPush(t); got != "[message broadcast to-everyone]" {
			t.Errorf("subscriber %d: got %s", i, got)
		}
	}
}

func TestPatternSubscribe(t *testing.T) {
	h := startServer(t, nil)
	sub := h.dial(t)
	pub := h.dial(t)

	if got := sub.do("PSUBSCRIBE", "news.*"); got != "[psubscribe news.* (integer) 1]" {
		t.Fatalf("PSUBSCRIBE: %s", got)
	}

	pub.do("PUBLISH", "news.sports", "goal")
	// A pattern delivery names the pattern that matched, so a client watching
	// several can tell them apart.
	if got := sub.expectPush(t); got != "[pmessage news.* news.sports goal]" {
		t.Errorf("pattern delivery: got %s", got)
	}

	// A channel outside the pattern must not be delivered.
	if got := pub.do("PUBLISH", "other.thing", "ignored"); got != "(integer) 0" {
		t.Errorf("non-matching publish: got %s", got)
	}

	// An exact and a pattern subscription both fire, so the publisher counts
	// two deliveries even though it is one connection.
	sub.do("SUBSCRIBE", "news.tech")
	if got := pub.do("PUBLISH", "news.tech", "release"); got != "(integer) 2" {
		t.Errorf("overlapping subscriptions: got %s, want 2", got)
	}
	pushes := []string{sub.expectPush(t), sub.expectPush(t)}
	joined := strings.Join(pushes, " | ")
	if !strings.Contains(joined, "[message news.tech release]") ||
		!strings.Contains(joined, "[pmessage news.* news.tech release]") {
		t.Errorf("expected both an exact and a pattern delivery, got: %s", joined)
	}
}

func TestUnsubscribe(t *testing.T) {
	h := startServer(t, nil)
	sub := h.dial(t)
	pub := h.dial(t)

	sub.send("SUBSCRIBE", "a", "b", "c")
	for i := 0; i < 3; i++ {
		sub.read()
	}

	if got := sub.do("UNSUBSCRIBE", "b"); got != "[unsubscribe b (integer) 2]" {
		t.Errorf("UNSUBSCRIBE: %s", got)
	}
	if got := pub.do("PUBLISH", "b", "gone"); got != "(integer) 0" {
		t.Errorf("publish after unsubscribe: got %s", got)
	}

	// With no arguments, everything goes -- one confirmation per channel.
	sub.send("UNSUBSCRIBE")
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		v := sub.read()
		seen[string(v.Array[1].Str)] = true
	}
	if !seen["a"] || !seen["c"] {
		t.Errorf("bare UNSUBSCRIBE did not clear both channels: %v", seen)
	}

	// Once nothing is subscribed, ordinary commands work again.
	if got := sub.do("SET", "k", "v"); got != "OK" {
		t.Errorf("commands still blocked after unsubscribing: %s", got)
	}

	// Unsubscribing from nothing still gets a reply, so a client waiting on
	// one is never left hanging.
	fresh := h.dial(t)
	if got := fresh.do("UNSUBSCRIBE"); got != "[unsubscribe (nil) (integer) 0]" {
		t.Errorf("UNSUBSCRIBE with no subscriptions: %s", got)
	}
}

// A RESP2 connection in subscribe mode is a one-way stream, so only a few
// commands remain legal on it.
func TestSubscribeModeRestrictions(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SUBSCRIBE", "channel")

	for _, cmd := range [][]string{
		{"GET", "k"},
		{"SET", "k", "v"},
		{"DEL", "k"},
		{"INFO"},
	} {
		got := c.do(cmd...)
		if !strings.Contains(got, "only (P|S)SUBSCRIBE") {
			t.Errorf("%s in subscribe mode: got %s", strings.Join(cmd, " "), got)
		}
	}

	// PING stays available so a client can keep the connection alive. Its
	// reply shape changes in this mode, which TestSubscribeModeReplyShapes
	// covers.
	if got := c.do("PING"); got != "[pong ]" {
		t.Errorf("PING in subscribe mode: %s", got)
	}
	// RESET clears the mode.
	if got := c.do("RESET"); got != "RESET" {
		t.Errorf("RESET: %s", got)
	}
}

// Verified against Redis 7: a subscribed RESP2 connection may not publish, and
// PING on one answers with an array rather than a simple string, because every
// other frame the client receives is an array too.
func TestSubscribeModeReplyShapes(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	c.do("SUBSCRIBE", "loop")

	got := c.do("PUBLISH", "loop", "to-myself")
	if !strings.Contains(got, "only (P|S)SUBSCRIBE") {
		t.Errorf("PUBLISH while subscribed: got %s, want a refusal", got)
	}

	if got := c.do("PING"); got != "[pong ]" {
		t.Errorf("PING while subscribed: got %s, want the array form", got)
	}
	if got := c.do("PING", "hello"); got != "[pong hello]" {
		t.Errorf("PING with a payload while subscribed: got %s", got)
	}

	// Outside subscribe mode it goes back to the simple string.
	c.do("UNSUBSCRIBE", "loop")
	if got := c.do("PING"); got != "PONG" {
		t.Errorf("PING after unsubscribing: got %s", got)
	}
}

func TestPubSubIntrospection(t *testing.T) {
	h := startServer(t, nil)
	c := h.dial(t)
	sub1 := h.dial(t)
	sub2 := h.dial(t)

	sub1.do("SUBSCRIBE", "news")
	sub2.send("SUBSCRIBE", "news", "sports")
	sub2.read()
	sub2.read()
	sub2.do("PSUBSCRIBE", "events.*")

	c.send("PUBSUB", "CHANNELS")
	channels := c.read()
	if len(channels.Array) != 2 {
		t.Errorf("PUBSUB CHANNELS: got %s, want 2 channels", channels.String())
	}

	c.send("PUBSUB", "CHANNELS", "new*")
	if n := len(c.read().Array); n != 1 {
		t.Errorf("PUBSUB CHANNELS new*: got %d, want 1", n)
	}

	if got := c.do("PUBSUB", "NUMSUB", "news", "sports", "nothing"); got !=
		"[news (integer) 2 sports (integer) 1 nothing (integer) 0]" {
		t.Errorf("PUBSUB NUMSUB: %s", got)
	}
	if got := c.do("PUBSUB", "NUMPAT"); got != "(integer) 1" {
		t.Errorf("PUBSUB NUMPAT: %s", got)
	}

	// A disconnecting subscriber must be removed from every registry.
	_ = sub1.nc.Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if c.do("PUBSUB", "NUMSUB", "news") == "[news (integer) 1]" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("a disconnected subscriber was not removed: %s", c.do("PUBSUB", "NUMSUB", "news"))
}

// Publishing from many goroutines while subscribers come and go must not race
// or lose messages. Run with -race.
func TestPubSubConcurrent(t *testing.T) {
	h := startServer(t, nil)

	const (
		subscribers = 8
		messages    = 50
	)

	subs := make([]*conn, subscribers)
	for i := range subs {
		subs[i] = h.dial(t)
		subs[i].do("SUBSCRIBE", "fanout")
	}

	var wg sync.WaitGroup
	received := make([]int, subscribers)
	for i := range subs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < messages; j++ {
				v := subs[i].read()
				if len(v.Array) != 3 || string(v.Array[0].Str) != "message" {
					t.Errorf("subscriber %d got a malformed frame: %s", i, v.String())
					return
				}
				received[i]++
			}
		}(i)
	}

	pub := h.dial(t)
	for j := 0; j < messages; j++ {
		if got := pub.do("PUBLISH", "fanout", fmt.Sprintf("msg-%d", j)); got !=
			fmt.Sprintf("(integer) %d", subscribers) {
			t.Fatalf("publish %d: got %s", j, got)
		}
	}

	wg.Wait()
	for i, n := range received {
		if n != messages {
			t.Errorf("subscriber %d received %d messages, want %d", i, n, messages)
		}
	}
}
