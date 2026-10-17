package server

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Nafiz001/gedis/internal/store"
)

// Pub/Sub is the one place where a command run by one client writes to another
// client's socket, which makes it the one place the per-connection goroutine
// model needs an explicit rule.
//
// The rule: every client owns a writeMu, and nothing writes to that client's
// connection without holding it. The connection loop takes it around each
// command's reply, and a publisher takes it around each delivery. Contention is
// effectively nil -- a connection is almost never receiving a reply and a
// published message at the same instant -- so the cost is one uncontended
// lock per command batch.
//
// The alternative, an outbox channel and a writer goroutine per client, would
// double the goroutine count and add a queue to reason about for a problem this
// mutex solves outright.

// pubsubDeliveryTimeout bounds how long a publisher waits on one subscriber.
//
// Without it a single subscriber that has stopped reading -- a laptop that went
// to sleep mid-stream -- would block the publishing client indefinitely once
// the kernel send buffer filled. Redis solves this with output buffer limits
// and disconnects the offender; gedis uses a write deadline and does the same.
const pubsubDeliveryTimeout = 100 * time.Millisecond

// pubsubRegistry maps channels and patterns to the clients listening on them.
type pubsubRegistry struct {
	mu sync.RWMutex
	// channels holds exact subscriptions, patterns holds glob ones. They are
	// separate because a publish must check every pattern but can look up an
	// exact channel in one map hit.
	channels map[string]map[uint64]*client
	patterns map[string]map[uint64]*client
}

func newPubsubRegistry() *pubsubRegistry {
	return &pubsubRegistry{
		channels: make(map[string]map[uint64]*client),
		patterns: make(map[string]map[uint64]*client),
	}
}

func (r *pubsubRegistry) subscribe(c *client, channel string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	subs, ok := r.channels[channel]
	if !ok {
		subs = make(map[uint64]*client)
		r.channels[channel] = subs
	}
	if _, already := subs[c.id]; already {
		return false
	}
	subs[c.id] = c
	c.channels[channel] = struct{}{}
	return true
}

func (r *pubsubRegistry) psubscribe(c *client, pattern string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	subs, ok := r.patterns[pattern]
	if !ok {
		subs = make(map[uint64]*client)
		r.patterns[pattern] = subs
	}
	if _, already := subs[c.id]; already {
		return false
	}
	subs[c.id] = c
	c.patterns[pattern] = struct{}{}
	return true
}

func (r *pubsubRegistry) unsubscribe(c *client, channel string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.unsubscribeLocked(c, channel)
}

func (r *pubsubRegistry) unsubscribeLocked(c *client, channel string) bool {
	subs, ok := r.channels[channel]
	if !ok {
		return false
	}
	if _, subscribed := subs[c.id]; !subscribed {
		return false
	}
	delete(subs, c.id)
	// Drop the channel entirely once nobody is listening, so the map does not
	// accumulate an entry for every channel ever used.
	if len(subs) == 0 {
		delete(r.channels, channel)
	}
	delete(c.channels, channel)
	return true
}

func (r *pubsubRegistry) punsubscribe(c *client, pattern string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.punsubscribeLocked(c, pattern)
}

func (r *pubsubRegistry) punsubscribeLocked(c *client, pattern string) bool {
	subs, ok := r.patterns[pattern]
	if !ok {
		return false
	}
	if _, subscribed := subs[c.id]; !subscribed {
		return false
	}
	delete(subs, c.id)
	if len(subs) == 0 {
		delete(r.patterns, pattern)
	}
	delete(c.patterns, pattern)
	return true
}

// removeClient drops every subscription a disconnecting client held.
func (r *pubsubRegistry) removeClient(c *client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for channel := range c.channels {
		r.unsubscribeLocked(c, channel)
	}
	for pattern := range c.patterns {
		r.punsubscribeLocked(c, pattern)
	}
}

// recipient is one delivery target: the client, and the pattern that matched
// when the subscription was a pattern one.
type recipient struct {
	c       *client
	pattern string
}

// publish delivers a message and returns how many clients received it.
//
// Targets are collected under a read lock and delivered after releasing it.
// Writing to sockets while holding the registry lock would let one slow
// subscriber stall every SUBSCRIBE and PUBLISH on the server.
func (r *pubsubRegistry) publish(from *client, channel string, payload []byte) int {
	r.mu.RLock()
	var targets []recipient
	for _, c := range r.channels[channel] {
		targets = append(targets, recipient{c: c})
	}
	for pattern, subs := range r.patterns {
		if !store.Match(pattern, channel) {
			continue
		}
		for _, c := range subs {
			targets = append(targets, recipient{c: c, pattern: pattern})
		}
	}
	r.mu.RUnlock()

	delivered := 0
	for _, t := range targets {
		if t.c.deliverMessage(from, channel, t.pattern, payload) {
			delivered++
		}
	}
	return delivered
}

// channelNames lists active channels, optionally filtered by a glob.
func (r *pubsubRegistry) channelNames(pattern string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var out []string
	for channel := range r.channels {
		if pattern == "" || store.Match(pattern, channel) {
			out = append(out, channel)
		}
	}
	sort.Strings(out)
	return out
}

func (r *pubsubRegistry) subscriberCount(channel string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.channels[channel])
}

func (r *pubsubRegistry) patternCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.patterns)
}

// deliverMessage pushes one message to this client's connection.
//
// The c == from branch is a guard rather than a live path. Over RESP2 a
// subscribed connection may not run PUBLISH at all, so a client cannot
// currently publish to itself -- but if it could, the publisher would already
// hold its own write lock and taking it again would deadlock, since Go mutexes
// are not reentrant. The check costs a pointer comparison and removes a whole
// class of future failure.
func (c *client) deliverMessage(from *client, channel, pattern string, payload []byte) bool {
	if c == from {
		c.writeMessage(channel, pattern, payload)
		return true
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.closing {
		return false
	}

	// Bound the wait so a subscriber that has stopped reading cannot hold the
	// publisher hostage once its send buffer fills.
	if c.conn != nil {
		_ = c.conn.SetWriteDeadline(time.Now().Add(pubsubDeliveryTimeout))
	}
	c.writeMessage(channel, pattern, payload)
	err := c.w.Flush()
	if c.conn != nil {
		_ = c.conn.SetWriteDeadline(time.Time{})
	}

	if err != nil {
		// The subscriber is wedged or gone. Close its connection, which its
		// own goroutine notices on the next read and cleans up after.
		c.closing = true
		if c.conn != nil {
			_ = c.conn.Close()
		}
		return false
	}
	return true
}

// writeMessage emits a pub/sub push frame. The caller holds writeMu.
//
// A pattern delivery carries an extra field naming the pattern that matched,
// so a client subscribed to several patterns can tell which one fired.
func (c *client) writeMessage(channel, pattern string, payload []byte) {
	if pattern != "" {
		c.replyArrayLen(4)
		c.replyBulkString("pmessage")
		c.replyBulkString(pattern)
		c.replyBulkString(channel)
		c.replyBulk(payload)
		return
	}
	c.replyArrayLen(3)
	c.replyBulkString("message")
	c.replyBulkString(channel)
	c.replyBulk(payload)
}

// subscriptionCount is the total a subscribe/unsubscribe confirmation reports.
func (c *client) subscriptionCount() int {
	return len(c.channels) + len(c.patterns)
}

// inSubscribeMode reports whether the client has any subscription.
//
// While subscribed, a RESP2 connection is a one-way message stream and only a
// small set of commands remains legal. Redis relaxed this in RESP3, where
// pushes are a distinct frame type the client can demultiplex; gedis speaks
// RESP2 only, so the restriction stands.
func (c *client) inSubscribeMode() bool { return c.subscriptionCount() > 0 }

// commandsAllowedWhileSubscribed is the set that still works in subscribe mode.
var commandsAllowedWhileSubscribed = map[string]bool{
	"subscribe":    true,
	"unsubscribe":  true,
	"psubscribe":   true,
	"punsubscribe": true,
	"ping":         true,
	"quit":         true,
	"reset":        true,
	"shutdown":     true,
}

// --- commands ---

// subscribeHandler builds SUBSCRIBE and PSUBSCRIBE.
func subscribeHandler(isPattern bool) handlerFunc {
	return func(c *client, args [][]byte) {
		verb := "subscribe"
		if isPattern {
			verb = "psubscribe"
		}
		// One confirmation per channel, each carrying the running total.
		for _, raw := range args[1:] {
			name := string(raw)
			if isPattern {
				c.srv.pubsub.psubscribe(c, name)
			} else {
				c.srv.pubsub.subscribe(c, name)
			}
			c.replyArrayLen(3)
			c.replyBulkString(verb)
			c.replyBulkString(name)
			c.replyInt(int64(c.subscriptionCount()))
		}
	}
}

var (
	cmdSubscribe  = subscribeHandler(false)
	cmdPSubscribe = subscribeHandler(true)
)

// unsubscribeHandler builds UNSUBSCRIBE and PUNSUBSCRIBE.
func unsubscribeHandler(isPattern bool) handlerFunc {
	return func(c *client, args [][]byte) {
		verb := "unsubscribe"
		if isPattern {
			verb = "punsubscribe"
		}

		// With no arguments, drop everything of that kind.
		names := make([]string, 0, len(args)-1)
		if len(args) == 1 {
			source := c.channels
			if isPattern {
				source = c.patterns
			}
			for name := range source {
				names = append(names, name)
			}
			sort.Strings(names)
		} else {
			for _, raw := range args[1:] {
				names = append(names, string(raw))
			}
		}

		// Unsubscribing from nothing still gets one confirmation, so a client
		// waiting for a reply is never left hanging.
		if len(names) == 0 {
			c.replyArrayLen(3)
			c.replyBulkString(verb)
			c.replyNull()
			c.replyInt(int64(c.subscriptionCount()))
			return
		}

		for _, name := range names {
			if isPattern {
				c.srv.pubsub.punsubscribe(c, name)
			} else {
				c.srv.pubsub.unsubscribe(c, name)
			}
			c.replyArrayLen(3)
			c.replyBulkString(verb)
			c.replyBulkString(name)
			c.replyInt(int64(c.subscriptionCount()))
		}
	}
}

var (
	cmdUnsubscribe  = unsubscribeHandler(false)
	cmdPUnsubscribe = unsubscribeHandler(true)
)

func cmdPublish(c *client, args [][]byte) {
	channel := string(args[1])
	delivered := c.srv.pubsub.publish(c, channel, args[2])

	// PUBLISH changes no data, so it is not a write and never reaches the
	// log. It is still forwarded to replicas, so that subscribers attached to
	// a replica see messages published on the master.
	c.srv.replicas.propagate(c.dbIndex, args)

	c.replyInt(int64(delivered))
}

// cmdPubSub implements the PUBSUB introspection subcommands.
func cmdPubSub(c *client, args [][]byte) {
	switch strings.ToLower(string(args[1])) {
	case "channels":
		pattern := ""
		if len(args) == 3 {
			pattern = string(args[2])
		} else if len(args) > 3 {
			c.replyWrongArity("pubsub|channels")
			return
		}
		c.replyStrings(c.srv.pubsub.channelNames(pattern))

	case "numsub":
		// A flat channel/count array, in the order asked.
		c.replyArrayLen((len(args) - 2) * 2)
		for _, raw := range args[2:] {
			channel := string(raw)
			c.replyBulkString(channel)
			c.replyInt(int64(c.srv.pubsub.subscriberCount(channel)))
		}

	case "numpat":
		// The number of distinct patterns, not of pattern subscribers.
		c.replyInt(int64(c.srv.pubsub.patternCount()))

	default:
		c.replyErrorf("ERR Unknown PUBSUB subcommand or wrong number of arguments for '%s'",
			string(args[1]))
	}
}
