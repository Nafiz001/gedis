package server

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nafizahmed/gedis/internal/store"
)

func cmdDBSize(c *client, args [][]byte) {
	c.replyInt(int64(c.db.Len()))
}

func cmdFlushDB(c *client, args [][]byte) {
	// ASYNC and SYNC are accepted and ignored: the distinction only matters for
	// a server that frees memory on the main thread, which this one does not.
	if len(args) > 2 {
		c.replyWrongArity("flushdb")
		return
	}
	c.db.Flush()
	// Flush removes keys without going through the per-key write path, so
	// watchers have to be invalidated explicitly or a transaction could
	// survive its data being wiped.
	c.srv.watch.touchDatabase(c.dbIndex)
	c.replyOK()
}

func cmdFlushAll(c *client, args [][]byte) {
	if len(args) > 2 {
		c.replyWrongArity("flushall")
		return
	}
	for i := 0; i < c.srv.cfg.Databases; i++ {
		c.srv.dbs[i].Flush()
	}
	c.srv.watch.touchAll()
	c.replyOK()
}

func cmdTime(c *client, args [][]byte) {
	now := time.Now()
	c.replyArrayLen(2)
	c.replyBulkString(strconv.FormatInt(now.Unix(), 10))
	c.replyBulkString(strconv.FormatInt(int64(now.Nanosecond()/1000), 10))
}

// cmdInfo reports server state in the INFO text format: sections introduced by
// a "# Name" line, then field:value lines.
func cmdInfo(c *client, args [][]byte) {
	section := ""
	if len(args) > 1 {
		section = strings.ToLower(string(args[1]))
	}

	var b strings.Builder
	want := func(name string) bool {
		return section == "" || section == "all" || section == "everything" || section == name
	}

	if want("server") {
		b.WriteString("# Server\r\n")
		fmt.Fprintf(&b, "gedis_version:%s\r\n", Version)
		fmt.Fprintf(&b, "go_version:%s\r\n", runtime.Version())
		fmt.Fprintf(&b, "os:%s %s\r\n", runtime.GOOS, runtime.GOARCH)
		fmt.Fprintf(&b, "process_id:%d\r\n", os.Getpid())
		fmt.Fprintf(&b, "tcp_port:%s\r\n", portOf(c.srv))
		fmt.Fprintf(&b, "uptime_in_seconds:%d\r\n", int64(time.Since(c.srv.startedAt).Seconds()))
		fmt.Fprintf(&b, "uptime_in_days:%d\r\n", int64(time.Since(c.srv.startedAt).Hours()/24))
		b.WriteString("\r\n")
	}

	if want("clients") {
		b.WriteString("# Clients\r\n")
		fmt.Fprintf(&b, "connected_clients:%d\r\n", c.srv.clientCount())
		fmt.Fprintf(&b, "maxclients:%d\r\n", c.srv.cfg.MaxClients)
		fmt.Fprintf(&b, "clients_in_transaction:%d\r\n", c.srv.blockedTransactionCount())
		fmt.Fprintf(&b, "pubsub_channels:%d\r\n", len(c.srv.pubsub.channelNames("")))
		fmt.Fprintf(&b, "pubsub_patterns:%d\r\n", c.srv.pubsub.patternCount())
		b.WriteString("\r\n")
	}

	if want("memory") {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		b.WriteString("# Memory\r\n")
		// used_memory is the Go heap in use, which is the closest honest
		// analogue to what Redis reports from its allocator.
		fmt.Fprintf(&b, "used_memory:%d\r\n", m.HeapAlloc)
		fmt.Fprintf(&b, "used_memory_human:%.2fM\r\n", float64(m.HeapAlloc)/(1024*1024))
		fmt.Fprintf(&b, "used_memory_rss:%d\r\n", m.Sys)
		fmt.Fprintf(&b, "mem_allocator:go-%s\r\n", runtime.Version())
		fmt.Fprintf(&b, "num_gc:%d\r\n", m.NumGC)
		b.WriteString(c.srv.memoryLimitInfo())
		b.WriteString("\r\n")
	}

	if want("stats") {
		b.WriteString("# Stats\r\n")
		fmt.Fprintf(&b, "total_connections_received:%d\r\n", c.srv.stats.connectionsReceived.Load())
		fmt.Fprintf(&b, "total_commands_processed:%d\r\n", c.srv.stats.commandsProcessed.Load())
		fmt.Fprintf(&b, "rejected_connections:%d\r\n", c.srv.stats.rejectedConnections.Load())

		var hits, misses, expired int64
		for _, db := range c.srv.dbs {
			st := db.Stats()
			hits += st.Hits
			misses += st.Misses
			expired += st.ExpiredKeys
		}
		fmt.Fprintf(&b, "keyspace_hits:%d\r\n", hits)
		fmt.Fprintf(&b, "keyspace_misses:%d\r\n", misses)
		fmt.Fprintf(&b, "expired_keys:%d\r\n", expired)
		b.WriteString("\r\n")
	}

	if want("persistence") {
		b.WriteString(c.srv.persistenceInfo())
	}

	if want("replication") {
		b.WriteString(c.srv.replicationInfo())
	}

	if want("cpu") {
		b.WriteString("# CPU\r\n")
		fmt.Fprintf(&b, "num_cpu:%d\r\n", runtime.NumCPU())
		fmt.Fprintf(&b, "num_goroutine:%d\r\n", runtime.NumGoroutine())
		b.WriteString("\r\n")
	}

	if want("keyspace") {
		b.WriteString("# Keyspace\r\n")
		for i, db := range c.srv.dbs {
			st := db.Stats()
			if st.Keys == 0 {
				continue // Redis omits empty databases entirely
			}
			fmt.Fprintf(&b, "db%d:keys=%d,expires=%d,avg_ttl=0\r\n", i, st.Keys, st.Expires)
		}
		b.WriteString("\r\n")
	}

	c.replyBulkString(b.String())
}

func portOf(s *Server) string {
	addr := s.Addr()
	if addr == nil {
		return "0"
	}
	str := addr.String()
	if i := strings.LastIndex(str, ":"); i >= 0 {
		return str[i+1:]
	}
	return "0"
}

// cmdConfig implements the small slice of CONFIG that clients and benchmark
// tools actually probe on startup.
func cmdConfig(c *client, args [][]byte) {
	switch strings.ToLower(string(args[1])) {
	case "get":
		if len(args) < 3 {
			c.replyWrongArity("config|get")
			return
		}
		params := c.srv.configParams()

		// CONFIG GET takes glob patterns and returns a flat key/value array.
		matched := map[string]string{}
		for _, raw := range args[2:] {
			pattern := strings.ToLower(string(raw))
			for k, v := range params {
				if store.Match(pattern, k) {
					matched[k] = v
				}
			}
		}
		keys := make([]string, 0, len(matched))
		for k := range matched {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		c.replyArrayLen(len(keys) * 2)
		for _, k := range keys {
			c.replyBulkString(k)
			c.replyBulkString(matched[k])
		}

	case "set":
		if len(args) < 4 {
			c.replyWrongArity("config|set")
			return
		}
		// Accepted but not applied: the settings clients try to change at
		// runtime (appendonly, save, maxmemory) are not implemented yet, and
		// failing here would break tools that set them defensively.
		c.replyOK()

	case "resetstat":
		c.srv.stats.commandsProcessed.Store(0)
		c.srv.stats.connectionsReceived.Store(0)
		c.srv.stats.rejectedConnections.Store(0)
		c.replyOK()

	case "rewrite":
		c.replyError("ERR The server is running without a config file")

	default:
		c.replyErrorf("ERR Unknown CONFIG subcommand or wrong number of arguments for '%s'",
			string(args[1]))
	}
}

func (s *Server) configParams() map[string]string {
	return map[string]string{
		"maxmemory":              strconv.FormatInt(s.cfg.MaxMemory, 10),
		"maxmemory-policy":       s.cfg.MaxMemoryPolicy.String(),
		"maxclients":             strconv.Itoa(s.cfg.MaxClients),
		"databases":              strconv.Itoa(s.cfg.Databases),
		"appendonly":             boolToYesNo(s.cfg.AppendOnly),
		"appendfsync":            s.cfg.AppendFsync.String(),
		"dir":                    s.cfg.Dir,
		"dbfilename":             s.cfg.SnapshotFilename,
		"appendfilename":         s.cfg.AppendFilename,
		"save":                   formatSavePoints(s.cfg.SavePoints),
		"timeout":                strconv.Itoa(int(s.cfg.IdleTimeout.Seconds())),
		"proto-max-bulk-len":     "536870912",
		"list-max-listpack-size": "128",
	}
}

// cmdDebug exposes the introspection hooks that make the server testable from
// redis-cli.
func cmdDebug(c *client, args [][]byte) {
	switch strings.ToLower(string(args[1])) {
	case "sleep":
		if len(args) != 3 {
			c.replyWrongArity("debug|sleep")
			return
		}
		secs, err := strconv.ParseFloat(string(args[2]), 64)
		if err != nil || secs < 0 {
			c.replyNotFloat()
			return
		}
		// Only this connection blocks. In real Redis DEBUG SLEEP stalls the
		// whole server, which is precisely the difference this design exists
		// to demonstrate.
		time.Sleep(time.Duration(secs * float64(time.Second)))
		c.replyOK()

	case "jmap", "set-active-expire", "quicklist-packed-threshold", "stringmatch-len", "change-repl-id":
		c.replyOK()

	case "expire-cycle":
		// Force a sweep instead of waiting for the ticker; used by tests.
		c.db.RunExpiryCycle()
		c.replyOK()

	case "object":
		if len(args) != 3 {
			c.replyWrongArity("debug|object")
			return
		}
		key := string(args[2])
		v, ok := c.db.Get(key)
		if !ok {
			c.replyError("ERR no such key")
			return
		}
		size := 0
		if b, isString := v.([]byte); isString {
			size = len(b)
		}
		ttl, _ := c.db.TTL(key)
		c.replySimple(fmt.Sprintf("Value at:0x0 refcount:1 encoding:raw serializedlength:%d ttl:%d",
			size, ttl))

	default:
		c.replyErrorf("ERR DEBUG subcommand '%s' not supported", string(args[1]))
	}
}

func cmdShutdown(c *client, args [][]byte) {
	// NOSAVE and SAVE are accepted for compatibility; with no persistence yet
	// there is nothing to write either way.
	c.srv.log.Info("shutdown requested by client", "client", c.id)
	c.srv.Close()
	// No reply: Redis closes the connection without answering SHUTDOWN, and a
	// client that got "+OK" would wrongly believe the socket is still usable.
	c.closeAfterReply = true
}

func boolToYesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// formatSavePoints renders save points the way the config flag accepts them.
func formatSavePoints(points []SavePoint) string {
	parts := make([]string, 0, len(points))
	for _, p := range points {
		parts = append(parts, fmt.Sprintf("%d:%d", int(p.After.Seconds()), p.Changes))
	}
	return strings.Join(parts, ",")
}
