package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Replication only means anything across two processes, so these tests stand up
// a real master and a real replica on separate ports and let them talk over TCP.

// startNamedServer brings up an independent server with its own directory.
func startNamedServer(t *testing.T, tweak func(*Config)) *harness {
	t.Helper()

	clk := &testClock{}
	clk.ms.Store(1_700_000_000_000)

	cfg := DefaultConfig()
	cfg.Addr = "127.0.0.1:0"
	cfg.Databases = 4
	cfg.Shards = 8
	cfg.Dir = t.TempDir()
	cfg.SavePoints = nil
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
			t.Error("server did not shut down")
		}
	})

	return &harness{srv: srv, addr: srv.Addr().String(), clk: clk}
}

// waitFor polls until cond holds, so tests never depend on a fixed sleep.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// hostPort splits a listener address for REPLICAOF.
func hostPort(addr string) (string, string) {
	i := strings.LastIndex(addr, ":")
	return addr[:i], addr[i+1:]
}

func TestReplicationFullSync(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	mc := master.dial(t)
	// Data written before the replica connects must arrive in the snapshot.
	mc.do("SET", "before", "sync")
	mc.do("RPUSH", "list", "a", "b")
	mc.do("HSET", "hash", "f", "v")
	mc.do("SADD", "set", "m")
	mc.do("ZADD", "zset", "1", "z")

	rc := replica.dial(t)
	host, port := hostPort(master.addr)
	if got := rc.do("REPLICAOF", host, port); got != "OK" {
		t.Fatalf("REPLICAOF: %s", got)
	}

	waitFor(t, "the replica to finish syncing", func() bool {
		return rc.do("GET", "before") == "sync"
	})

	runCmdTests(t, rc, []cmdTest{
		{[]string{"GET", "before"}, "sync"},
		{[]string{"LRANGE", "list", "0", "-1"}, "[a b]"},
		{[]string{"HGET", "hash", "f"}, "v"},
		{[]string{"SISMEMBER", "set", "m"}, "(integer) 1"},
		{[]string{"ZSCORE", "zset", "z"}, "1"},
	})
}

func TestReplicationStreamsWrites(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	mc := master.dial(t)
	rc := replica.dial(t)
	host, port := hostPort(master.addr)
	rc.do("REPLICAOF", host, port)

	waitFor(t, "the initial sync", func() bool {
		return replica.srv.repl.linkUp.Load()
	})

	// Everything after the sync has to arrive over the stream.
	mc.do("SET", "streamed", "yes")
	mc.do("INCRBY", "counter", "5")
	mc.do("RPUSH", "queue", "job1", "job2")
	mc.do("HSET", "profile", "name", "nafiz")

	waitFor(t, "streamed writes to arrive", func() bool {
		return rc.do("GET", "streamed") == "yes"
	})

	runCmdTests(t, rc, []cmdTest{
		{[]string{"GET", "streamed"}, "yes"},
		{[]string{"GET", "counter"}, "5"},
		{[]string{"LRANGE", "queue", "0", "-1"}, "[job1 job2]"},
		{[]string{"HGET", "profile", "name"}, "nafiz"},
	})

	// Deletions propagate too, or a replica slowly diverges upward.
	mc.do("DEL", "streamed")
	waitFor(t, "the deletion to arrive", func() bool {
		return rc.do("EXISTS", "streamed") == "(integer) 0"
	})
}

// A replica serves reads and refuses writes, so the master stays the only
// place data changes.
func TestReplicaIsReadOnly(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	mc := master.dial(t)
	mc.do("SET", "k", "v")

	rc := replica.dial(t)
	host, port := hostPort(master.addr)
	rc.do("REPLICAOF", host, port)
	waitFor(t, "sync", func() bool { return rc.do("GET", "k") == "v" })

	for _, cmd := range [][]string{
		{"SET", "new", "v"},
		{"DEL", "k"},
		{"INCR", "counter"},
		{"RPUSH", "list", "v"},
		{"FLUSHALL"},
	} {
		got := rc.do(cmd...)
		if !strings.HasPrefix(got, "(error) READONLY") {
			t.Errorf("%s on a replica: got %s, want READONLY", strings.Join(cmd, " "), got)
		}
	}

	// Reads still work, which is the point of having a replica.
	if got := rc.do("GET", "k"); got != "v" {
		t.Errorf("read on a replica: %s", got)
	}
}

// Promotion keeps the replicated data and re-enables writes, which is what a
// failover is.
func TestReplicaPromotion(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	mc := master.dial(t)
	mc.do("SET", "inherited", "data")

	rc := replica.dial(t)
	host, port := hostPort(master.addr)
	rc.do("REPLICAOF", host, port)
	waitFor(t, "sync", func() bool { return rc.do("GET", "inherited") == "data" })

	if got := rc.do("REPLICAOF", "NO", "ONE"); got != "OK" {
		t.Fatalf("REPLICAOF NO ONE: %s", got)
	}

	// The data survives promotion, and writes are accepted again.
	if got := rc.do("GET", "inherited"); got != "data" {
		t.Errorf("data lost on promotion: %s", got)
	}
	if got := rc.do("SET", "own", "write"); got != "OK" {
		t.Errorf("a promoted master still refuses writes: %s", got)
	}

	// The old master's writes must no longer reach it.
	mc.do("SET", "after-promotion", "v")
	time.Sleep(200 * time.Millisecond)
	if got := rc.do("EXISTS", "after-promotion"); got != "(integer) 0" {
		t.Errorf("a promoted master is still following its old master: %s", got)
	}
}

// Writes that land while the snapshot is being sent must not be lost in the
// gap between the snapshot and the stream.
func TestReplicationNoGapDuringSync(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	mc := master.dial(t)
	// Enough keys that serialising the snapshot takes real time.
	for i := 0; i < 2000; i++ {
		mc.do("SET", fmt.Sprintf("key:%d", i), fmt.Sprintf("value:%d", i))
	}

	rc := replica.dial(t)
	host, port := hostPort(master.addr)

	// Start the sync, then immediately keep writing on a separate connection.
	writer := master.dial(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			writer.do("SET", fmt.Sprintf("during:%d", i), "written-during-sync")
		}
	}()

	rc.do("REPLICAOF", host, port)
	<-done

	waitFor(t, "every write to reach the replica", func() bool {
		return rc.do("GET", "during:199") == "written-during-sync"
	})

	// Every one of them, not just the last.
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("during:%d", i)
		if got := rc.do("GET", key); got != "written-during-sync" {
			t.Fatalf("%s was lost between the snapshot and the stream: got %s", key, got)
		}
	}
	if got := rc.do("DBSIZE"); got != "(integer) 2200" {
		t.Errorf("DBSIZE on the replica: got %s, want 2200", got)
	}
}

func TestReplicationInfo(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	mc := master.dial(t)
	mc.send("INFO", "replication")
	info := string(mc.read().Str)
	if !strings.Contains(info, "role:master") {
		t.Errorf("a standalone server should report role:master\n%s", info)
	}
	if !strings.Contains(info, "connected_slaves:0") {
		t.Errorf("expected no replicas yet\n%s", info)
	}
	if !strings.Contains(info, "master_replid:") {
		t.Errorf("missing the replication id\n%s", info)
	}

	rc := replica.dial(t)
	host, port := hostPort(master.addr)
	rc.do("REPLICAOF", host, port)
	waitFor(t, "the link to come up", func() bool { return replica.srv.repl.linkUp.Load() })

	rc.send("INFO", "replication")
	replicaInfo := string(rc.read().Str)
	for _, want := range []string{"role:slave", "master_link_status:up", "slave_read_only:1"} {
		if !strings.Contains(replicaInfo, want) {
			t.Errorf("replica INFO is missing %q\n%s", want, replicaInfo)
		}
	}

	waitFor(t, "the master to register the replica", func() bool {
		return master.srv.replicas.count() == 1
	})
	mc.send("INFO", "replication")
	masterInfo := string(mc.read().Str)
	if !strings.Contains(masterInfo, "connected_slaves:1") {
		t.Errorf("master does not report the replica\n%s", masterInfo)
	}
	if !strings.Contains(masterInfo, "slave0:") {
		t.Errorf("master does not list the replica\n%s", masterInfo)
	}
}

// A replica that loses its master must reconnect and resync on its own.
func TestReplicaReconnects(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	mc := master.dial(t)
	mc.do("SET", "first", "1")

	rc := replica.dial(t)
	host, port := hostPort(master.addr)
	rc.do("REPLICAOF", host, port)
	waitFor(t, "the first sync", func() bool { return rc.do("GET", "first") == "1" })

	// Drop every replica connection, as a master restart would.
	for _, link := range master.srv.replicas.snapshot() {
		link.fail()
	}
	waitFor(t, "the master to notice the drop", func() bool {
		return master.srv.replicas.count() == 0
	})

	// The replica should come back on its own and pick up new data.
	mc.do("SET", "second", "2")
	waitFor(t, "the replica to reconnect and resync", func() bool {
		return rc.do("GET", "second") == "2"
	})
}

// Pub/sub messages published on a master reach subscribers on a replica.
func TestReplicationForwardsPublish(t *testing.T) {
	master := startNamedServer(t, nil)
	replica := startNamedServer(t, nil)

	rc := replica.dial(t)
	host, port := hostPort(master.addr)
	rc.do("REPLICAOF", host, port)
	waitFor(t, "sync", func() bool { return replica.srv.repl.linkUp.Load() })

	sub := replica.dial(t)
	sub.do("SUBSCRIBE", "events")

	mc := master.dial(t)
	mc.do("PUBLISH", "events", "from-the-master")

	if got := sub.expectPush(t); got != "[message events from-the-master]" {
		t.Errorf("a message published on the master did not reach the replica: %s", got)
	}
}
