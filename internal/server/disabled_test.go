package server

import (
	"strconv"
	"strings"
	"testing"
)

func TestDisabledCommands(t *testing.T) {
	h := startServer(t, func(cfg *Config) {
		cfg.DisabledCommands = []string{"CONFIG", "debug", "replicaof"}
	})
	c := h.dial(t)

	// A disabled command answers exactly as an unknown one would, whatever
	// case it is sent in.
	for _, args := range [][]string{
		{"CONFIG", "GET", "dir"},
		{"config", "set", "dir", "/tmp"},
		{"DEBUG", "SLEEP", "0"},
		{"REPLICAOF", "127.0.0.1", "6379"},
	} {
		got := c.do(args...)
		want := "(error) ERR unknown command '" + args[0] + "'"
		if !strings.HasPrefix(got, want) {
			t.Errorf("%v: got %q, want it to start with %q", args, got, want)
		}
	}

	// Everything else still works.
	runCmdTests(t, c, []cmdTest{
		{[]string{"SET", "k", "v"}, "OK"},
		{[]string{"GET", "k"}, "v"},
	})

	// Inside a transaction it is refused when queued, so EXEC aborts rather
	// than running it.
	runCmdTests(t, c, []cmdTest{{[]string{"MULTI"}, "OK"}})
	if got := c.do("CONFIG", "GET", "dir"); !strings.HasPrefix(got, "(error) ERR unknown command") {
		t.Errorf("CONFIG inside MULTI: got %q", got)
	}
	if got := c.do("EXEC"); !strings.HasPrefix(got, "(error) EXECABORT") {
		t.Errorf("EXEC after a disabled command: got %q", got)
	}

	// COMMAND does not advertise what the server will not run.
	plain := startServer(t, nil).dial(t)
	all := countOf(t, plain.do("COMMAND", "COUNT"))
	fewer := countOf(t, c.do("COMMAND", "COUNT"))
	if all-fewer != 3 {
		t.Errorf("COMMAND COUNT: %d with three disabled, %d without", fewer, all)
	}

	// The switch belongs to one server, not the process: another server
	// started alongside still has CONFIG.
	if got := plain.do("CONFIG", "GET", "maxmemory"); strings.HasPrefix(got, "(error)") {
		t.Errorf("CONFIG on a server with nothing disabled: got %q", got)
	}
}

func TestKnownCommand(t *testing.T) {
	for name, want := range map[string]bool{"config": true, "CONFIG": true, "Get": true, "nosuchcommand": false, "": false} {
		if got := KnownCommand(name); got != want {
			t.Errorf("KnownCommand(%q) = %v, want %v", name, got, want)
		}
	}
}

func countOf(t *testing.T, reply string) int {
	t.Helper()
	n, err := strconv.Atoi(strings.TrimPrefix(reply, "(integer) "))
	if err != nil {
		t.Fatalf("not an integer reply: %q", reply)
	}
	return n
}
