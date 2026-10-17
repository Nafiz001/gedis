// Command gedis-server runs a Redis-compatible in-memory data store.
//
// Point the official redis-cli at it:
//
//	gedis-server --addr :6380
//	redis-cli -p 6380
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Nafiz001/gedis/internal/persistence"
	"github.com/Nafiz001/gedis/internal/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "gedis:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := server.DefaultConfig()

	var (
		logLevel    string
		showVersion bool
		appendFsync string
		savePoints  string
		maxMemory   string
		evictPolicy string
		replicaOf   string
		disable     string
	)
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "address to listen on")
	flag.IntVar(&cfg.Databases, "databases", cfg.Databases, "number of SELECT-able databases")
	flag.StringVar(&cfg.Password, "requirepass", "", "require clients to AUTH with this password")
	flag.IntVar(&cfg.MaxClients, "maxclients", 0, "maximum concurrent clients (0 = unlimited)")
	flag.IntVar(&cfg.Shards, "shards", cfg.Shards, "lock shards per database")
	flag.DurationVar(&cfg.IdleTimeout, "timeout", 0, "close connections idle this long (0 = never)")
	flag.StringVar(&logLevel, "loglevel", "info", "log level: debug, info, warn, error")
	flag.BoolVar(&showVersion, "version", false, "print the version and exit")

	flag.StringVar(&cfg.Dir, "dir", cfg.Dir, "directory for persistence files")
	flag.BoolVar(&cfg.AppendOnly, "appendonly", false, "log every write to an append-only file")
	flag.StringVar(&cfg.AppendFilename, "appendfilename", cfg.AppendFilename, "append-only file name")
	flag.StringVar(&appendFsync, "appendfsync", "everysec", "durability: always, everysec, or no")
	flag.StringVar(&cfg.SnapshotFilename, "dbfilename", cfg.SnapshotFilename, "snapshot file name")
	flag.StringVar(&savePoints, "save", "", "snapshot points as seconds:changes, comma separated; \"off\" to disable")
	flag.StringVar(&maxMemory, "maxmemory", "0", "memory limit, e.g. 512mb or 2gb; 0 for no limit")
	flag.StringVar(&evictPolicy, "maxmemory-policy", "noeviction",
		"noeviction, allkeys-lru, allkeys-random, volatile-lru, volatile-random, volatile-ttl")
	flag.StringVar(&replicaOf, "replicaof", "", "replicate from a master, given as host:port")
	flag.StringVar(&disable, "disable-commands", "",
		"commands to switch off, comma separated, e.g. config,debug,replicaof (like Redis's rename-command NAME \"\")")
	flag.Parse()

	if showVersion {
		fmt.Println("gedis", server.Version)
		return nil
	}

	level, err := parseLevel(logLevel)
	if err != nil {
		return err
	}

	if cfg.AppendFsync, err = persistence.ParseFsyncPolicy(appendFsync); err != nil {
		return err
	}
	if cfg.MaxMemoryPolicy, err = server.ParseEvictionPolicy(evictPolicy); err != nil {
		return err
	}
	if cfg.MaxMemory, err = parseMemory(maxMemory); err != nil {
		return err
	}
	// An empty -save keeps the defaults; "off" or an explicit list replaces
	// them. Distinguishing the two is why the flag is a string rather than a
	// slice with a zero value that means both.
	if savePoints != "" {
		if cfg.SavePoints, err = server.ParseSavePoints(savePoints); err != nil {
			return err
		}
	}
	for _, name := range strings.Split(disable, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		// A typo here would leave the command it meant switched on, which is
		// the opposite of what the flag is for, so it is fatal.
		if !server.KnownCommand(name) {
			return fmt.Errorf("-disable-commands: %q is not a gedis command", name)
		}
		cfg.DisabledCommands = append(cfg.DisabledCommands, name)
	}
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if cfg.MaxMemory > 0 && cfg.MaxMemoryPolicy == server.EvictNone {
		cfg.Logger.Warn("maxmemory is set with the noeviction policy, so writes will be refused once it is reached")
	}

	srv := server.New(cfg)

	var master [2]string
	if replicaOf != "" {
		host, port, splitErr := net.SplitHostPort(replicaOf)
		if splitErr != nil {
			return fmt.Errorf("-replicaof must look like host:port: %w", splitErr)
		}
		master = [2]string{host, port}
	}

	// Bind before installing the signal handler so a port conflict is reported
	// as a plain startup error rather than a server that appears to start and
	// then vanishes.
	if err := srv.Listen(); err != nil {
		return err
	}

	// Start replicating only once the listener is up: the handshake tells the
	// master which port this server itself is reachable on.
	if master[0] != "" {
		srv.FollowMaster(master[0], master[1])
	}

	// SIGINT and SIGTERM trigger a graceful stop: the listener closes, in-flight
	// commands finish, and only then does the process exit.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		cfg.Logger.Info("signal received, shutting down")
		// A second signal during shutdown means the operator wants out now.
		hard := make(chan os.Signal, 1)
		signal.Notify(hard, os.Interrupt, syscall.SIGTERM)
		select {
		case <-hard:
			cfg.Logger.Warn("second signal received, exiting immediately")
			os.Exit(1)
		case <-time.After(30 * time.Second):
			cfg.Logger.Warn("shutdown timed out, exiting")
			os.Exit(1)
		}
	}()

	return srv.Serve(ctx)
}

func parseLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}

// parseMemory reads a byte count with an optional unit suffix, so a limit can
// be written the way people say it: 512mb rather than 536870912.
func parseMemory(s string) (int64, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" || s == "0" {
		return 0, nil
	}

	units := []struct {
		suffix string
		factor int64
	}{
		// Longest first, so "kb" is not matched as "b".
		{"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10},
		{"g", 1 << 30}, {"m", 1 << 20}, {"k", 1 << 10},
		{"b", 1},
	}
	for _, u := range units {
		if !strings.HasSuffix(s, u.suffix) {
			continue
		}
		n, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimSuffix(s, u.suffix)), 64)
		if err != nil {
			return 0, fmt.Errorf("invalid memory size %q", s)
		}
		return int64(n * float64(u.factor)), nil
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid memory size %q", s)
	}
	return n, nil
}
