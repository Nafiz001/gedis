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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nafizahmed/gedis/internal/server"
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
	)
	flag.StringVar(&cfg.Addr, "addr", cfg.Addr, "address to listen on")
	flag.IntVar(&cfg.Databases, "databases", cfg.Databases, "number of SELECT-able databases")
	flag.StringVar(&cfg.Password, "requirepass", "", "require clients to AUTH with this password")
	flag.IntVar(&cfg.MaxClients, "maxclients", 0, "maximum concurrent clients (0 = unlimited)")
	flag.IntVar(&cfg.Shards, "shards", cfg.Shards, "lock shards per database")
	flag.DurationVar(&cfg.IdleTimeout, "timeout", 0, "close connections idle this long (0 = never)")
	flag.StringVar(&logLevel, "loglevel", "info", "log level: debug, info, warn, error")
	flag.BoolVar(&showVersion, "version", false, "print the version and exit")
	flag.Parse()

	if showVersion {
		fmt.Println("gedis", server.Version)
		return nil
	}

	level, err := parseLevel(logLevel)
	if err != nil {
		return err
	}
	cfg.Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	srv := server.New(cfg)

	// Bind before installing the signal handler so a port conflict is reported
	// as a plain startup error rather than a server that appears to start and
	// then vanishes.
	if err := srv.Listen(); err != nil {
		return err
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
