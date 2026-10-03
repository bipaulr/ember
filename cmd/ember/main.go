// Command ember runs the Ember server.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"ember/internal/command"
	"ember/internal/server"
	"ember/internal/store"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ember:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("ember", flag.ContinueOnError)
	var (
		host       = fs.String("host", "0.0.0.0", "interface to listen on")
		port       = fs.Int("port", 6380, "TCP port (6380 avoids clashing with Redis)")
		shards     = fs.Int("shards", store.DefaultShards, "number of keyspace shards")
		maxClients = fs.Int("max-clients", 10000, "maximum simultaneous clients")
		verbose    = fs.Bool("verbose", false, "debug logging")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	st := store.New(*shards)
	env := command.NewEnv(st)
	srv := server.New(env, server.Config{
		Addr:       *host + ":" + strconv.Itoa(*port),
		MaxClients: *maxClients,
		Logger:     log,
	})
	ln, err := srv.Listen()
	if err != nil {
		return err
	}

	stopExpiry := make(chan struct{})
	go st.RunExpiry(100*time.Millisecond, stopExpiry)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("ember ready", "addr", ln.Addr().String(), "shards", st.NumShards())

	select {
	case err := <-errc:
		close(stopExpiry)
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = srv.Shutdown(sctx)
	close(stopExpiry)
	return err
}
