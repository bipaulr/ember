// Command ember runs the Ember server.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"ember/internal/aof"
	"ember/internal/command"
	"ember/internal/server"
	"ember/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "ember:", err)
		os.Exit(1)
	}
}

// run starts the server and blocks until ctx is cancelled (graceful shutdown)
// or the server fails. Logs go to logw.
func run(ctx context.Context, args []string, logw io.Writer) error {
	fs := flag.NewFlagSet("ember", flag.ContinueOnError)
	var (
		host       = fs.String("host", "0.0.0.0", "interface to listen on")
		port       = fs.Int("port", 6380, "TCP port (6380 avoids clashing with Redis)")
		shards     = fs.Int("shards", store.DefaultShards, "number of keyspace shards")
		maxClients = fs.Int("max-clients", 10000, "maximum simultaneous clients")
		aofPath    = fs.String("aof-path", "ember.aof", "append-only file path; empty disables persistence")
		fsync      = fs.String("appendfsync", "everysec", "AOF fsync policy: always, everysec or no")
		rewriteMin = fs.Int64("aof-rewrite-min-size", aof.DefaultRewriteMinSize, "automatic rewrite needs the AOF to be at least this many bytes (negative disables)")
		verbose    = fs.Bool("verbose", false, "debug logging")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}
	policy, err := aof.ParsePolicy(*fsync)
	if err != nil {
		return err
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(logw, &slog.HandlerOptions{Level: level}))

	st := store.New(*shards)
	env := command.NewEnv(st)

	// Replay the AOF before the listener exists, so no client can observe a
	// half-loaded keyspace.
	var journal *aof.AOF
	if *aofPath != "" {
		journal, err = openAOF(st, env, *aofPath, policy, *rewriteMin, log)
		if err != nil {
			return err
		}
		env.AOF = journal
	}

	srv := server.New(env, server.Config{
		Addr:       *host + ":" + strconv.Itoa(*port),
		MaxClients: *maxClients,
		Logger:     log,
	})
	ln, err := srv.Listen()
	if err != nil {
		if journal != nil {
			_ = journal.Close()
		}
		return err
	}

	stopExpiry := make(chan struct{})
	go st.RunExpiry(100*time.Millisecond, stopExpiry)

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	log.Info("ember ready", "addr", ln.Addr().String(), "shards", st.NumShards(),
		"aof", *aofPath, "appendfsync", policy.String())

	var serveErr error
	select {
	case serveErr = <-errc:
	case <-ctx.Done():
		log.Info("shutting down")
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		serveErr = srv.Shutdown(sctx)
		cancel()
	}
	close(stopExpiry)
	// Only after every connection has finished: flush and fsync the AOF.
	if journal != nil {
		if err := journal.Close(); err != nil && serveErr == nil {
			serveErr = err
		}
	}
	return serveErr
}

func openAOF(st *store.Store, env *command.Env, path string, policy aof.Policy, rewriteMin int64, log *slog.Logger) (*aof.AOF, error) {
	start := time.Now()
	st.SetLoading(true)
	res, err := aof.Replay(path, env.Replayer())
	st.SetLoading(false)
	if err != nil {
		return nil, err
	}
	if res.Truncated {
		log.Warn("aof ended with a partial entry, probably from a crash during a write; truncated it",
			"path", path, "discarded_bytes", res.DiscardedBytes)
	}
	if res.Commands > 0 {
		log.Info("aof loaded", "commands", res.Commands, "keys", st.Len(), "took", time.Since(start).Round(time.Millisecond))
	}
	a, err := aof.Open(aof.Options{Path: path, Policy: policy, Shards: st.NumShards(), Logger: log, RewriteMinSize: rewriteMin})
	if err != nil {
		return nil, err
	}
	a.SetSource(st)
	st.SetPropagator(a.Append)
	return a, nil
}
