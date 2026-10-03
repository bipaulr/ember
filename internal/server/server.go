// Package server accepts TCP connections and runs one goroutine per client.
package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"ember/internal/command"
	"ember/internal/pubsub"
	"ember/internal/resp"
)

// Config holds listener settings.
type Config struct {
	Addr       string
	MaxClients int
	Logger     *slog.Logger
	// SubQueueMessages and SubQueueBytes bound each pub/sub subscriber's
	// outbound queue; a subscriber that exceeds either is disconnected.
	// Zero selects the pubsub defaults.
	SubQueueMessages int
	SubQueueBytes    int64
}

// Server owns the listener and all live connections.
type Server struct {
	cfg Config
	env *command.Env
	log *slog.Logger

	mu      sync.Mutex
	ln      net.Listener
	conns   map[net.Conn]struct{}
	wg      sync.WaitGroup
	active  atomic.Int64
	closing atomic.Bool
}

// New creates a server around a shared environment.
func New(env *command.Env, cfg Config) *Server {
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if cfg.MaxClients <= 0 {
		cfg.MaxClients = 10000
	}
	env.MaxClients = cfg.MaxClients
	return &Server{cfg: cfg, env: env, log: cfg.Logger, conns: make(map[net.Conn]struct{})}
}

// Listen binds the configured address.
func (s *Server) Listen() (net.Listener, error) {
	ln, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return nil, err
	}
	if ta, ok := ln.Addr().(*net.TCPAddr); ok {
		s.env.Port = ta.Port
	}
	return ln, nil
}

// Serve accepts connections on ln until Shutdown is called, then returns nil.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	s.ln = ln
	s.mu.Unlock()
	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.closing.Load() || errors.Is(err, net.ErrClosed) {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				backoff = min(max(backoff*2, 5*time.Millisecond), time.Second)
				time.Sleep(backoff)
				continue
			}
			return err
		}
		backoff = 0
		if int(s.active.Load()) >= s.cfg.MaxClients {
			s.env.Rejected.Add(1)
			_, _ = conn.Write([]byte("-ERR max number of clients reached\r\n"))
			_ = conn.Close()
			continue
		}
		s.mu.Lock()
		if s.closing.Load() {
			s.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		s.conns[conn] = struct{}{}
		s.active.Add(1)
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(conn)
	}
}

// Shutdown stops accepting, lets commands that are already executing finish,
// closes idle connections, and waits for every connection goroutine. If ctx
// expires first, remaining connections are closed forcibly.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing.Store(true)
	if s.ln != nil {
		_ = s.ln.Close()
	}
	// A past read deadline wakes every connection blocked in Read. A command
	// that is mid-execution is not interrupted; the loop exits after it.
	for c := range s.conns {
		_ = c.SetReadDeadline(time.Now())
	}
	s.mu.Unlock()

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		<-done
		return ctx.Err()
	}
}

// flushReader flushes pending replies right before the connection would block
// waiting for more input. Because bufio only calls Read when its buffer is
// empty, every pipelined command already received is answered before this
// point, and all of their replies leave in one write.
//
// It also owns the connection's write lock. The connection goroutine holds mu
// while it executes commands and releases it only while blocked in Read, so the
// pub/sub pump can write pushed messages exactly when no reply is half-written.
type flushReader struct {
	conn net.Conn
	w    *resp.Writer
	mu   *sync.Mutex
}

func (f *flushReader) Read(p []byte) (int, error) {
	if f.w.Buffered() > 0 {
		if err := f.w.Flush(); err != nil {
			return 0, err
		}
	}
	f.mu.Unlock()
	n, err := f.conn.Read(p)
	f.mu.Lock()
	return n, err
}

// pumpWriteTimeout bounds how long a pushed message may block on a socket.
const pumpWriteTimeout = 10 * time.Second

// pump writes queued pub/sub messages to the socket. It is the only writer for
// pushed data and runs until the subscriber is closed.
func pump(conn net.Conn, mu *sync.Mutex, sub *pubsub.Subscriber) {
	for {
		select {
		case <-sub.Done():
			return
		case msg := <-sub.C:
			mu.Lock()
			_ = conn.SetWriteDeadline(time.Now().Add(pumpWriteTimeout))
			_, err := conn.Write(msg)
			_ = conn.SetWriteDeadline(time.Time{})
			mu.Unlock()
			sub.Release(len(msg))
			if err != nil {
				_ = conn.Close()
				return
			}
		}
	}
}

// connWriter is the io.Writer behind a client's reply buffer. Before any bytes
// leave for the client, it commits the AOF if this client has run write
// commands. Under appendfsync=always that means a reply is never sent for a
// write that is not yet on disk; commits from concurrent clients share one fsync.
type connWriter struct {
	conn net.Conn
	env  *command.Env
	cl   *command.Client
}

func (cw *connWriter) Write(p []byte) (int, error) {
	if cw.cl.Dirty {
		cw.cl.Dirty = false
		if cw.env.AOF != nil {
			if err := cw.env.AOF.Commit(); err != nil {
				return 0, err
			}
		}
	}
	return cw.conn.Write(p)
}

func (s *Server) serveConn(conn net.Conn) {
	cl := s.env.NewClient(conn.RemoteAddr().String())
	w := resp.NewWriter(&connWriter{conn: conn, env: s.env, cl: cl})
	var wmu sync.Mutex
	var pumps sync.WaitGroup
	r := resp.NewReader(&flushReader{conn: conn, w: w, mu: &wmu})
	c := &command.Ctx{W: w, Store: s.env.Store, Client: cl, Env: s.env}
	cl.NewSub = func() *pubsub.Subscriber {
		sub := pubsub.NewSubscriber(s.cfg.SubQueueMessages, s.cfg.SubQueueBytes, func() {
			s.env.Hub.Overflows.Add(1)
			s.log.Warn("disconnecting slow subscriber: queue full", "client", cl.Addr, "id", cl.ID)
			_ = conn.Close()
		})
		pumps.Add(1)
		go func() { defer pumps.Done(); pump(conn, &wmu, sub) }()
		return sub
	}
	wmu.Lock() // held while running commands; see flushReader
	defer func() {
		_ = conn.Close()
		wmu.Unlock()
		if cl.Sub != nil {
			s.env.Hub.RemoveAll(cl.Sub)
			cl.Sub.Close()
		}
		pumps.Wait()
		s.env.RemoveClient(cl)
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.active.Add(-1)
		s.wg.Done()
	}()

	for {
		args, err := r.ReadCommand()
		if err != nil {
			if resp.IsProtocolError(err) {
				w.WriteError("ERR " + err.Error())
				_ = w.Flush()
				s.log.Debug("protocol error", "client", cl.Addr, "err", err)
			} else if !isClosedErr(err) {
				s.log.Debug("read error", "client", cl.Addr, "err", err)
			}
			_ = w.Flush()
			return
		}
		if len(args) == 0 {
			continue
		}
		c.Args = args
		s.env.Table.Execute(c)
		if cl.Closing || s.closing.Load() {
			_ = w.Flush()
			return
		}
	}
}

func isClosedErr(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, os.ErrDeadlineExceeded)
}
