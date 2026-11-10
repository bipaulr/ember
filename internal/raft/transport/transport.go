// Package transport carries Raft RPCs between real processes over TCP, using
// the standard library's net/rpc with its default gob encoding.
//
// net/rpc was chosen because it is the least code that works: it frames
// messages, matches replies to requests, and lets many calls share one
// connection. What it lacks is deadlines, so each call here is bounded by its
// context, and a connection that fails or times out is closed and dialled
// again on the next call.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"ember/internal/raft"
)

const dialTimeout = time.Second

// TCP implements raft.Transport and serves incoming RPCs for one node.
type TCP struct {
	addrs map[int]string // node ID -> Raft address

	mu      sync.Mutex
	clients map[int]*rpc.Client   // one cached connection per peer
	ln      net.Listener          // set by Serve
	conns   map[net.Conn]struct{} // accepted connections, closed by Close
	closed  bool
}

// New creates a transport that can reach the given peers.
func New(addrs map[int]string) *TCP {
	return &TCP{addrs: addrs, clients: make(map[int]*rpc.Client), conns: make(map[net.Conn]struct{})}
}

// service adapts a raft.RPCHandler to the method shape net/rpc requires:
// func (t *T) Method(args *A, reply *R) error.
type service struct {
	h raft.RPCHandler
}

func (s *service) RequestVote(args *raft.RequestVoteArgs, reply *raft.RequestVoteReply) error {
	*reply = *s.h.HandleRequestVote(args)
	return nil
}

func (s *service) AppendEntries(args *raft.AppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	*reply = *s.h.HandleAppendEntries(args)
	return nil
}

// Serve accepts connections on ln and delivers their requests to h. It
// returns nil after Close.
func (t *TCP) Serve(ln net.Listener, h raft.RPCHandler) error {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &service{h: h}); err != nil {
		return err
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ln.Close()
	}
	t.ln = ln
	t.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if t.isClosed() {
				return nil
			}
			return err
		}
		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			_ = conn.Close()
			return nil
		}
		t.conns[conn] = struct{}{}
		t.mu.Unlock()
		go func() {
			srv.ServeConn(conn) // returns when the peer disconnects
			t.mu.Lock()
			delete(t.conns, conn)
			t.mu.Unlock()
		}()
	}
}

func (t *TCP) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// Close stops serving and closes every connection in both directions.
func (t *TCP) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil
	}
	t.closed = true
	var err error
	if t.ln != nil {
		err = t.ln.Close()
	}
	for conn := range t.conns {
		_ = conn.Close()
	}
	for _, c := range t.clients {
		_ = c.Close()
	}
	return err
}

// RequestVote implements raft.Transport.
func (t *TCP) RequestVote(ctx context.Context, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	reply := new(raft.RequestVoteReply)
	if err := t.call(ctx, to, "Raft.RequestVote", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// AppendEntries implements raft.Transport.
func (t *TCP) AppendEntries(ctx context.Context, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	reply := new(raft.AppendEntriesReply)
	if err := t.call(ctx, to, "Raft.AppendEntries", args, reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// call performs one RPC. reply must be a fresh value: gob does not transmit
// zero-valued fields, so decoding into a reused struct would leave old values
// (a previous Success: true, say) in place.
func (t *TCP) call(ctx context.Context, to int, method string, args, reply any) error {
	client, err := t.client(ctx, to)
	if err != nil {
		return err
	}
	call := client.Go(method, args, reply, make(chan *rpc.Call, 1))
	select {
	case <-call.Done:
		if call.Error != nil {
			t.discard(to, client)
		}
		return call.Error
	case <-ctx.Done():
		// The reply may still arrive, but nobody is waiting for it. Treat the
		// connection as broken so the next call starts clean.
		t.discard(to, client)
		return ctx.Err()
	}
}

// client returns the cached connection to a peer, dialling if there is none.
// The dial happens outside the lock so one unreachable peer cannot delay
// calls to the others.
func (t *TCP) client(ctx context.Context, to int) (*rpc.Client, error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil, errors.New("transport: closed")
	}
	if c := t.clients[to]; c != nil {
		t.mu.Unlock()
		return c, nil
	}
	addr, ok := t.addrs[to]
	t.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("transport: no address for node %d", to)
	}

	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	fresh := rpc.NewClient(conn)

	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		_ = fresh.Close()
		return nil, errors.New("transport: closed")
	}
	if existing := t.clients[to]; existing != nil {
		_ = fresh.Close() // another call connected first; use its connection
		return existing, nil
	}
	t.clients[to] = fresh
	return fresh, nil
}

// discard closes a connection and removes it from the cache, unless a newer
// connection has already replaced it.
func (t *TCP) discard(to int, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[to] == c {
		delete(t.clients, to)
	}
	t.mu.Unlock()
	_ = c.Close()
}
