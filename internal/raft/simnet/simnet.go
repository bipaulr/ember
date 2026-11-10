// Package simnet is an in-memory network for running a whole Raft cluster
// inside one test process. Nodes can be cut off and rejoined at will, which
// real sockets do not allow.
package simnet

import (
	"context"
	"errors"
	"sync"
	"time"

	"ember/internal/raft"
)

// ErrUnreachable is returned when a message or its reply cannot be delivered.
var ErrUnreachable = errors.New("simnet: peer unreachable")

// Network connects the nodes registered with it. Every node starts connected.
type Network struct {
	mu       sync.Mutex
	handlers map[int]raft.RPCHandler
	offline  map[int]bool
	latency  time.Duration
}

// New creates an empty network.
func New() *Network {
	return &Network{handlers: make(map[int]raft.RPCHandler), offline: make(map[int]bool)}
}

// Register makes h the receiver of messages addressed to id.
func (n *Network) Register(id int, h raft.RPCHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Disconnect cuts id off: nothing it sends and nothing sent to it is delivered.
func (n *Network) Disconnect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.offline[id] = true
}

// Connect undoes Disconnect.
func (n *Network) Connect(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.offline, id)
}

// SetLatency makes every message take d to arrive. A request and its reply are
// two messages, so one RPC takes 2*d.
func (n *Network) SetLatency(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.latency = d
}

// travel waits for one message to cross the network, or for ctx to end.
func (n *Network) travel(ctx context.Context) error {
	n.mu.Lock()
	d := n.latency
	n.mu.Unlock()
	if d == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Endpoint returns the transport that node id uses to reach its peers.
func (n *Network) Endpoint(id int) raft.Transport {
	return &endpoint{net: n, id: id}
}

// handlerFor returns the receiver for a message, or nil if either end is cut off.
func (n *Network) handlerFor(from, to int) raft.RPCHandler {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.offline[from] || n.offline[to] {
		return nil
	}
	return n.handlers[to]
}

type endpoint struct {
	net *Network
	id  int
}

// call delivers one request and its reply. The link is checked twice because a
// node can be cut off while the receiver is working, in which case the request
// was processed but the sender never learns the answer. Raft must cope with that.
func call[Reply any](ctx context.Context, e *endpoint, to int, deliver func(h raft.RPCHandler) *Reply) (*Reply, error) {
	if err := e.net.travel(ctx); err != nil {
		return nil, err
	}
	h := e.net.handlerFor(e.id, to)
	if h == nil {
		return nil, ErrUnreachable
	}
	reply := deliver(h)
	if err := e.net.travel(ctx); err != nil {
		return nil, err // the request was processed; the caller gave up on the reply
	}
	if e.net.handlerFor(e.id, to) == nil {
		return nil, ErrUnreachable
	}
	return reply, nil
}

func (e *endpoint) RequestVote(ctx context.Context, to int, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return call(ctx, e, to, func(h raft.RPCHandler) *raft.RequestVoteReply { return h.HandleRequestVote(args) })
}

func (e *endpoint) AppendEntries(ctx context.Context, to int, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return call(ctx, e, to, func(h raft.RPCHandler) *raft.AppendEntriesReply { return h.HandleAppendEntries(args) })
}
