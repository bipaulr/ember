// Package cluster joins Ember's command layer to Raft. It plays two roles:
//
//   - For the command layer it is the ClusterNode: a write command is not executed
//     directly but submitted here, replicated, and answered once committed.
//   - For Raft it is the StateMachine: each committed entry is decoded back into
//     a command and executed against the local store.
//
// Every node executes the same commands in the same order, so every store
// ends up in the same state. The one input a command could read that is not in
// the log is the time, so the leader's clock reading is put into each entry
// and the store's clock is driven from the log (see Clock).
package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"time"

	"ember/internal/command"
	"ember/internal/raft"
	"ember/internal/resp"
)

// Raft is the part of a Raft node the cluster layer uses.
type Raft interface {
	Propose(data []byte) (index, term uint64, ok bool)
	Status() raft.Status
}

// Defaults for Config.
const (
	DefaultProposeTimeout = 5 * time.Second
	// tickInterval bounds how stale the log clock gets on an idle cluster,
	// and so how late a key can expire.
	tickInterval = 100 * time.Millisecond
)

// Config configures a Cluster.
type Config struct {
	ID    int
	Env   *command.Env
	Clock *Clock // must be the clock the Env's store was created with
	// ClientAddrs maps each node ID to the address clients use to reach it,
	// for redirecting writes to the leader.
	ClientAddrs map[int]string
	// ProposeTimeout is how long a write waits to be committed before the
	// client is told the outcome is unknown. Zero selects DefaultProposeTimeout.
	ProposeTimeout time.Duration
	// WallClock returns Unix milliseconds. Nil uses the system clock.
	WallClock func() int64
}

// Cluster is one node's bridge between commands and Raft.
type Cluster struct {
	cfg  Config
	raft Raft

	// mu guards waiters. It is held across raft.Propose so that the apply
	// goroutine cannot reach an index before its waiter is registered.
	mu      sync.Mutex
	waiters map[uint64]*waiter // log index -> client waiting for that entry

	// Used only by the apply goroutine.
	applyBuf bytes.Buffer
	applyCtx *command.Ctx

	stop chan struct{}
	wg   sync.WaitGroup
}

// waiter is a client blocked in Propose.
type waiter struct {
	term uint64 // term of the entry the client proposed
	done chan result
}

type result struct {
	reply []byte
	err   error
}

// New creates the bridge and installs it as env's proposer. Call Attach before
// the Raft node starts.
func New(cfg Config) *Cluster {
	if cfg.ProposeTimeout <= 0 {
		cfg.ProposeTimeout = DefaultProposeTimeout
	}
	if cfg.WallClock == nil {
		cfg.WallClock = func() int64 { return time.Now().UnixMilli() }
	}
	c := &Cluster{cfg: cfg, waiters: make(map[uint64]*waiter), stop: make(chan struct{})}
	c.applyCtx = &command.Ctx{
		W:        resp.NewWriter(&c.applyBuf),
		Store:    cfg.Env.Store,
		Client:   &command.Client{},
		Env:      cfg.Env,
		Applying: true,
	}
	cfg.Env.Cluster = c
	return c
}

// Attach connects the Raft node. The node needs the Cluster as its state
// machine and the Cluster needs the node, so one of them is set afterwards.
func (c *Cluster) Attach(r Raft) { c.raft = r }

// Start runs the background clock tick. Stop ends it.
func (c *Cluster) Start() {
	c.wg.Add(1)
	go c.tickLoop()
}

// Stop ends background work and releases clients still waiting on a write.
func (c *Cluster) Stop() {
	select {
	case <-c.stop:
		return
	default:
	}
	close(c.stop)
	c.wg.Wait()
}

var (
	errTimeout  = errors.New("ERR write timed out waiting for a majority of nodes; it may still be applied later")
	errStopping = errors.New("ERR server is shutting down; the write may still be applied")
)

// Propose replicates a write command and returns its RESP-encoded reply once
// the command has been committed and executed on this node. The returned
// error's text is a complete RESP error message.
func (c *Cluster) Propose(args [][]byte) ([]byte, error) {
	data := encodeEntry(c.cfg.WallClock(), args)

	c.mu.Lock()
	index, term, ok := c.raft.Propose(data)
	if !ok {
		c.mu.Unlock()
		return nil, c.notLeaderError()
	}
	w := &waiter{term: term, done: make(chan result, 1)}
	if old := c.waiters[index]; old != nil {
		// This node proposed at this index before, lost leadership, had that
		// entry removed from its log, and is leader again. The earlier entry
		// is gone for good.
		old.done <- result{err: c.notLeaderError()}
	}
	c.waiters[index] = w
	c.mu.Unlock()

	timeout := time.NewTimer(c.cfg.ProposeTimeout)
	defer timeout.Stop()
	select {
	case r := <-w.done:
		return r.reply, r.err
	case <-timeout.C:
		c.forget(index, w)
		return nil, errTimeout
	case <-c.stop:
		c.forget(index, w)
		return nil, errStopping
	}
}

func (c *Cluster) forget(index uint64, w *waiter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waiters[index] == w {
		delete(c.waiters, index)
	}
}

// notLeaderError tells the client where to send its write instead. The shape
// "MOVED <slot> <host:port>" is what Redis Cluster uses, so redis-cli -c
// follows it on its own. Ember has no slots; the 0 is a placeholder.
func (c *Cluster) notLeaderError() error {
	leader := c.raft.Status().LeaderID
	addr, known := c.cfg.ClientAddrs[leader]
	if leader == raft.None || leader == c.cfg.ID || !known {
		return errors.New("CLUSTERDOWN no leader is known right now; retry shortly")
	}
	return fmt.Errorf("MOVED 0 %s", addr)
}

// Info implements command.ClusterNode.
func (c *Cluster) Info() command.ClusterInfo {
	st := c.raft.Status()
	return command.ClusterInfo{
		NodeID:       st.ID,
		Role:         st.Role.String(),
		Term:         st.Term,
		LeaderID:     st.LeaderID,
		LeaderAddr:   c.cfg.ClientAddrs[st.LeaderID],
		LastLogIndex: st.LastLogIndex,
		CommitIndex:  st.CommitIndex,
		LastApplied:  st.LastApplied,
	}
}

// Apply executes one committed entry. Raft calls it from a single goroutine,
// in log order, on every node.
func (c *Cluster) Apply(e raft.Entry) {
	var reply []byte
	if !e.Noop { // a new leader's empty entry changes nothing, but may settle a waiter below
		reply = c.run(e)
	}

	c.mu.Lock()
	w := c.waiters[e.Index]
	delete(c.waiters, e.Index)
	c.mu.Unlock()
	if w == nil {
		return // proposed by another node, or the client gave up waiting
	}
	if w.term != e.Term {
		// A different leader's entry was committed at the index where our
		// client's entry used to be: ours was overwritten and never ran.
		w.done <- result{err: c.notLeaderError()}
		return
	}
	w.done <- result{reply: reply}
}

// run decodes an entry, moves the clock to its timestamp and executes its command.
func (c *Cluster) run(e raft.Entry) []byte {
	timestamp, args, err := decodeEntry(e.Data)
	if err != nil {
		// Entries are produced by encodeEntry on a leader, so this means the
		// log is damaged. Continuing would let nodes drift apart silently.
		panic(fmt.Sprintf("cluster: undecodable log entry %d: %v", e.Index, err))
	}
	// Advance time first: the command must see the moment it was proposed at.
	c.cfg.Clock.advance(timestamp)
	if len(args) == 0 {
		return nil // a tick: it only moves the clock
	}
	return c.execute(args)
}

// execute runs a command against the local store and returns its reply.
func (c *Cluster) execute(args [][]byte) []byte {
	c.applyBuf.Reset()
	c.applyCtx.Args = args
	c.cfg.Env.Table.Execute(c.applyCtx)
	_ = c.applyCtx.W.Flush() // writes to a bytes.Buffer cannot fail
	return bytes.Clone(c.applyBuf.Bytes())
}

// tickLoop keeps time moving when no writes arrive. Without it, a key with a
// TTL would never expire on an idle cluster, because the log clock only
// advances when an entry is applied. Only the leader ticks, and only while
// some key has a TTL.
func (c *Cluster) tickLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.stop:
			return
		case <-ticker.C:
		}
		if c.raft.Status().Role != raft.Leader || c.cfg.Env.Store.TTLKeys() == 0 {
			continue
		}
		now := c.cfg.WallClock()
		if now-c.cfg.Clock.Now() >= tickInterval.Milliseconds() {
			c.raft.Propose(encodeEntry(now, nil))
		}
	}
}
