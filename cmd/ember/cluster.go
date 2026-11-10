package main

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"ember/internal/cluster"
	"ember/internal/command"
	"ember/internal/raft"
	"ember/internal/raft/transport"
	"ember/internal/store"
)

// clusterOptions are the flags that turn a single-node Ember into a member of
// a Raft cluster.
type clusterOptions struct {
	nodeID            int
	members           string // the --cluster flag; see cluster.ParsePeers
	electionTimeout   time.Duration
	heartbeatInterval time.Duration
}

// clusterNode is everything that runs in cluster mode on top of the normal server.
type clusterNode struct {
	env       *command.Env
	raft      *raft.Node
	bridge    *cluster.Cluster
	transport *transport.TCP
}

// startCluster builds the store, the Raft node and the glue between them, and
// starts taking part in the cluster. The client listener is started by the
// caller afterwards.
func startCluster(opt clusterOptions, shards int, log *slog.Logger) (*clusterNode, error) {
	members, err := cluster.ParsePeers(opt.members)
	if err != nil {
		return nil, err
	}
	self, ok := members[opt.nodeID]
	if !ok {
		return nil, fmt.Errorf("--node-id %d is not listed in --cluster", opt.nodeID)
	}
	if opt.heartbeatInterval <= 0 || opt.electionTimeout < 3*opt.heartbeatInterval {
		return nil, fmt.Errorf("--election-timeout (%v) must be at least 3 times --heartbeat-interval (%v)",
			opt.electionTimeout, opt.heartbeatInterval)
	}

	var peerIDs []int
	raftAddrs := make(map[int]string)
	clientAddrs := make(map[int]string)
	for id, m := range members {
		clientAddrs[id] = m.ClientAddr
		if id != opt.nodeID {
			peerIDs = append(peerIDs, id)
			raftAddrs[id] = m.RaftAddr
		}
	}

	// In cluster mode the store reads time from the replicated log, not from
	// this machine; see cluster.Clock.
	clock := &cluster.Clock{}
	env := command.NewEnv(store.NewWithClock(shards, clock.Now))
	bridge := cluster.New(cluster.Config{ID: opt.nodeID, Env: env, Clock: clock, ClientAddrs: clientAddrs})

	tr := transport.New(raftAddrs)
	node := raft.New(raft.Config{
		ID:                opt.nodeID,
		Peers:             peerIDs,
		Transport:         tr,
		StateMachine:      bridge,
		ElectionTimeout:   opt.electionTimeout,
		HeartbeatInterval: opt.heartbeatInterval,
		Logger:            log,
	})
	bridge.Attach(node)

	ln, err := net.Listen("tcp", self.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("listening for raft peers: %w", err)
	}
	go func() {
		if err := tr.Serve(ln, node); err != nil {
			log.Error("raft transport stopped", "err", err)
		}
	}()
	node.Start()
	bridge.Start()
	log.Info("cluster mode", "node", opt.nodeID, "raft_listen", self.RaftAddr, "members", len(members))
	return &clusterNode{env: env, raft: node, bridge: bridge, transport: tr}, nil
}

// stop leaves the cluster. Clients still waiting on a write are released first.
func (c *clusterNode) stop() {
	c.bridge.Stop()
	c.raft.Stop()
	_ = c.transport.Close()
}
