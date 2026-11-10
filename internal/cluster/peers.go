package cluster

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Peer is one node's pair of addresses.
type Peer struct {
	RaftAddr   string // where other nodes send Raft RPCs
	ClientAddr string // where clients connect; used in MOVED redirects
}

// ParsePeers reads the --cluster flag: a comma-separated list with one item
// per node, this node included, in the form
//
//	id=raft-host:port/client-host:port
//
// Every node of a cluster must be started with the same list.
func ParsePeers(spec string) (map[int]Peer, error) {
	peers := make(map[int]Peer)
	seen := make(map[string]bool)
	for _, item := range strings.Split(spec, ",") {
		item = strings.TrimSpace(item)
		idText, addrs, ok := strings.Cut(item, "=")
		if !ok {
			return nil, fmt.Errorf("cluster member %q: want id=raft-host:port/client-host:port", item)
		}
		id, err := strconv.Atoi(idText)
		if err != nil || id < 1 {
			return nil, fmt.Errorf("cluster member %q: id must be a whole number, 1 or greater", item)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("cluster member id %d is listed twice", id)
		}
		raftAddr, clientAddr, ok := strings.Cut(addrs, "/")
		if !ok {
			return nil, fmt.Errorf("cluster member %q: want two addresses separated by /", item)
		}
		for _, addr := range []string{raftAddr, clientAddr} {
			if _, _, err := net.SplitHostPort(addr); err != nil {
				return nil, fmt.Errorf("cluster member %q: address %q: %v", item, addr, err)
			}
			if seen[addr] {
				return nil, fmt.Errorf("address %s is used twice", addr)
			}
			seen[addr] = true
		}
		peers[id] = Peer{RaftAddr: raftAddr, ClientAddr: clientAddr}
	}
	return peers, nil
}
