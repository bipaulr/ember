package raft

import "slices"

// applyLoop hands committed entries to the state machine, in order, one
// goroutine for the node's whole life. It sleeps until commitIndex moves.
//
// The state machine is called without mu held, so a slow Apply never delays
// heartbeats or votes.
func (n *Node) applyLoop() {
	for {
		n.mu.Lock()
		for n.lastApplied >= n.commitIndex && !n.stopped {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		// Committed entries are never changed or removed, so a copy taken here
		// stays correct after the lock is released.
		batch := slices.Clone(n.entries[n.lastApplied+1 : n.commitIndex+1])
		n.mu.Unlock()

		if n.cfg.StateMachine != nil {
			for _, e := range batch {
				n.cfg.StateMachine.Apply(e)
			}
		}

		n.mu.Lock()
		n.lastApplied = batch[len(batch)-1].Index
		n.mu.Unlock()
	}
}
