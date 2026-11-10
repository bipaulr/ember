package transport_test

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"ember/internal/raft"
	"ember/internal/raft/transport"
)

// fakeNode records what it receives and answers with fixed replies.
type fakeNode struct {
	gotVote   chan *raft.RequestVoteArgs
	gotAppend chan *raft.AppendEntriesArgs
	block     chan struct{} // if non-nil, handlers wait for it to be closed
}

func newFakeNode() *fakeNode {
	return &fakeNode{gotVote: make(chan *raft.RequestVoteArgs, 8), gotAppend: make(chan *raft.AppendEntriesArgs, 8)}
}

func (f *fakeNode) HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply {
	if f.block != nil {
		<-f.block
	}
	f.gotVote <- args
	return &raft.RequestVoteReply{Term: args.Term + 1, VoteGranted: true}
}

func (f *fakeNode) HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	f.gotAppend <- args
	return &raft.AppendEntriesReply{Term: args.Term, Success: false, ConflictIndex: 7}
}

// listen starts a transport serving h on a free local port and returns its address.
func listen(t *testing.T, h raft.RPCHandler) (addr string, tr *transport.TCP) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	tr = transport.New(nil)
	go func() { _ = tr.Serve(ln, h) }()
	t.Cleanup(func() { _ = tr.Close() })
	return ln.Addr().String(), tr
}

func ctxWithTimeout(t *testing.T, d time.Duration) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

func TestRequestVoteRoundTrip(t *testing.T) {
	node := newFakeNode()
	addr, _ := listen(t, node)
	client := transport.New(map[int]string{2: addr})
	defer func() { _ = client.Close() }()

	args := &raft.RequestVoteArgs{Term: 4, CandidateID: 1, LastLogIndex: 9, LastLogTerm: 3}
	reply, err := client.RequestVote(ctxWithTimeout(t, 5*time.Second), 2, args)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Term != 5 || !reply.VoteGranted {
		t.Fatalf("reply = %+v", reply)
	}
	if got := <-node.gotVote; !reflect.DeepEqual(got, args) {
		t.Fatalf("receiver saw %+v, want %+v", got, args)
	}
}

func TestAppendEntriesCarriesEntriesAndFalseReplyFields(t *testing.T) {
	node := newFakeNode()
	addr, _ := listen(t, node)
	client := transport.New(map[int]string{2: addr})
	defer func() { _ = client.Close() }()

	args := &raft.AppendEntriesArgs{
		Term: 2, LeaderID: 1, PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 3,
		Entries: []raft.Entry{{Index: 4, Term: 2, Data: []byte("SET a 1")}, {Index: 5, Term: 2, Data: []byte{0, 255}}},
	}
	reply, err := client.AppendEntries(ctxWithTimeout(t, 5*time.Second), 2, args)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Success || reply.ConflictIndex != 7 || reply.Term != 2 {
		t.Fatalf("reply = %+v", reply)
	}
	if got := <-node.gotAppend; !reflect.DeepEqual(got, args) {
		t.Fatalf("receiver saw %+v, want %+v", got, args)
	}
}

func TestCallToUnknownOrDeadPeerFails(t *testing.T) {
	// Reserve a port, then free it, so nothing is listening there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()

	client := transport.New(map[int]string{2: dead})
	defer func() { _ = client.Close() }()
	ctx := ctxWithTimeout(t, 2*time.Second)

	if _, err := client.RequestVote(ctx, 2, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("call to a peer that is not listening succeeded")
	}
	if _, err := client.RequestVote(ctx, 9, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("call to a peer with no configured address succeeded")
	}
}

func TestCallReturnsWhenContextEnds(t *testing.T) {
	node := newFakeNode()
	node.block = make(chan struct{})
	defer close(node.block)
	addr, _ := listen(t, node)
	client := transport.New(map[int]string{2: addr})
	defer func() { _ = client.Close() }()

	start := time.Now()
	_, err := client.RequestVote(ctxWithTimeout(t, 50*time.Millisecond), 2, &raft.RequestVoteArgs{})
	if err == nil {
		t.Fatal("call succeeded although the receiver never answered")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("call took %v; it should stop when its context does", took)
	}
}

func TestReconnectsAfterPeerRestarts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	first := transport.New(nil)
	go func() { _ = first.Serve(ln, newFakeNode()) }()

	client := transport.New(map[int]string{2: addr})
	defer func() { _ = client.Close() }()
	if _, err := client.RequestVote(ctxWithTimeout(t, 5*time.Second), 2, &raft.RequestVoteArgs{}); err != nil {
		t.Fatal(err)
	}

	// The peer goes away: calls fail.
	_ = first.Close()
	if _, err := client.RequestVote(ctxWithTimeout(t, time.Second), 2, &raft.RequestVoteArgs{}); err == nil {
		t.Fatal("call succeeded after the peer shut down")
	}

	// It comes back on the same address: the next call must dial again.
	ln2, err := net.Listen("tcp", addr)
	if err != nil {
		t.Skipf("could not rebind %s: %v", addr, err)
	}
	second := transport.New(nil)
	go func() { _ = second.Serve(ln2, newFakeNode()) }()
	defer func() { _ = second.Close() }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := client.RequestVote(ctxWithTimeout(t, time.Second), 2, &raft.RequestVoteArgs{})
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never reconnected: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
