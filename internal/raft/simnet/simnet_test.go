package simnet_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"ember/internal/raft"
	"ember/internal/raft/simnet"
)

// recorder is a receiver that counts what reaches it and can run a hook while
// it is "processing" a request.
type recorder struct {
	votes, appends int
	during         func()
}

func (r *recorder) HandleRequestVote(args *raft.RequestVoteArgs) *raft.RequestVoteReply {
	r.votes++
	if r.during != nil {
		r.during()
	}
	return &raft.RequestVoteReply{Term: args.Term, VoteGranted: true}
}

func (r *recorder) HandleAppendEntries(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply {
	r.appends++
	return &raft.AppendEntriesReply{Term: args.Term, Success: true}
}

func TestDeliversRequestsAndReplies(t *testing.T) {
	net := simnet.New()
	rec := &recorder{}
	net.Register(2, rec)
	from1 := net.Endpoint(1)

	vote, err := from1.RequestVote(context.Background(), 2, &raft.RequestVoteArgs{Term: 3})
	if err != nil || !vote.VoteGranted || vote.Term != 3 {
		t.Fatalf("RequestVote = %+v, %v", vote, err)
	}
	app, err := from1.AppendEntries(context.Background(), 2, &raft.AppendEntriesArgs{Term: 3})
	if err != nil || !app.Success {
		t.Fatalf("AppendEntries = %+v, %v", app, err)
	}
	if rec.votes != 1 || rec.appends != 1 {
		t.Fatalf("receiver saw %d votes and %d appends, want 1 and 1", rec.votes, rec.appends)
	}
}

func TestDisconnectedNodeNeitherSendsNorReceives(t *testing.T) {
	net := simnet.New()
	rec1, rec2 := &recorder{}, &recorder{}
	net.Register(1, rec1)
	net.Register(2, rec2)

	net.Disconnect(2)
	if _, err := net.Endpoint(1).AppendEntries(context.Background(), 2, &raft.AppendEntriesArgs{}); !errors.Is(err, simnet.ErrUnreachable) {
		t.Fatalf("send to a disconnected node: err = %v", err)
	}
	if _, err := net.Endpoint(2).AppendEntries(context.Background(), 1, &raft.AppendEntriesArgs{}); !errors.Is(err, simnet.ErrUnreachable) {
		t.Fatalf("send from a disconnected node: err = %v", err)
	}
	if rec1.appends != 0 || rec2.appends != 0 {
		t.Fatal("a message crossed a cut link")
	}

	net.Connect(2)
	if _, err := net.Endpoint(1).AppendEntries(context.Background(), 2, &raft.AppendEntriesArgs{}); err != nil {
		t.Fatalf("send after reconnect: %v", err)
	}
}

func TestUnknownPeerIsUnreachable(t *testing.T) {
	net := simnet.New()
	if _, err := net.Endpoint(1).RequestVote(context.Background(), 9, &raft.RequestVoteArgs{}); !errors.Is(err, simnet.ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", err)
	}
}

func TestReplyIsLostIfLinkIsCutWhileProcessing(t *testing.T) {
	net := simnet.New()
	rec := &recorder{}
	rec.during = func() { net.Disconnect(2) }
	net.Register(2, rec)

	_, err := net.Endpoint(1).RequestVote(context.Background(), 2, &raft.RequestVoteArgs{})
	if !errors.Is(err, simnet.ErrUnreachable) {
		t.Fatalf("err = %v, want ErrUnreachable", err)
	}
	if rec.votes != 1 {
		t.Fatal("the request should have been processed even though its reply was lost")
	}
}

func TestCancelledContextSendsNothing(t *testing.T) {
	net := simnet.New()
	rec := &recorder{}
	net.Register(2, rec)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := net.Endpoint(1).RequestVote(ctx, 2, &raft.RequestVoteArgs{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if rec.votes != 0 {
		t.Fatal("a request was delivered after its context was cancelled")
	}
}

func TestLatencyDelaysEachMessage(t *testing.T) {
	net := simnet.New()
	net.Register(2, &recorder{})
	const latency = 30 * time.Millisecond
	net.SetLatency(latency)

	start := time.Now()
	if _, err := net.Endpoint(1).RequestVote(context.Background(), 2, &raft.RequestVoteArgs{}); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 2*latency {
		t.Fatalf("RPC took %v, want at least %v (request plus reply)", took, 2*latency)
	}
}

func TestCallerCanGiveUpWhileMessageIsInFlight(t *testing.T) {
	net := simnet.New()
	rec := &recorder{}
	net.Register(2, rec)
	net.SetLatency(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	if _, err := net.Endpoint(1).RequestVote(ctx, 2, &raft.RequestVoteArgs{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if rec.votes != 0 {
		t.Fatal("the request arrived although the caller gave up first")
	}
}

func TestPartitionSeparatesGroupsUntilHealed(t *testing.T) {
	net := simnet.New()
	for id := 1; id <= 5; id++ {
		net.Register(id, &recorder{})
	}
	send := func(from, to int) error {
		_, err := net.Endpoint(from).AppendEntries(context.Background(), to, &raft.AppendEntriesArgs{})
		return err
	}

	net.Partition([]int{1, 2}) // 3, 4 and 5 form the other side
	if err := send(1, 2); err != nil {
		t.Fatalf("same side, listed group: %v", err)
	}
	if err := send(3, 5); err != nil {
		t.Fatalf("same side, unlisted nodes: %v", err)
	}
	if err := send(1, 3); !errors.Is(err, simnet.ErrUnreachable) {
		t.Fatalf("across the partition: err = %v", err)
	}
	if err := send(4, 2); !errors.Is(err, simnet.ErrUnreachable) {
		t.Fatalf("across the partition, other direction: err = %v", err)
	}

	net.Heal()
	if err := send(1, 3); err != nil {
		t.Fatalf("after heal: %v", err)
	}
}
