package integration

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestPubSubViaGoRedis(t *testing.T) {
	c, _ := inProc(t)

	sub := c.Subscribe(bg, "news")
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(bg); err != nil { // subscription confirmation
		t.Fatal(err)
	}
	psub := c.PSubscribe(bg, "alerts.*")
	defer func() { _ = psub.Close() }()
	if _, err := psub.Receive(bg); err != nil {
		t.Fatal(err)
	}

	if n, err := c.Publish(bg, "news", "hello").Result(); err != nil || n != 1 {
		t.Fatalf("receivers=%d err=%v", n, err)
	}
	ctx, cancel := context.WithTimeout(bg, 3*time.Second)
	defer cancel()
	m, err := sub.ReceiveMessage(ctx)
	if err != nil || m.Channel != "news" || m.Payload != "hello" {
		t.Fatalf("got %+v, %v", m, err)
	}
	// Still one receiver: the alerts.* pattern does not match "news".
	if n, _ := c.Publish(bg, "news", "again").Result(); n != 1 {
		t.Fatalf("receivers=%d", n)
	}
}

func TestPubSubMessagesInOrder(t *testing.T) {
	c, _ := inProc(t)
	sub := c.Subscribe(bg, "ch")
	defer func() { _ = sub.Close() }()
	if _, err := sub.Receive(bg); err != nil {
		t.Fatal(err)
	}
	const n = 500
	go func() {
		for i := 0; i < n; i++ {
			c.Publish(bg, "ch", fmt.Sprint(i))
		}
	}()
	ch := sub.Channel()
	for i := 0; i < n; i++ {
		select {
		case m := <-ch:
			if m.Payload != fmt.Sprint(i) || m.Channel != "ch" {
				t.Fatalf("message %d: %+v", i, m)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

func TestPatternAndPingViaGoRedis(t *testing.T) {
	c, _ := inProc(t)
	psub := c.PSubscribe(bg, "alerts.*")
	defer func() { _ = psub.Close() }()
	if _, err := psub.Receive(bg); err != nil {
		t.Fatal(err)
	}
	// PING while subscribed uses the array form of PONG.
	if err := psub.Ping(bg, "are you there"); err != nil {
		t.Fatal(err)
	}
	if n, _ := c.Publish(bg, "alerts.fire", "evacuate").Result(); n != 1 {
		t.Fatalf("receivers=%d", n)
	}
	ch := psub.Channel()
	for {
		select {
		case m := <-ch:
			if m.Pattern != "alerts.*" || m.Channel != "alerts.fire" || m.Payload != "evacuate" {
				t.Fatalf("%+v", m)
			}
			return
		case <-time.After(3 * time.Second):
			t.Fatal("no pmessage")
		}
	}
}

func TestUnsubscribeViaGoRedis(t *testing.T) {
	c, env := inProc(t)
	sub := c.Subscribe(bg, "a", "b")
	if _, err := sub.Receive(bg); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Receive(bg); err != nil {
		t.Fatal(err)
	}
	if err := sub.Unsubscribe(bg, "a"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for env.Hub.NumSub("a") != 0 {
		if time.Now().After(deadline) {
			t.Fatal("still subscribed to a")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if n, _ := c.Publish(bg, "b", "x").Result(); n != 1 {
		t.Fatal(n)
	}
	_ = sub.Close()
	for env.Hub.NumChannels() != 0 {
		if time.Now().After(deadline.Add(2 * time.Second)) {
			t.Fatal("subscription not cleaned up after Close")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
