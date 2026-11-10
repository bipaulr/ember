package cluster_test

import (
	"reflect"
	"testing"

	"ember/internal/cluster"
)

func TestParsePeers(t *testing.T) {
	got, err := cluster.ParsePeers("1=10.0.0.1:7000/10.0.0.1:6380, 2=b:7000/b:6380,3=c:7000/c:6380")
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]cluster.Peer{
		1: {RaftAddr: "10.0.0.1:7000", ClientAddr: "10.0.0.1:6380"},
		2: {RaftAddr: "b:7000", ClientAddr: "b:6380"},
		3: {RaftAddr: "c:7000", ClientAddr: "c:6380"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestParsePeersRejectsBadInput(t *testing.T) {
	for name, spec := range map[string]string{
		"empty":             "",
		"no id":             "10.0.0.1:7000/10.0.0.1:6380",
		"id not a number":   "a=h:1/h:2",
		"id zero":           "0=h:1/h:2",
		"missing client":    "1=h:7000",
		"address w/o port":  "1=h/h:6380",
		"duplicate id":      "1=h:1/h:2,1=h:3/h:4",
		"duplicate address": "1=h:1/h:2,2=h:1/h:3",
	} {
		if _, err := cluster.ParsePeers(spec); err == nil {
			t.Errorf("%s: %q was accepted", name, spec)
		}
	}
}
