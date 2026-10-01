package sharding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestLeaderCacheInvalidationDoesNotReplayMutations(t *testing.T) {
	var leader, probes, mutations atomic.Int64
	leader.Store(1)
	shard := Shard{ID: "data", ClusterID: "data", Peers: map[uint64]string{}}
	for id := uint64(1); id <= 3; id++ {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/raft/status" {
				probes.Add(1)
				json.NewEncoder(w).Encode(map[string]any{"leader_id": leader.Load()})
				return
			}
			mutations.Add(1)
			if int64(id) != leader.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		}))
		t.Cleanup(server.Close)
		shard.Peers[id] = server.URL
	}
	client := NewClient("test")
	defer client.HTTP.CloseIdleConnections()
	for range 100 {
		if _, err := client.Leader(context.Background(), shard); err != nil {
			t.Fatal(err)
		}
	}
	if probes.Load() != 1 {
		t.Fatalf("cache still probes for each request: %d", probes.Load())
	}
	leader.Store(2)
	response, err := client.Do(context.Background(), shard, http.MethodPost, "/write", map[string]any{"id": "one"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || mutations.Load() != 1 {
		t.Fatalf("ambiguous mutation was replayed: status=%d attempts=%d", response.StatusCode, mutations.Load())
	}
	response, err = client.Do(context.Background(), shard, http.MethodPost, "/write", map[string]any{"id": "one"})
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || mutations.Load() != 2 || probes.Load() != 2 {
		t.Fatalf("explicit retry did not discover the new leader: status=%d attempts=%d probes=%d", response.StatusCode, mutations.Load(), probes.Load())
	}
}
