package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestEdgeLookupCacheCoalescesConcurrentDecode(t *testing.T) {
	cache := newEdgeLookupCache(8, 1<<20)
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	loader := func() (map[string][]graph.Edge, bool, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return map[string][]graph.Edge{
			"service:api": {{ID: "edge:1"}},
		}, true, nil
	}

	const workers = 8
	var wait sync.WaitGroup
	errors := make(chan error, workers)
	wait.Add(workers)
	for range workers {
		go func() {
			defer wait.Done()
			edges, ok, err := cache.load(
				context.Background(), "tenant/version/shard", loader,
			)
			if err == nil && (!ok || len(edges["service:api"]) != 1) {
				t.Errorf("loaded edges=%v ok=%v", edges, ok)
			}
			errors <- err
		}()
	}
	<-started
	close(release)
	wait.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatalf("load: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("decode calls=%d, want 1", calls.Load())
	}
}

func TestEdgeLookupCacheWaiterRetriesCanceledLeader(t *testing.T) {
	cache := newEdgeLookupCache(8, 1<<20)
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseLoads := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseLoads)
	var calls atomic.Int32
	loader := func(ctx context.Context) func() (
		map[string][]graph.Edge, bool, error,
	) {
		return func() (map[string][]graph.Edge, bool, error) {
			switch calls.Add(1) {
			case 1:
				close(firstStarted)
			case 2:
				close(secondStarted)
			}
			select {
			case <-release:
				return map[string][]graph.Edge{
					"service:api": {{ID: "edge:1"}},
				}, true, nil
			case <-ctx.Done():
				return nil, false, ctx.Err()
			}
		}
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := cache.load(
			leaderCtx, "tenant/version/shard", loader(leaderCtx),
		)
		leaderDone <- err
	}()
	<-firstStarted

	waiterDone := make(chan error, 1)
	go func() {
		edges, ok, err := cache.load(
			context.Background(),
			"tenant/version/shard",
			loader(context.Background()),
		)
		if err == nil && (!ok || len(edges["service:api"]) != 1) {
			err = errors.New("waiter returned invalid decoded edges")
		}
		waiterDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancelLeader()
	if err := <-leaderDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context canceled", err)
	}

	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatalf("decode calls = %d, want healthy waiter retry", calls.Load())
	}
	releaseLoads()
	if err := <-waiterDone; err != nil {
		t.Fatalf("waiter: %v", err)
	}
}
