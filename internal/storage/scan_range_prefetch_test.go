package storage

import (
	"context"
	"fmt"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"sync"
	"testing"
	"time"
)

type pipelineScanStore struct {
	ObjectStore
	first, second         string
	nextStarted           chan struct{}
	firstStarted          chan struct{}
	firstOnce, nextOnce   sync.Once
	mu                    sync.Mutex
	calls                 map[string]int
	blockNext, blockFirst bool
	active, maxActive     int
}

func (s *pipelineScanStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.mu.Lock()
	s.calls[key]++
	s.active++
	s.maxActive = max(s.active, s.maxActive)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	if key == s.second {
		s.nextOnce.Do(func() { close(s.nextStarted) })
		if s.blockNext {
			<-ctx.Done()
			return nil, ObjectMeta{}, ctx.Err()
		}
	}
	if key == s.first {
		s.firstOnce.Do(func() { close(s.firstStarted) })
		select {
		case <-s.nextStarted:
		case <-ctx.Done():
			return nil, ObjectMeta{}, ctx.Err()
		}
		if s.blockFirst {
			<-ctx.Done()
			return nil, ObjectMeta{}, ctx.Err()
		}
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func TestRangeScanPrefetchIsBoundedAndCanceled(t *testing.T) {
	for _, edge := range []bool{false, true} {
		for _, mode := range []string{"complete", "early-stop", "cancel"} {
			t.Run(fmt.Sprintf("edge=%v/%s", edge, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				base := NewMemoryStore()
				writer := NewTenantStore(base, "test")
				catalog := IndexCatalog{TenantID: "tenant-a", Version: 1}
				var objects []IndexObject
				if edge {
					var shards []EdgeShardData
					for i := 0; i < 3; i++ {
						lower := ""
						if i > 0 {
							lower = fmt.Sprintf("edge:%d", i*2)
						}
						shards = append(shards, EdgeShardData{Shard: "00", RelationType: "links", Version: 1, pageRole: rangeRole(lower), Edges: []graph.Edge{{ID: fmt.Sprintf("edge:%d", i*2), Type: "links", From: "host:a", To: "host:b"}, {ID: fmt.Sprintf("edge:%d", i*2+1), Type: "links", From: "host:a", To: "host:b"}}})
					}
					shards, err := prepareBoundedEdgeShards(ctx, "tenant-a", shards)
					if err != nil {
						t.Fatal(err)
					}
					catalog.EdgeShards = edgePageSpecs(shards, graph.New())
					writer.decorateIndexCatalog(&catalog, "tenant-a")
					if err := writer.writeParquetEdgeShards(ctx, "tenant-a", shards); err != nil {
						t.Fatal(err)
					}
					objects = catalog.EdgeShards[0].Objects
				} else {
					var pages []EntityPageData
					for i := 0; i < 3; i++ {
						lower := ""
						if i > 0 {
							lower = fmt.Sprintf("host:%d", i*2)
						}
						pages = append(pages, EntityPageData{Shard: "00", Version: 1, pageRole: rangeRole(lower), Entities: []graph.Entity{{ID: fmt.Sprintf("host:%d", i*2), Kind: "host"}, {ID: fmt.Sprintf("host:%d", i*2+1), Kind: "host"}}})
					}
					pages, err := prepareBoundedEntityPages(ctx, "tenant-a", pages)
					if err != nil {
						t.Fatal(err)
					}
					catalog.EntityPages = entityPageSpecs(pages)
					writer.decorateIndexCatalog(&catalog, "tenant-a")
					if err := writer.writeParquetEntityPages(ctx, "tenant-a", pages, 1); err != nil {
						t.Fatal(err)
					}
					objects = catalog.EntityPages[0].Objects
				}
				reads := &pipelineScanStore{ObjectStore: base, first: objects[0].Key, second: objects[1].Key, nextStarted: make(chan struct{}), firstStarted: make(chan struct{}), calls: map[string]int{}, blockNext: mode != "complete", blockFirst: mode == "cancel"}
				reader := NewTenantStore(reads, "test")
				reader.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 8})
				done := make(chan error, 1)
				go func() {
					limit := 100
					if mode == "early-stop" {
						limit = 1
					}
					if edge {
						result, ok, err := reader.listEdgesFromShards(ctx, "tenant-a", 1, catalog, EdgeScanOptions{Limit: limit}, scanCursor{})
						if err == nil && (!ok || len(result.Edges) != min(limit, 6)) {
							err = fmt.Errorf("unexpected edge count %d, available %v", len(result.Edges), ok)
						}
						done <- err
					} else {
						result, ok, err := reader.listEntitiesFromPages(ctx, "tenant-a", 1, catalog, EntityScanOptions{Limit: limit}, scanCursor{}, nil)
						if err == nil && (!ok || len(result.Entities) != min(limit, 6)) {
							err = fmt.Errorf("unexpected entity count %d, available %v", len(result.Entities), ok)
						}
						done <- err
					}
				}()
				if mode == "cancel" {
					select {
					case <-reads.firstStarted:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					select {
					case <-reads.nextStarted:
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					cancel()
				}
				err := <-done
				if mode == "cancel" {
					if err != context.Canceled {
						t.Fatalf("canceled scan: %v", err)
					}
				} else if err != nil {
					t.Fatal(err)
				}
				reads.mu.Lock()
				defer reads.mu.Unlock()
				if reads.active != 0 || reads.maxActive > 2 {
					t.Fatalf("unfinished or unbounded I/O: active=%d peak=%d", reads.active, reads.maxActive)
				}
				want := 1
				if mode != "complete" {
					want = 0
				}
				if reads.calls[objects[2].Key] != want {
					t.Fatalf("third page GETs=%d, want %d", reads.calls[objects[2].Key], want)
				}
				for _, object := range objects {
					if reads.calls[object.Key] > 1 {
						t.Fatalf("duplicate GET for %s", object.Key)
					}
				}
				reader.indexCache.mu.Lock()
				defer reader.indexCache.mu.Unlock()
				if len(reader.indexCache.prefetching) != 0 {
					t.Fatal("scan retained global prefetch admission")
				}
			})
		}
	}
}
