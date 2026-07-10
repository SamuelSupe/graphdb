package httpapi

import (
	"context"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"graphdb/internal/graph"
	"graphdb/internal/storage"
)

func TestQueryReadMemoAvoidsDuplicateManifestAndCatalogReads(t *testing.T) {
	ctx := context.Background()
	base := storage.NewMemoryStore()
	writer := storage.NewTenantStore(base, "test")
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := writer.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	objects := &queryCountingStore{ObjectStore: base}
	server := &Server{Store: storage.NewTenantStore(objects, "test")}
	memoCtx := withQueryReadMemo(ctx)
	request := httptest.NewRequest("GET", "/v1/query", nil).WithContext(memoCtx)
	if _, err := server.readTarget(request, "tenant-a", readFreshness{}); err != nil {
		t.Fatalf("first target: %v", err)
	}
	if _, err := server.readTarget(request, "tenant-a", readFreshness{}); err != nil {
		t.Fatalf("second target: %v", err)
	}
	if _, _, ok := server.lazyQueryOptions(memoCtx, "tenant-a", 1); !ok {
		t.Fatal("lazy query options unavailable")
	}
	if options := server.queryOptions(memoCtx, "tenant-a", 1); options.IndexLookup == nil {
		t.Fatal("fallback query options unavailable")
	}
	secondRequestCtx := withQueryReadMemo(context.Background())
	if _, _, ok := server.lazyQueryOptions(secondRequestCtx, "tenant-a", 1); !ok {
		t.Fatal("cross-request lazy query options unavailable")
	}
	if _, _, ok := server.lazyQueryOptions(withQueryReadMemo(context.Background()), "tenant-a", unconstrainedVersion); !ok {
		t.Fatal("allow-stale catalog options unavailable")
	}
	if got := objects.countContains("/manifest.parquet"); got != 1 {
		t.Fatalf("manifest reads = %d, want 1", got)
	}
	if got := objects.countContains("/indexes/catalog.parquet"); got != 1 {
		t.Fatalf("catalog reads = %d, want 1", got)
	}
}

func TestAllowStaleWithoutMinimumSkipsManifestProbe(t *testing.T) {
	objects := &queryCountingStore{ObjectStore: storage.NewMemoryStore()}
	server := &Server{Store: storage.NewTenantStore(objects, "test")}
	request := httptest.NewRequest("GET", "/v1/query?allow_stale=true", nil)
	target, err := server.readTarget(request, "tenant-a", readFreshness{})
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if !target.AllowStale || target.TargetVersion != 0 || target.ManifestVersion != unconstrainedVersion {
		t.Fatalf("target = %#v", target)
	}
	if got := objects.countContains("/manifests/"); got != 0 {
		t.Fatalf("manifest reads = %d, want 0", got)
	}
}

type queryCountingStore struct {
	storage.ObjectStore
	mu   sync.Mutex
	gets map[string]int
}

func (s *queryCountingStore) GetWithMeta(ctx context.Context, key string) ([]byte, storage.ObjectMeta, error) {
	s.mu.Lock()
	if s.gets == nil {
		s.gets = map[string]int{}
	}
	s.gets[key]++
	s.mu.Unlock()
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *queryCountingStore) countContains(fragment string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for key, count := range s.gets {
		if strings.Contains(key, fragment) {
			total += count
		}
	}
	return total
}
