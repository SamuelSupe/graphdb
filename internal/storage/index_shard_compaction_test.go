package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestSecondaryIndexShardPackingMergesSmallShards(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	entities := make([]graph.Entity, 0, 10)
	for i := 0; i < 10; i++ {
		hostname := fmt.Sprintf("%02d-value", i)
		entities = append(entities, graph.Entity{
			ID:     "host:" + hostname,
			Kind:   "host",
			Fields: graph.Fields{"hostname": hostname},
		})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	spec := requireFieldIndexSpec(t, catalog, "host", "hostname")
	if _, ok := indexObjectByRole(spec.Objects, "postings"); ok {
		t.Fatalf("non-empty field index should not publish full postings object: %#v", spec.Objects)
	}
	if keys := uniqueObjectKeys(spec.Objects); len(keys) != 1 {
		t.Fatalf("small logical shards should share one pack object, keys=%#v objects=%#v", keys, spec.Objects)
	}
	fullKey := store.parquetSecondaryIndexVersionKey("tenant-a", catalog.Version, "host", "hostname")
	if _, err := store.Objects.Get(ctx, fullKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("full postings object err=%v, want ErrNotFound", err)
	}

	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"00-value", "09-value"})
	if err != nil || !ok || len(ids) != 2 {
		t.Fatalf("match ids=%#v ok=%v err=%v", ids, ok, err)
	}
	if got := objects.GetWithMetaCount(uniqueObjectKeys(spec.Objects)[0]); got != 1 {
		t.Fatalf("packed object reads = %d, want 1", got)
	}
	entries, ok, err := lookup.ScanFieldIndex(ctx, "host", "hostname")
	if err != nil || !ok || len(entries) != 10 {
		t.Fatalf("scan entries=%#v ok=%v err=%v", entries, ok, err)
	}
}

func TestSecondaryIndexHotStringShardSplitsByLongerPrefix(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	entities := make([]graph.Entity, 0, 2200)
	for i := 0; i < 1100; i++ {
		entities = append(entities,
			graph.Entity{ID: fmt.Sprintf("host:a-%04d", i), Kind: "host", Fields: graph.Fields{"hostname": "hotkey-a"}},
			graph.Entity{ID: fmt.Sprintf("host:b-%04d", i), Kind: "host", Fields: graph.Fields{"hostname": "hotkey-b"}},
		)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	spec := requireFieldIndexSpec(t, catalog, "host", "hostname")
	if _, ok := indexObjectByRole(spec.Objects, secondaryIndexShardRole("s_hotkey-")); ok {
		t.Fatalf("hot shard should be split beyond seven byte prefix: %#v", spec.Objects)
	}
	if _, ok := indexObjectByRole(spec.Objects, secondaryIndexShardRole("s_hotkey-a")); !ok {
		t.Fatalf("missing split shard s_hotkey-a in %#v", spec.Objects)
	}
	if _, ok := indexObjectByRole(spec.Objects, secondaryIndexShardRole("s_hotkey-b")); !ok {
		t.Fatalf("missing split shard s_hotkey-b in %#v", spec.Objects)
	}
	if keys := uniqueObjectKeys(spec.Objects); len(keys) != 2 {
		t.Fatalf("hot split should produce two physical shard objects, keys=%#v objects=%#v", keys, spec.Objects)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"hotkey-b"})
	if err != nil || !ok || len(ids) != 1100 {
		t.Fatalf("hot shard lookup ids=%d ok=%v err=%v", len(ids), ok, err)
	}
}

func uniqueObjectKeys(objects []IndexObject) []string {
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		if object.Key == "" {
			continue
		}
		if _, ok := seen[object.Key]; ok {
			continue
		}
		seen[object.Key] = struct{}{}
		keys = append(keys, object.Key)
	}
	return keys
}
