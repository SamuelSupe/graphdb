package storage

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIndexObjectCacheAvoidsRepeatedFieldIndexReads(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 16})
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects

	for i := 0; i < 2; i++ {
		lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
		ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"})
		if err != nil || !ok || len(ids) != 1 {
			t.Fatalf("lookup %d ids=%#v ok=%v err=%v", i, ids, ok, err)
		}
	}
	if got := objects.CountContains("/indexes/parquet/versions/"); got != 1 {
		t.Fatalf("parquet index object gets = %d, want one object-store read", got)
	}
}

func TestIndexObjectMemoryCacheHonorsByteLimit(t *testing.T) {
	cache := newIndexObjectCache(10)
	cache.maxBytes = 420
	cache.put("first", cachedIndexObject{data: make([]byte, 300), meta: ObjectMeta{Key: "first"}})
	cache.put("second", cachedIndexObject{data: make([]byte, 300), meta: ObjectMeta{Key: "second"}})
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.bytes > cache.maxBytes || len(cache.data) > 1 {
		t.Fatalf("memory cache bytes=%d entries=%d max=%d", cache.bytes, len(cache.data), cache.maxBytes)
	}
}

func TestIndexObjectDiskCacheHonorsEntryLimit(t *testing.T) {
	dir := t.TempDir()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{
		MaxEntries:   2,
		MaxBytes:     1 << 20,
		DiskDir:      dir,
		DiskMaxBytes: 1 << 20,
	})
	for i := int64(1); i <= 3; i++ {
		store.putCachedIndexObject("entity_page", "tenant-a", i, "object", "content", "schema", []byte("data"), ObjectMeta{Key: "object"})
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read cache dir: %v", err)
	}
	dataFiles := 0
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".parquet") {
			dataFiles++
		}
	}
	if dataFiles > 2 {
		t.Fatalf("disk cache data files = %d, want at most 2", dataFiles)
	}
}

func TestIndexObjectDiskCacheDoesNotPersistVerifiedState(t *testing.T) {
	dir := t.TempDir()
	cacheKey := indexObjectCacheKey("secondary_index", "tenant-a", 1, "object", "content", "schema")
	first := newIndexObjectCache(2)
	first.disk = newIndexDiskCache(dir, 2, 1<<20, time.Hour)
	first.put(cacheKey, cachedIndexObject{
		data:     []byte("parquet bytes"),
		meta:     ObjectMeta{Key: "object", ETag: "etag"},
		verified: true,
	})

	second := newIndexObjectCache(2)
	second.disk = newIndexDiskCache(dir, 2, 1<<20, time.Hour)
	entry, ok, err := second.get(context.Background(), cacheKey, "object")
	if err != nil || !ok {
		t.Fatalf("disk get ok=%v err=%v", ok, err)
	}
	if entry.verified {
		t.Fatal("disk cache persisted process-local content verification")
	}
}

func TestIndexObjectCacheAvoidsRepeatedEdgeShardReads(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 16})
	if _, err := store.Commit(ctx, "tenant-a", twoHostEdgeMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects

	for i := 0; i < 2; i++ {
		lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
		edges, ok, err := lookup.OutEdges(ctx, "service:api", nil)
		if err != nil || !ok || len(edges) != 1 {
			t.Fatalf("lookup %d edges=%#v ok=%v err=%v", i, edges, ok, err)
		}
	}
	if got := objects.CountContains("/indexes/parquet/versions/"); got != 1 {
		t.Fatalf("parquet edge shard gets = %d, want one object-store read", got)
	}
}

func TestIndexObjectCacheAvoidsRepeatedEntityPageReadsAcrossScanPages(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	ids := sameEntityShardIDs(t, "host:cache-page-", 2)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: ids[0], Kind: "host"},
			{ID: ids[1], Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	counting := &countingMetaReadStore{ObjectStore: base}
	store.Objects = counting
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 16})

	first, err := store.ListEntitiesFromCatalog(ctx, "tenant-a", catalog, EntityScanOptions{Kind: "host", Limit: 1})
	if err != nil || len(first.Entities) != 1 || first.NextCursor == "" {
		t.Fatalf("first page=%#v err=%v", first, err)
	}
	second, err := store.ListEntitiesFromCatalog(ctx, "tenant-a", catalog, EntityScanOptions{Kind: "host", Limit: 1, Cursor: first.NextCursor})
	if err != nil || len(second.Entities) != 1 {
		t.Fatalf("second page=%#v err=%v", second, err)
	}
	pageKey := store.parquetEntityPageVersionKey("tenant-a", catalog.Version, entityShardID(ids[0]))
	if got := counting.GetWithMetaCount(pageKey); got != 1 {
		t.Fatalf("entity page GetWithMeta count = %d, want one object-store read", got)
	}
}

func TestReaderObjectCacheKeysIncludeSchemaHash(t *testing.T) {
	indexKeyA := indexObjectCacheKey("entity_page", "tenant-a", 7, "object-key", "content-a", "schema-a")
	indexKeyB := indexObjectCacheKey("entity_page", "tenant-a", 7, "object-key", "content-a", "schema-b")
	if indexKeyA == indexKeyB {
		t.Fatal("index object cache key ignored schema hash")
	}

	pageKeyA := entityPageCacheKey("tenant-a", 7, "object-key", "content-a", "schema-a")
	pageKeyB := entityPageCacheKey("tenant-a", 7, "object-key", "content-a", "schema-b")
	if pageKeyA == pageKeyB {
		t.Fatal("entity page cache key ignored schema hash")
	}
}

func TestReaderObjectCachesDoNotReuseAcrossSchemaHash(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	store.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 4})

	store.putCachedIndexObject("entity_page", "tenant-a", 7, "object-key", "content-a", "schema-a", []byte("cached-page"), ObjectMeta{Key: "object-key"})
	if data, _, ok, err := store.cachedIndexObject(ctx, "entity_page", "tenant-a", 7, "object-key", "content-a", "schema-a"); err != nil || !ok || string(data) != "cached-page" {
		t.Fatalf("same schema index cache lookup data=%q ok=%v err=%v", data, ok, err)
	}
	if data, _, ok, err := store.cachedIndexObject(ctx, "entity_page", "tenant-a", 7, "object-key", "content-a", "schema-b"); err != nil || ok || len(data) != 0 {
		t.Fatalf("different schema index cache lookup data=%q ok=%v err=%v", data, ok, err)
	}

	page := EntityPageData{Entities: []graph.Entity{{ID: "host:a", Kind: "host"}}}
	store.putCachedEntityPage("tenant-a", 7, "object-key", "content-a", "schema-a", page, "etag-a")
	if got, etag, ok := store.cachedEntityPage("tenant-a", 7, "object-key", "content-a", "schema-a"); !ok || etag != "etag-a" || len(got.Entities) != 1 || got.Entities[0].ID != "host:a" {
		t.Fatalf("same schema entity page lookup page=%#v etag=%q ok=%v", got, etag, ok)
	}
	if got, etag, ok := store.cachedEntityPage("tenant-a", 7, "object-key", "content-a", "schema-b"); ok || etag != "" || len(got.Entities) != 0 {
		t.Fatalf("different schema entity page lookup page=%#v etag=%q ok=%v", got, etag, ok)
	}
}

func TestIndexObjectCacheDropsMismatchedSecondaryIndexAndReloads(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	spec := requireFieldIndexSpec(t, catalog, "host", "hostname")
	key := requireAnyIndexObjectKey(t, spec.Objects)
	bogus := SecondaryIndex{
		TenantID: "tenant-a",
		Kind:     "host",
		Field:    "hostname",
		Version:  catalog.Version,
		Values:   map[string][]string{"wrong": {"host:missing"}},
	}
	data, err := marshalParquetSecondaryIndex(ctx, bogus)
	if err != nil {
		t.Fatalf("marshal bogus index: %v", err)
	}
	store.putCachedIndexObject("secondary_index", "tenant-a", catalog.Version, key, spec.ContentHash, spec.SchemaHash, data, ObjectMeta{Key: key})

	index, ok, err := store.loadParquetSecondaryIndexObject(ctx, "tenant-a", catalog.Version, spec)
	if err != nil || !ok || !fieldIndexMatchesCatalog(index, spec, catalog.Version) {
		t.Fatalf("reloaded index ok=%v err=%v index=%#v", ok, err, index)
	}
}

func TestIndexObjectCacheDropsMismatchedEdgeShardAndReloads(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", twoHostEdgeMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	shardID := edgeShardID("service:api")
	spec := requireEdgeShardSpec(t, catalog, "runs_on", shardID)
	key := requireIndexObjectKey(t, spec.Objects, "shard")
	bogus := EdgeShardData{
		TenantID:      "tenant-a",
		RelationType:  "runs_on",
		Shard:         shardID,
		Version:       catalog.Version,
		LayoutVersion: CurrentObjectLayoutVersion,
	}
	data, err := marshalParquetEdgeShard(ctx, bogus)
	if err != nil {
		t.Fatalf("marshal bogus shard: %v", err)
	}
	store.putCachedIndexObject("edge_shard", "tenant-a", catalog.Version, key, spec.ContentHash, spec.SchemaHash, data, ObjectMeta{Key: key})

	shard, ok, err := store.loadParquetEdgeShardObject(ctx, "tenant-a", catalog.Version, spec)
	if err != nil || !ok || !edgeShardMatchesCatalog(shard, spec, catalog.Version) {
		t.Fatalf("reloaded shard ok=%v err=%v shard=%#v", ok, err, shard)
	}
}

func TestIndexObjectCacheDropsMismatchedEntityPageAndReloads(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	ids := sameEntityShardIDs(t, "host:cache-reload-", 2)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: ids[0], Kind: "host"},
			{ID: ids[1], Kind: "host"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	shardID := entityShardID(ids[0])
	spec := requireEntityPageSpec(t, catalog, shardID)
	key := requireIndexObjectKey(t, spec.Objects, "page")
	bogus := EntityPageData{
		TenantID:      "tenant-a",
		Shard:         shardID,
		Version:       catalog.Version,
		LayoutVersion: CurrentObjectLayoutVersion,
	}
	data, err := marshalParquetEntityPage(ctx, bogus)
	if err != nil {
		t.Fatalf("marshal bogus page: %v", err)
	}
	store.putCachedIndexObject("entity_page", "tenant-a", catalog.Version, key, spec.ContentHash, spec.SchemaHash, data, ObjectMeta{Key: key})

	page, _, ok, err := store.loadParquetEntityPageObject(ctx, "tenant-a", catalog.Version, spec)
	if err != nil || !ok || !entityPageReadable(page, "tenant-a", catalog.Version, spec) || len(page.Entities) != 2 {
		t.Fatalf("reloaded page ok=%v err=%v page=%#v", ok, err, page)
	}
}

func TestIndexObjectDiskCacheSurvivesStoreRecreate(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	if _, err := writer.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := writer.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	objects := &countingMetaReadStore{ObjectStore: base}
	cacheDir := t.TempDir()
	first := NewTenantStore(objects, "test")
	first.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 1, DiskDir: cacheDir})
	second := NewTenantStore(objects, "test")
	second.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 1, DiskDir: cacheDir})
	objects.Reset()

	firstLookup := &PersistedIndexLookup{Store: first, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if _, ok, err := firstLookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"}); err != nil || !ok {
		t.Fatalf("first lookup ok=%v err=%v", ok, err)
	}
	secondLookup := &PersistedIndexLookup{Store: second, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if _, ok, err := secondLookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-02"}); err != nil || !ok {
		t.Fatalf("second lookup ok=%v err=%v", ok, err)
	}
	if got := objects.CountContains("/indexes/parquet/versions/"); got != 1 {
		t.Fatalf("parquet index object gets = %d, want disk cache hit for second store", got)
	}
}

type countingMetaReadStore struct {
	ObjectStore

	mu   sync.Mutex
	gets map[string]int
}

func (s *countingMetaReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.mu.Lock()
	if s.gets == nil {
		s.gets = map[string]int{}
	}
	s.gets[key]++
	s.mu.Unlock()
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *countingMetaReadStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if head, ok := s.ObjectStore.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	return meta, err
}

func (s *countingMetaReadStore) GetWithMetaCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

func (s *countingMetaReadStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = map[string]int{}
}

func (s *countingMetaReadStore) CountContains(fragment string) int {
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
