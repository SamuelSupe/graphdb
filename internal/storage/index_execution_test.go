package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
)

func TestPersistedIndexLookupAndIncrementalUpdate(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-01" {
		t.Fatalf("field lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
	edges, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}})
	wantEdgeID := graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")
	if err != nil || !ok || len(edges) != 1 || edges[0].ID != wantEdgeID || len(edges[0].Sources) != 1 || edges[0].Sources[0].EdgeID != "edge:api-host" {
		t.Fatalf("edge lookup edges=%#v ok=%v err=%v", edges, ok, err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("incremental commit: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog after incremental: %v", err)
	}
	if catalog.Version != 2 {
		t.Fatalf("catalog version = %d, want 2", catalog.Version)
	}
	if catalog.TenantID != "tenant-a" {
		t.Fatalf("catalog tenant = %q, want tenant-a", catalog.TenantID)
	}
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: 2, Catalog: catalog}
	ids, ok, err = lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-02"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-01" {
		t.Fatalf("updated lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
	ids, ok, err = lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"})
	if err != nil || !ok || len(ids) != 0 {
		t.Fatalf("old lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
	entries, ok, err := lookup.ScanFieldIndex(ctx, "host", "hostname")
	if err != nil || !ok || len(entries["s:app-02"]) != 1 || entries["s:app-02"][0] != "host:app-01" {
		t.Fatalf("scan entries=%#v ok=%v err=%v", entries, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func TestWriterHotCommitAvoidsFixedMetadataReads(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := newParquetIndexTenantStore(objects, "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 300})
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}

	objects.Reset()
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("hot commit: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	for _, fragment := range []string{"/_registry.parquet", "/manifest.parquet", "/metadata.parquet", "/control/writer-lease.parquet", "/indexes/catalog.parquet", "/config/source-policy.parquet", "/config/tenant.parquet"} {
		if got := objects.CountContains(fragment); got != 0 {
			t.Fatalf("hot commit GET count for %s = %d, want 0", fragment, got)
		}
	}
}

func TestRebuildIndexesAvoidsNewEntityRecordMissReads(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := newParquetIndexTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	objects.Reset()
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if got := objects.CountContains("/indexes/entities/by-id/"); got != 0 {
		t.Fatalf("new entity record GET count = %d, want 0", got)
	}
}

func TestOptionalEntityRecordsCanBeDisabled(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	store.WriteEntityRecords = false
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if count := entityRecordObjectCountForTest(t, ctx, store, "tenant-a"); count != 0 {
		t.Fatalf("entity record objects = %d, want 0", count)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	entity, ok, err := lookup.GetEntity(ctx, "host:app-01", []string{"hostname"})
	if err != nil || !ok || entity.Fields["hostname"] != "app-01" {
		t.Fatalf("page lookup entity=%#v ok=%v err=%v", entity, ok, err)
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("incremental commit: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if count := entityRecordObjectCountForTest(t, ctx, store, "tenant-a"); count != 0 {
		t.Fatalf("entity record objects after incremental = %d, want 0", count)
	}
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	entity, ok, err = lookup.GetEntity(ctx, "host:app-01", []string{"hostname"})
	if err != nil || !ok || entity.Fields["hostname"] != "app-02" {
		t.Fatalf("updated page lookup entity=%#v ok=%v err=%v", entity, ok, err)
	}
}

func TestPersistedEntityLookupUsesValidatedEntityRecordBeforePage(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	store.UseEntityRecordsForRead = true
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	entity, ok, err := lookup.GetEntity(ctx, "host:app-01", []string{"hostname"})
	if err != nil || !ok || entity.Fields["hostname"] != "app-01" {
		t.Fatalf("entity=%#v ok=%v err=%v", entity, ok, err)
	}
	lookup.pageMu.Lock()
	loadedPages := len(lookup.pageCache)
	lookup.pageMu.Unlock()
	if loadedPages != 0 {
		t.Fatalf("entity record lookup decoded %d page(s), want 0", loadedPages)
	}
}

func TestIncrementalIndexCatalogReusesUnchangedEntityPageObjects(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	entities := make([]graph.Entity, 0, 400)
	for i := 0; i < 400; i++ {
		id := fmt.Sprintf("host:%03d", i)
		entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"hostname": id, "region": "us-east-1"}})
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{
			"hostname": {Type: "string", Indexed: true},
			"region":   {Type: "string", Indexed: true},
		}}},
		UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:000", Kind: "host", Fields: graph.Fields{"hostname": "changed"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("incremental commit: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	reused, current := 0, 0
	for _, index := range catalog.Indexes {
		for _, object := range index.Objects {
			version, ok := store.parquetVersionFromKey("tenant-a", object.Key)
			if !ok {
				continue
			}
			switch version {
			case 1:
				reused++
			case catalog.Version:
				current++
			}
		}
	}
	if reused == 0 || current == 0 {
		t.Fatalf("secondary index object versions reused=%d current=%d, want both non-zero", reused, current)
	}
}

func entityRecordObjectCountForTest(t *testing.T, ctx context.Context, store *TenantStore, tenantID string) int {
	t.Helper()
	objects, err := store.Objects.List(ctx, store.entityRecordPrefix(tenantID))
	if err != nil {
		t.Fatalf("list entity records: %v", err)
	}
	count := 0
	for _, object := range objects {
		if _, ok, err := store.entityIDFromRecordKey(tenantID, object.Key); err != nil {
			t.Fatalf("parse entity record key %q: %v", object.Key, err)
		} else if ok {
			count++
		}
	}
	return count
}

func TestFilteredFieldIndexScanReadsCandidateShard(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	entities := make([]graph.Entity, 0, 100)
	for i := 0; i < 100; i++ {
		hostname := fmt.Sprintf("app-%06d", i)
		entities = append(entities, graph.Entity{ID: "host:" + hostname, Kind: "host", Fields: graph.Fields{"hostname": hostname}})
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
	fullKey := store.parquetSecondaryIndexVersionKey("tenant-a", catalog.Version, "host", "hostname")
	shardKey := ""
	for _, object := range spec.Objects {
		if object.Role == secondaryIndexShardRole("s_app-000") {
			shardKey = object.Key
			break
		}
	}
	if shardKey == "" {
		t.Fatalf("missing candidate shard object in %#v", spec.Objects)
	}

	objects := &countingMetaReadStore{ObjectStore: base}
	store.Objects = objects
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	entries, ok, err := lookup.ScanFieldIndexWithFilters(ctx, "host", "hostname", []query.Filter{
		{Field: "hostname", Op: "gte", Value: "app-000010"},
		{Field: "hostname", Op: "lt", Value: "app-000020"},
	})
	if err != nil || !ok || len(entries) == 0 {
		t.Fatalf("filtered scan entries=%#v ok=%v err=%v", entries, ok, err)
	}
	if got := objects.GetWithMetaCount(fullKey); got != 0 {
		t.Fatalf("full postings reads = %d, want 0", got)
	}
	if got := objects.GetWithMetaCount(shardKey); got != 1 {
		t.Fatalf("candidate shard reads = %d, want 1", got)
	}
}

func TestIncrementalIndexUpdateDoesNotOverwriteAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := newParquetIndexTenantStore(base, "test")
	writer.LeaseTTL = time.Millisecond
	if _, err := writer.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, err := writer.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild v1: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	objects := &takeoverDuringIncrementalIndexStore{ObjectStore: base, base: base, tenantID: "tenant-a"}
	committer := newParquetIndexTenantStore(objects, "test")
	committer.LeaseTTL = time.Nanosecond
	result, err := committer.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01b"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	if !objects.Triggered() {
		t.Fatal("test store did not trigger takeover")
	}
	if len(result.IndexWarnings) == 0 {
		t.Fatalf("commit result missing index warning: %#v", result)
	}

	manifest, err := writer.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if manifest.Version != 3 {
		t.Fatalf("manifest version = %d, want takeover version 3", manifest.Version)
	}
	catalog, err := writer.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get catalog: %v", err)
	}
	if catalog.Version == result.Version {
		t.Fatalf("stale incremental update published catalog version %d", catalog.Version)
	}
}

func TestIncrementalIndexDeleteDoesNotRemoveObjectAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := newParquetIndexTenantStore(base, "test")
	writer.LeaseTTL = time.Millisecond
	if _, err := writer.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, err := writer.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild v1: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	objects := &takeoverDuringIncrementalDeleteStore{ObjectStore: base, base: base, tenantID: "tenant-a"}
	committer := newParquetIndexTenantStore(objects, "test")
	committer.LeaseTTL = time.Nanosecond
	result, err := committer.CommitWithReport(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{"host:app-01"}}, CommitOptions{})
	if err != nil {
		t.Fatalf("delete commit: %v", err)
	}
	if !objects.Triggered() {
		t.Fatal("test store did not trigger takeover")
	}
	if len(result.IndexWarnings) == 0 {
		t.Fatalf("delete commit missing index warning: %#v", result)
	}

	record, err := writer.loadEntityRecord(ctx, "tenant-a", "host:app-01")
	if err != nil {
		t.Fatalf("load re-created entity record: %v", err)
	}
	if record.Version != 3 || record.Entity.Fields["hostname"] != "app-02" {
		t.Fatalf("entity record was removed or stale: %#v", record)
	}
}

func TestIncrementalIndexDeleteConflictKeepsLookupSafeAndHealthReportsOrphans(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(&failConditionalDeleteStore{ObjectStore: base}, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{"host:app-01"}}, CommitOptions{})
	if err != nil {
		t.Fatalf("delete commit: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	lookup := PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if _, ok, err := lookup.GetEntity(ctx, "host:app-01", nil); err != nil || ok {
		t.Fatalf("deleted entity lookup ok=%v err=%v", ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "ready" {
		t.Fatalf("health = %#v", health)
	}
}

func TestPersistedIndexObjectsIncludeTenantID(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	index, _ := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	if index.TenantID != "tenant-a" {
		t.Fatalf("index tenant = %q, want tenant-a", index.TenantID)
	}
	shardID := edgeShardID("service:api")
	shard, _ := readParquetEdgeShardForTest(t, ctx, store, "tenant-a", catalog, "runs_on", shardID)
	if shard.TenantID != "tenant-a" {
		t.Fatalf("shard tenant = %q, want tenant-a", shard.TenantID)
	}
	pageID := entityShardID("host:app-01")
	page, _ := readParquetEntityPageForTest(t, ctx, store, "tenant-a", catalog, pageID)
	if page.TenantID != "tenant-a" {
		t.Fatalf("page tenant = %q, want tenant-a", page.TenantID)
	}
	record, err := store.loadEntityRecord(ctx, "tenant-a", "host:app-01")
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	if record.TenantID != "tenant-a" {
		t.Fatalf("record tenant = %q, want tenant-a", record.TenantID)
	}
}

func TestEntityRecordsUseParquet(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	parquetKey := store.entityRecordKey("tenant-a", "host:app-01")
	if !strings.HasSuffix(parquetKey, ".parquet") {
		t.Fatalf("entity record key = %q, want parquet", parquetKey)
	}
	if _, err := store.Objects.Get(ctx, parquetKey); err != nil {
		t.Fatalf("get parquet record: %v", err)
	}
	record, err := store.loadEntityRecord(ctx, "tenant-a", "host:app-01")
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	if record.ID != "host:app-01" || record.Entity.ID != "host:app-01" {
		t.Fatalf("record = %#v, want host:app-01", record)
	}
}

func TestPersistedLookupRejectsMismatchedIndexObjectTenants(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	index, indexKey := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	index.TenantID = "tenant-b"
	writeParquetFieldIndexForTest(t, ctx, store, indexKey, index)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"}); err != nil || ok || len(ids) != 0 {
		t.Fatalf("field lookup ids=%#v ok=%v err=%v, want unavailable", ids, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "field index host.hostname tenant mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestPersistedEdgeLookupRejectsMismatchedShardTenant(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	shardID := edgeShardID("service:api")
	shard, shardKey := readParquetEdgeShardForTest(t, ctx, store, "tenant-a", catalog, "runs_on", shardID)
	shard.TenantID = "tenant-b"
	writeParquetEdgeShardForTest(t, ctx, store, shardKey, shard)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if edges, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}}); err != nil || ok || len(edges) != 0 {
		t.Fatalf("edge lookup edges=%#v ok=%v err=%v, want unavailable", edges, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "edge shard runs_on/"+shardID+" tenant mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestPersistedEntityLookupUsesPageWhenEntityRecordTenantMismatches(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	recordKey := store.entityRecordKey("tenant-a", "host:app-01")
	record, err := store.loadEntityRecord(ctx, "tenant-a", "host:app-01")
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	record.TenantID = "tenant-b"
	if err := writeEntityRecordForTest(ctx, store, recordKey, record); err != nil {
		t.Fatalf("write record: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if entity, ok, err := lookup.GetEntity(ctx, "host:app-01", nil); err != nil || !ok || entity.ID != "host:app-01" {
		t.Fatalf("entity lookup entity=%#v ok=%v err=%v, want page-backed entity", entity, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "entity record host:app-01 tenant mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestPersistedEntityLookupRejectsMismatchedEntityPageTenant(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	pageID := entityShardID("host:app-01")
	page, pageKey := readParquetEntityPageForTest(t, ctx, store, "tenant-a", catalog, pageID)
	page.TenantID = "tenant-b"
	writeParquetEntityPageForTest(t, ctx, store, pageKey, page)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if entity, ok, err := lookup.GetEntity(ctx, "host:app-01", nil); err != nil || ok {
		t.Fatalf("entity lookup entity=%#v ok=%v err=%v, want unavailable", entity, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "entity page "+pageID+" tenant mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestIncrementalIndexRejectsMismatchedSecondaryIndexTenant(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	index, indexKey := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	index.TenantID = "tenant-b"
	writeParquetFieldIndexForTest(t, ctx, store, indexKey, index)
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	currentCatalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current catalog: %v", err)
	}
	currentIndex, _ := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", currentCatalog, "host", "hostname")
	if currentIndex.TenantID != "tenant-a" || currentCatalog.Version != result.Manifest.Version {
		t.Fatalf("current parquet index = %#v catalog=%#v", currentIndex, currentCatalog)
	}
}

func TestCommitReportsIncrementalIndexUpdateWarning(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := newParquetIndexTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	store.Objects = &failPutStore{ObjectStore: objects, contains: "/indexes/catalog.parquet"}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit with index warning: %v", err)
	}
	if result.Version != 2 {
		t.Fatalf("result version = %d, want committed version 2", result.Version)
	}
	if len(result.IndexWarnings) != 1 || !strings.Contains(result.IndexWarnings[0], "incremental index update failed") {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "stale" {
		t.Fatalf("health = %#v, want stale catalog after warning", health)
	}
}

func TestCommitReportsStaleCatalogVersionWarning(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	catalog.Version = 0
	writeParquetIndexCatalogForTest(t, ctx, store, "tenant-a", catalog)
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit with stale catalog warning: %v", err)
	}
	if result.Version != 2 {
		t.Fatalf("result version = %d, want committed version 2", result.Version)
	}
	if len(result.IndexWarnings) != 1 || !strings.Contains(result.IndexWarnings[0], "index catalog version 0 does not match previous graph version 1") {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "stale" {
		t.Fatalf("health = %#v, want stale catalog after warning", health)
	}
}

func TestEntityFieldNameRejectsEmptyFieldsPrefix(t *testing.T) {
	if field, ok := entityFieldName("fields."); ok || field != "" {
		t.Fatalf("fields. resolved as field=%q ok=%v", field, ok)
	}
	if field, ok := entityFieldName("fields.hostname"); !ok || field != "hostname" {
		t.Fatalf("fields.hostname resolved as field=%q ok=%v", field, ok)
	}
}

func TestPersistedIndexLookupTreatsJSONNumbersAsNumbers(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"cpu": {Type: "number", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"cpu": 8},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "cpu", []any{json.Number("8.0")})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-01" {
		t.Fatalf("numeric lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
}

func TestPersistedIndexesIncludeInheritedCIFields(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{
			{
				Name: "asset",
				Fields: map[string]graph.FieldSpec{
					"region": {Type: "string", Indexed: true},
				},
			},
			{
				Name:    "host",
				Extends: []string{"asset"},
				Fields: map[string]graph.FieldSpec{
					"hostname": {Type: "string"},
				},
			},
		},
		UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "us-east-1"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "region", []any{"us-east-1"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-01" {
		t.Fatalf("inherited field lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
	if _, ok := catalogField(catalog, "host", "region"); !ok {
		t.Fatalf("catalog missing inherited host.region index: %#v", catalog.Indexes)
	}
}

func TestPersistedIndexLookupMaterializesEntityFromPageWithoutRecordRead(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := newParquetIndexTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	objects.Reset()

	reader := newParquetIndexTenantStore(objects, "test")
	lookup := &PersistedIndexLookup{Store: reader, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-01" {
		t.Fatalf("field lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
	if got := objects.CountContains("/indexes/entities/by-id/"); got != 0 {
		t.Fatalf("entity record gets after field index lookup = %d, want 0", got)
	}
	if err := objects.Delete(ctx, reader.entityRecordKey("tenant-a", "host:app-01")); err != nil {
		t.Fatalf("delete optional entity record: %v", err)
	}
	entity, ok, err := lookup.GetEntity(ctx, "host:app-01", []string{"hostname"})
	if err != nil || !ok || entity.Fields["hostname"] != "app-01" {
		t.Fatalf("entity=%#v ok=%v err=%v", entity, ok, err)
	}
	if got := objects.CountContains("/indexes/entities/by-id/"); got != 0 {
		t.Fatalf("entity record gets after materialization = %d, want 0", got)
	}
}

func TestPersistedIndexLookupUsesRecordHashesBeforeReadingEntityPage(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01", "app-02", "app-03"})
	if err != nil || !ok || len(ids) != 3 {
		t.Fatalf("field lookup ids=%#v ok=%v err=%v", ids, ok, err)
	}
	shard := entityShardID("host:app-01")
	lookup.pageMu.Lock()
	pageIndex := lookup.pageIndex[shard]
	lookup.pageMu.Unlock()
	if len(pageIndex) != 0 {
		t.Fatalf("page index entries = %d, want 0 when record hashes are sufficient", len(pageIndex))
	}
}

func TestPersistedIndexLookupDoesNotHeadEntityPagesDuringFieldLookup(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	ids := sameEntityShardIDs(t, "host:same-shard-head-", 4)
	entities := make([]graph.Entity, 0, len(ids))
	for _, id := range ids {
		entities = append(entities, graph.Entity{
			ID:     id,
			Kind:   "host",
			Fields: graph.Fields{"hostname": "shared"},
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
	counting := &countingHeadStore{ObjectStore: base}
	store.Objects = counting
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	got, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"shared"})
	if err != nil || !ok || len(got) != len(ids) {
		t.Fatalf("lookup ids=%#v ok=%v err=%v", got, ok, err)
	}
	pageKey := store.parquetEntityPageVersionKey("tenant-a", catalog.Version, entityShardID(ids[0]))
	if count := counting.HeadCount(pageKey); count != 0 {
		t.Fatalf("entity page HEAD count = %d, want 0 during field lookup", count)
	}
}

func TestPersistedLazyMatchReadsOnlyPageBoundaryEntityPages(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := newParquetIndexTenantStore(objects, "test")
	entities := make([]graph.Entity, 0, 20)
	for i := 0; i < 20; i++ {
		entities = append(entities, graph.Entity{
			ID:     fmt.Sprintf("host:shared-%02d", i),
			Kind:   "host",
			Fields: graph.Fields{"hostname": "shared"},
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
	objects.Reset()

	reader := newParquetIndexTenantStore(objects, "test")
	lookup := &PersistedIndexLookup{Store: reader, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	g := graph.New()
	g.Version = catalog.Version
	response, err := query.ExecuteContextWithOptions(ctx, g, query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "eq", Value: "shared"}},
		Limit: 5,
	}, query.ExecuteOptions{
		PlannerStats: catalog.PlannerStats(),
		IndexLookup:  lookup,
		EntityLookup: lookup,
	})
	if err != nil {
		t.Fatalf("lazy match: %v", err)
	}
	if len(response.Results) != 5 || response.NextCursor == "" {
		t.Fatalf("response=%#v, want first page with cursor", response)
	}
	if got := objects.CountContains("/indexes/entities/by-id/"); got != 0 {
		t.Fatalf("entity record gets = %d, want 0", got)
	}
}

func TestPersistedIndexLookupReturnsIsolatedEntities(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	entity, ok, err := lookup.GetEntity(ctx, "host:app-01", nil)
	if err != nil || !ok {
		t.Fatalf("entity ok=%v err=%v", ok, err)
	}
	entity.Fields["hostname"] = "poison"

	reloaded, ok, err := lookup.GetEntity(ctx, "host:app-01", nil)
	if err != nil || !ok {
		t.Fatalf("reloaded ok=%v err=%v", ok, err)
	}
	if reloaded.Fields["hostname"] != "app-01" {
		t.Fatalf("cached entity was mutated: %#v", reloaded.Fields)
	}
}

func TestRebuildIndexesTombstonesStaleEntityRecords(t *testing.T) {
	ctx := context.Background()
	objects := newCountingReadStore(NewMemoryStore())
	store := newParquetIndexTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	if err := store.Objects.Put(ctx, store.entityRecordKey("tenant-a", "host:stale"), []byte(`{"id":`)); err != nil {
		t.Fatalf("write stale invalid record: %v", err)
	}

	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild repair: %v", err)
	}
	record, err := store.loadEntityRecord(ctx, "tenant-a", "host:stale")
	if err != nil {
		t.Fatalf("load stale tombstone: %v", err)
	}
	if !record.Deleted || record.ID != "host:stale" {
		t.Fatalf("stale record was not tombstoned: %#v", record)
	}
}

func TestRebuildIndexesSkipsNonRecordEntityObjects(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	foreignKey := store.entityRecordPrefix("tenant-a") + "foreign"
	if err := store.Objects.Put(ctx, foreignKey, []byte("keep")); err != nil {
		t.Fatalf("write foreign object: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	got, err := store.Objects.Get(ctx, foreignKey)
	if err != nil {
		t.Fatalf("load foreign object: %v", err)
	}
	if string(got) != "keep" {
		t.Fatalf("foreign object changed to %q, want keep", string(got))
	}
}

func TestIndexHealthSkipsNonRecordEntityObjects(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	foreignKey := store.entityRecordPrefix("tenant-a") + "foreign"
	if err := store.Objects.Put(ctx, foreignKey, []byte("keep")); err != nil {
		t.Fatalf("write foreign object: %v", err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "ready" {
		t.Fatalf("health=%#v", health)
	}
}

func TestRebuildIndexesDoesNotTombstoneRecordAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := newParquetIndexTenantStore(base, "test")
	writer.LeaseTTL = time.Millisecond
	if _, err := writer.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := writer.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	staleKey := writer.entityRecordKey("tenant-a", "host:stale")
	if err := writer.Objects.Put(ctx, staleKey, []byte(`{"id":`)); err != nil {
		t.Fatalf("write stale invalid record: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	objects := &takeoverDuringStaleRecordCleanupStore{ObjectStore: base, base: base, tenantID: "tenant-a", triggerKey: staleKey}
	rebuilder := newParquetIndexTenantStore(objects, "test")
	rebuilder.LeaseTTL = time.Nanosecond
	_, err := rebuilder.RebuildIndexes(ctx, "tenant-a")
	if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("rebuild err = %v, want conflict after takeover", err)
	}
	if !objects.Triggered() {
		t.Fatal("test store did not trigger takeover")
	}

	record, err := writer.loadEntityRecord(ctx, "tenant-a", "host:stale")
	if err != nil {
		t.Fatalf("load record after takeover: %v", err)
	}
	if record.Deleted || record.Entity.ID != "host:stale" || record.Entity.Fields["hostname"] != "stale" {
		t.Fatalf("new record was tombstoned by stale rebuild: %#v", record)
	}
}

func TestIndexHealthReportsStaleAfterSchemaChange(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertCITypes: []graph.CIType{{
		Name: "host", Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}, "region": {Type: "string", Indexed: true}},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("schema commit: %v", err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "stale" {
		t.Fatalf("health = %#v", health)
	}
}

type countingReadStore struct {
	ObjectStore

	mu   sync.Mutex
	gets map[string]int
}

func newCountingReadStore(base ObjectStore) *countingReadStore {
	return &countingReadStore{ObjectStore: base, gets: map[string]int{}}
}

func (s *countingReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets[key]++
	s.mu.Unlock()
	return s.ObjectStore.Get(ctx, key)
}

func (s *countingReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.mu.Lock()
	s.gets[key]++
	s.mu.Unlock()
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *countingReadStore) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = map[string]int{}
}

func (s *countingReadStore) CountContains(fragment string) int {
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

type countingHeadStore struct {
	ObjectStore

	mu    sync.Mutex
	heads map[string]int
}

func (s *countingHeadStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	s.mu.Lock()
	if s.heads == nil {
		s.heads = map[string]int{}
	}
	s.heads[key]++
	s.mu.Unlock()
	return objectMeta(ctx, s.ObjectStore, key)
}

func (s *countingHeadStore) HeadCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heads[key]
}

func TestIndexHealthReportsCorruptPersistedIndexObjects(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := newParquetIndexTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	fieldIndex, fieldKey := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	fieldIndex.Values = map[string][]string{}
	writeParquetFieldIndexForTest(t, ctx, store, fieldKey, fieldIndex)
	shardID := edgeShardID("service:api")
	shard, shardKey := readParquetEdgeShardForTest(t, ctx, store, "tenant-a", catalog, "runs_on", shardID)
	shard.Edges = nil
	writeParquetEdgeShardForTest(t, ctx, store, shardKey, shard)
	if err := objects.Delete(ctx, store.entityRecordKey("tenant-a", "host:app-01")); err != nil {
		t.Fatalf("delete entity record: %v", err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" {
		t.Fatalf("health = %#v", health)
	}
	for _, want := range []string{
		"field index host.hostname entry count mismatch",
		"edge shard runs_on/" + shardID + " count mismatch",
	} {
		if !healthIssueContains(health, want) {
			t.Fatalf("health issues %q missing %q", health.Issues, want)
		}
	}
}

func TestIndexHealthReportsOrphanPersistedIndexObjects(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	fieldKey := store.parquetSecondaryIndexVersionKey("tenant-a", catalog.Version, "host", "orphan")
	edgeKey := store.parquetEdgeShardVersionKey("tenant-a", catalog.Version, "orphan_relation", "ff")
	pageKey := store.parquetEntityPageVersionKey("tenant-a", catalog.Version, "ff")
	writeParquetFieldIndexForTest(t, ctx, store, fieldKey, SecondaryIndex{
		TenantID: "tenant-a",
		Kind:     "host",
		Field:    "orphan",
		Values:   map[string][]string{"s:x": {"host:app-01"}},
		Version:  catalog.Version,
	})
	writeParquetEdgeShardForTest(t, ctx, store, edgeKey, EdgeShardData{
		TenantID:     "tenant-a",
		RelationType: "orphan_relation",
		Shard:        "ff",
		Version:      catalog.Version,
	})
	writeParquetEntityPageForTest(t, ctx, store, pageKey, EntityPageData{
		TenantID: "tenant-a",
		Shard:    "ff",
		Version:  catalog.Version,
	})
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	for _, key := range []string{fieldKey, edgeKey, pageKey} {
		if !healthIssueContains(health, "orphan index object "+key) {
			t.Fatalf("health issues %q missing orphan %s", health.Issues, key)
		}
	}
	if health.Status != "ready" {
		t.Fatalf("health = %#v", health)
	}
}

func TestIndexHealthSkipsVanishedListedOrphanIndexObject(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	fieldKey := store.parquetSecondaryIndexVersionKey("tenant-a", catalog.Version, "host", "vanished")
	writeParquetFieldIndexForTest(t, ctx, store, fieldKey, SecondaryIndex{
		TenantID: "tenant-a",
		Kind:     "host",
		Field:    "vanished",
		Values:   map[string][]string{"s:x": {"host:app-01"}},
		Version:  catalog.Version,
	})
	objects := &deleteListedObjectStore{ObjectStore: base, key: fieldKey}
	store.Objects = objects

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if !objects.deleted {
		t.Fatal("test store did not delete the listed orphan object")
	}
	if health.Status != "ready" {
		t.Fatalf("health = %#v, want ready after stale list entry is skipped", health)
	}
}

func TestIndexHealthReportsEdgeShardContentMismatch(t *testing.T) {
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
	shard, key := readParquetEdgeShardForTest(t, ctx, store, "tenant-a", catalog, "runs_on", shardID)
	shard.Edges[0].To = "host:app-02"
	shard.Edges[0].ID = graph.CanonicalEdgeID(shard.Edges[0])
	writeParquetEdgeShardForTest(t, ctx, store, key, shard)
	quick, err := store.IndexHealthWithOptions(ctx, "tenant-a", IndexHealthOptions{})
	if err != nil {
		t.Fatalf("quick health: %v", err)
	}
	if quick.Status != "ready" {
		t.Fatalf("quick health = %#v, want ready without deep content scan", quick)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthIssueContains(health, "edge shard runs_on/"+shardID+" content mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestIndexHealthAllowsMissingOptionalEntityRecordInPage(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := newParquetIndexTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if err := objects.Delete(ctx, store.entityRecordKey("tenant-a", "host:app-02")); err != nil {
		t.Fatalf("delete middle entity record: %v", err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "ready" {
		t.Fatalf("health = %#v, want ready when optional entity record is missing", health)
	}
}

func TestIndexHealthReportsEntityRecordContentMismatch(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	record, err := store.loadEntityRecord(ctx, "tenant-a", "host:app-02")
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	record.Entity.Fields["hostname"] = "stale"
	if err := writeEntityRecordForTest(ctx, store, store.entityRecordKey("tenant-a", "host:app-02"), record); err != nil {
		t.Fatalf("corrupt record: %v", err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthIssueContains(health, "entity record host:app-02 content mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestIndexHealthReportsEntityPageContentMismatchWithGraph(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	shardID := entityShardID("host:app-02")
	stalePage, _ := readParquetEntityPageForTest(t, ctx, store, "tenant-a", catalog, shardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02-current"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("update entity: %v", err)
	}
	currentCatalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("current catalog: %v", err)
	}
	pageKey := requireIndexObjectKey(t, requireEntityPageSpec(t, currentCatalog, shardID).Objects, "page")
	writeParquetEntityPageForTest(t, ctx, store, pageKey, stalePage)
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthIssueContains(health, "entity page "+shardID+" content mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestIndexHealthReportsEntityPageMetadataMissing(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	shardID := entityShardID("host:app-02")
	pageKey := requireIndexObjectKey(t, requireEntityPageSpec(t, catalog, shardID).Objects, "page")
	if err := base.Delete(ctx, pageKey); err != nil {
		t.Fatalf("delete entity page: %v", err)
	}

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthIssueContains(health, "missing entity page "+shardID) {
		t.Fatalf("health = %#v", health)
	}
}

func TestPersistedLookupRejectsFieldIndexWithMismatchedTopValues(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	index, indexKey := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	index.Values = map[string][]string{"s:app-01": {"host:app-01", "host:app-02"}}
	writeParquetFieldIndexForTest(t, ctx, store, indexKey, index)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"})
	if err != nil || ok || len(ids) != 0 {
		t.Fatalf("lookup ids=%#v ok=%v err=%v, want unavailable", ids, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthIssueContains(health, "field index host.hostname top values mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestPersistedLookupRejectsFieldIndexWithMismatchedIDs(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", multiHostIndexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	index, indexKey := readParquetFieldIndexForTest(t, ctx, store, "tenant-a", catalog, "host", "hostname")
	index.Values["s:app-01"] = []string{"host:app-02"}
	writeParquetFieldIndexForTest(t, ctx, store, indexKey, index)

	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"})
	if err != nil || ok || len(ids) != 0 {
		t.Fatalf("lookup ids=%#v ok=%v err=%v, want unavailable", ids, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthIssueContains(health, "field index host.hostname content mismatch") {
		t.Fatalf("health = %#v", health)
	}
}

func TestIncrementalIndexUsesResolvedIdentityTarget(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
				"region":   {Type: "string", Indexed: true},
			},
			IdentityKeys: []graph.IdentityKey{{Name: "hostname", Fields: []string{"hostname"}}},
		}},
		UpsertEntities: []graph.Entity{{
			ID: "host:canonical", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "r1"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:alias", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "r2"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("identity merge commit: %v", err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if catalog.Version != 2 {
		t.Fatalf("catalog version = %d, want 2", catalog.Version)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "region", []any{"r2"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:canonical" {
		t.Fatalf("region r2 ids=%#v ok=%v err=%v", ids, ok, err)
	}
	ids, ok, err = lookup.MatchFieldIndex(ctx, "host", "region", []any{"r1"})
	if err != nil || !ok || len(ids) != 0 {
		t.Fatalf("region r1 ids=%#v ok=%v err=%v", ids, ok, err)
	}
	entity, ok, err := lookup.GetEntity(ctx, "host:canonical", nil)
	if err != nil || !ok || entity.Fields["region"] != "r2" {
		t.Fatalf("canonical entity=%#v ok=%v err=%v", entity, ok, err)
	}
	if _, ok, err := lookup.GetEntity(ctx, "host:alias", nil); err != nil || ok {
		t.Fatalf("alias lookup ok=%v err=%v", ok, err)
	}
}

func multiHostIndexMutations() graph.Mutations {
	return graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
			{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02"}},
			{ID: "host:app-03", Kind: "host", Fields: graph.Fields{"hostname": "app-03"}},
		},
	}
}

func twoHostEdgeMutations() graph.Mutations {
	return graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:app-01", Kind: "host"},
			{ID: "host:app-02", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host-1", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}
}

func healthIssueContains(health IndexHealth, want string) bool {
	for _, issue := range health.Issues {
		if strings.Contains(issue, want) {
			return true
		}
	}
	return false
}

func writeEntityRecordForTest(ctx context.Context, store *TenantStore, key string, record EntityRecord) error {
	meta, err := objectMeta(ctx, store.Objects, key)
	if errors.Is(err, ErrNotFound) {
		meta = ObjectMeta{Key: key}
	} else if err != nil {
		return err
	}
	return store.putEntityRecordWithMeta(ctx, key, record, meta)
}

func catalogField(catalog IndexCatalog, kind string, field string) (IndexSpec, bool) {
	for _, index := range catalog.Indexes {
		if index.Kind == kind && index.Field == field {
			return index, true
		}
	}
	return IndexSpec{}, false
}

func catalogEntityPage(catalog IndexCatalog, shard string) (EntityPageSpec, bool) {
	for _, page := range catalog.EntityPages {
		if page.Shard == shard {
			return page, true
		}
	}
	return EntityPageSpec{}, false
}

func sameEntityShardIDs(t *testing.T, prefix string, count int) []string {
	t.Helper()
	byShard := map[string][]string{}
	for i := 0; i < 2048; i++ {
		id := fmt.Sprintf("%s%04d", prefix, i)
		shard := entityShardID(id)
		byShard[shard] = append(byShard[shard], id)
		if len(byShard[shard]) >= count {
			return append([]string(nil), byShard[shard][:count]...)
		}
	}
	t.Fatalf("could not find %d ids in one entity shard", count)
	return nil
}

func TestIndexShardIDsDistributeCommonCMDBPrefixes(t *testing.T) {
	entityShards := map[string]struct{}{}
	edgeShards := map[string]struct{}{}
	for i := 0; i < 64; i++ {
		entityShards[entityShardID(fmt.Sprintf("host:app-%04d", i))] = struct{}{}
		edgeShards[edgeShardID(fmt.Sprintf("service:api-%04d", i))] = struct{}{}
	}
	if len(entityShards) < 16 {
		t.Fatalf("entity shards for common host prefix = %d, want distributed", len(entityShards))
	}
	if len(edgeShards) < 16 {
		t.Fatalf("edge shards for common service prefix = %d, want distributed", len(edgeShards))
	}
}

func TestIncrementalIndexSkipsMissingAndNonScalarFields(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"tags": {Type: "any", Indexed: true}}}},
		UpsertEntities: []graph.Entity{{
			ID: "host:scalar", Kind: "host", Fields: graph.Fields{"tags": "blue"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "host:missing", Kind: "host"},
		{ID: "host:scalar", Kind: "host", Fields: graph.Fields{"tags": map[string]any{"env": "prod"}}},
	}}, CommitOptions{}); err != nil {
		t.Fatalf("incremental: %v", err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "tags", []any{"blue"})
	if err != nil || !ok || len(ids) != 0 {
		t.Fatalf("old scalar ids=%#v ok=%v err=%v", ids, ok, err)
	}
	ids, ok, err = lookup.MatchFieldIndex(ctx, "host", "tags", []any{nil})
	if err != nil || !ok || len(ids) != 0 {
		t.Fatalf("missing field was indexed as null: ids=%#v ok=%v err=%v", ids, ok, err)
	}
	ids, ok, err = lookup.MatchFieldIndex(ctx, "host", "tags", []any{map[string]any{"env": "prod"}})
	if err != nil || ok {
		t.Fatalf("non-scalar lookup should fall back: ids=%#v ok=%v err=%v", ids, ok, err)
	}
}

func TestIncrementalIndexRemovesEdgesForDeletedEntity(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	edges, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}})
	if err != nil || !ok || len(edges) != 1 {
		t.Fatalf("edge lookup before delete edges=%#v ok=%v err=%v", edges, ok, err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{"host:app-01"}}, CommitOptions{}); err != nil {
		t.Fatalf("delete entity: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog after delete: %v", err)
	}
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	edges, ok, err = lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}})
	if err != nil || !ok || len(edges) != 0 {
		t.Fatalf("edge lookup after delete edges=%#v ok=%v err=%v", edges, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func TestPersistedEntityLookupProjectionAndIncrementalDelete(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}, "region": {Type: "string"}},
		}},
		UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "us-east-1"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	entity, ok, err := lookup.GetEntity(ctx, "host:app-01", []string{"hostname"})
	if err != nil || !ok {
		t.Fatalf("entity lookup ok=%v err=%v", ok, err)
	}
	if _, ok := entity.Fields["hostname"]; !ok || len(entity.Fields) != 1 {
		t.Fatalf("projected entity = %#v", entity)
	}
	if _, ok := entity.FieldSources["hostname"]; !ok || len(entity.FieldSources) != 1 {
		t.Fatalf("projected field_sources = %#v, want only hostname", entity.FieldSources)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	entity, ok, err = lookup.GetEntity(ctx, "host:app-01", []string{"hostname"})
	if err != nil || !ok || entity.Fields["hostname"] != "app-02" {
		t.Fatalf("updated entity=%#v ok=%v err=%v", entity, ok, err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{"host:app-01"}}, CommitOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog after delete: %v", err)
	}
	lookup = &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if _, ok, err = lookup.GetEntity(ctx, "host:app-01", nil); err != nil || ok {
		t.Fatalf("deleted lookup ok=%v err=%v", ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func TestIncrementalEntityPageRewriteRefreshesSiblingRecords(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	ids := sameEntityShardIDs(t, "host:page-test-", 3)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: ids[0], Kind: "host", Fields: graph.Fields{"hostname": "one"}},
			{ID: ids[1], Kind: "host", Fields: graph.Fields{"hostname": "two"}},
			{ID: ids[2], Kind: "host", Fields: graph.Fields{"hostname": "three"}},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{ids[1]}}, CommitOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	shardID := entityShardID(ids[0])
	spec, ok := catalogEntityPage(catalog, shardID)
	if !ok || spec.ContentHash == "" {
		t.Fatalf("catalog page spec = %#v ok=%v", spec, ok)
	}
	for _, id := range []string{ids[0], ids[2]} {
		record, err := store.loadEntityRecord(ctx, "tenant-a", id)
		if err != nil {
			t.Fatalf("load record %s: %v", id, err)
		}
		if record.PageHash != spec.ContentHash || record.PageETag == "" {
			t.Fatalf("record %s hash/etag = %q/%q, want page hash %q and etag", id, record.PageHash, record.PageETag, spec.ContentHash)
		}
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil || health.Status != "ready" {
		t.Fatalf("health=%#v err=%v", health, err)
	}
}

func TestPersistedEntityLookupRejectsStalePageHashMismatch(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	shardID := entityShardID("host:app-01")
	stalePage, _ := readParquetEntityPageForTest(t, ctx, store, "tenant-a", catalog, shardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("update: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	pageKey := requireIndexObjectKey(t, requireEntityPageSpec(t, catalog, shardID).Objects, "page")
	writeParquetEntityPageForTest(t, ctx, store, pageKey, stalePage)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if entity, ok, err := lookup.GetEntity(ctx, "host:app-01", nil); err != nil || ok {
		t.Fatalf("stale lookup entity=%#v ok=%v err=%v, want unavailable", entity, ok, err)
	}
}

func TestPersistedEdgeLookupRejectsStaleShardHashMismatch(t *testing.T) {
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
	staleShard, _ := readParquetEdgeShardForTest(t, ctx, store, "tenant-a", catalog, "runs_on", shardID)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		DeleteEdges: []string{graph.CanonicalEdgeIDParts("runs_on", "service:api", "host:app-01")},
		UpsertEdges: []graph.Edge{{
			ID: "edge:api-host-2", Type: "runs_on", From: "service:api", To: "host:app-02",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("replace edge: %v", err)
	}
	catalog, err = store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	shardKey := requireIndexObjectKey(t, requireEdgeShardSpec(t, catalog, "runs_on", shardID).Objects, "shard")
	writeParquetEdgeShardForTest(t, ctx, store, shardKey, staleShard)
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if edges, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}}); err != nil || ok {
		t.Fatalf("stale edge lookup edges=%#v ok=%v err=%v, want unavailable", edges, ok, err)
	}
}

func TestPersistedEntityLookupRejectsStaleRecordMissingFromCurrentPage(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "a"}},
			{ID: "host:b", Kind: "host", Fields: graph.Fields{"hostname": "b"}},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	stale, err := store.loadEntityRecord(ctx, "tenant-a", "host:a")
	if err != nil {
		t.Fatalf("load stale record: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{"host:a"}}, CommitOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if err := writeEntityRecordForTest(ctx, store, store.entityRecordKey("tenant-a", "host:a"), stale); err != nil {
		t.Fatalf("restore stale record: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if entity, ok, err := lookup.GetEntity(ctx, "host:a", nil); err != nil || ok {
		t.Fatalf("stale entity lookup entity=%#v ok=%v err=%v", entity, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "stale entity record host:a") {
		t.Fatalf("health=%#v", health)
	}
}

func TestIndexHealthSkipsVanishedListedEntityRecord(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := newParquetIndexTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:live", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	staleKey := store.entityRecordKey("tenant-a", "host:stale")
	if err := writeEntityRecordForTest(ctx, store, staleKey, EntityRecord{
		TenantID: "tenant-a",
		ID:       "host:stale",
		Entity:   graph.Entity{ID: "host:stale", Kind: "host"},
		Version:  1,
	}); err != nil {
		t.Fatalf("put stale record: %v", err)
	}
	objects := &deleteListedObjectStore{ObjectStore: base, key: staleKey}
	store.Objects = objects

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if !objects.deleted {
		t.Fatal("test store did not delete the listed entity record")
	}
	if health.Status != "ready" {
		t.Fatalf("health = %#v, want ready after stale list entry is skipped", health)
	}
}

func TestIndexHealthReportsDeletedOptionalRecordForLiveEntity(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:app-01", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	key := store.entityRecordKey("tenant-a", "host:app-01")
	record, err := store.loadEntityRecord(ctx, "tenant-a", "host:app-01")
	if err != nil {
		t.Fatalf("load record: %v", err)
	}
	record.Deleted = true
	if err := writeEntityRecordForTest(ctx, store, key, record); err != nil {
		t.Fatalf("write tombstoned record: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if entity, ok, err := lookup.GetEntity(ctx, "host:app-01", nil); err != nil || !ok || entity.ID != "host:app-01" {
		t.Fatalf("deleted live record lookup entity=%#v ok=%v err=%v, want page-backed entity", entity, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "entity record host:app-01 is deleted") {
		t.Fatalf("health=%#v", health)
	}
}

func TestPersistedLookupRejectsHashlessCatalogObjects(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	for i := range catalog.Indexes {
		catalog.Indexes[i].ContentHash = ""
	}
	for i := range catalog.EdgeShards {
		catalog.EdgeShards[i].ContentHash = ""
	}
	for i := range catalog.EntityPages {
		catalog.EntityPages[i].ContentHash = ""
	}
	writeParquetIndexCatalogForTest(t, ctx, store, "tenant-a", catalog)

	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	if ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"}); err != nil || ok || len(ids) != 0 {
		t.Fatalf("hashless field lookup ids=%#v ok=%v err=%v, want unavailable", ids, ok, err)
	}
	if edges, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}}); err != nil || ok || len(edges) != 0 {
		t.Fatalf("hashless edge lookup edges=%#v ok=%v err=%v, want unavailable", edges, ok, err)
	}
	if entity, ok, err := lookup.GetEntity(ctx, "host:app-01", nil); err != nil || ok {
		t.Fatalf("hashless entity lookup entity=%#v ok=%v err=%v, want unavailable", entity, ok, err)
	}

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	edgeShard := edgeShardID("service:api")
	entityPage := entityShardID("host:app-01")
	if health.Status != "error" ||
		!healthIssueContains(health, "field index host.hostname content hash missing") ||
		!healthIssueContains(health, "edge shard runs_on/"+edgeShard+" content hash missing") ||
		!healthIssueContains(health, "entity page "+entityPage+" content hash missing") {
		t.Fatalf("health=%#v", health)
	}
}

func TestRebuildIndexesRemovesStaleEntityRecord(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "a"}},
			{ID: "host:b", Kind: "host", Fields: graph.Fields{"hostname": "b"}},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	stale, err := store.loadEntityRecord(ctx, "tenant-a", "host:a")
	if err != nil {
		t.Fatalf("load stale record: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteEntities: []string{"host:a"}}, CommitOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := writeEntityRecordForTest(ctx, store, store.entityRecordKey("tenant-a", "host:a"), stale); err != nil {
		t.Fatalf("restore stale record: %v", err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health before rebuild: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "stale entity record host:a") {
		t.Fatalf("health before rebuild=%#v", health)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild repair: %v", err)
	}
	health, err = store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health after rebuild: %v", err)
	}
	if health.Status != "ready" {
		t.Fatalf("health after rebuild=%#v", health)
	}
}

func TestRebuildIndexesRemovesInvalidEntityRecord(t *testing.T) {
	ctx := context.Background()
	store := newParquetIndexTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "a"}},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	brokenKey := store.entityRecordKey("tenant-a", "host:broken")
	if err := store.Objects.Put(ctx, brokenKey, []byte(`{"id":`)); err != nil {
		t.Fatalf("write broken record: %v", err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health before rebuild: %v", err)
	}
	if health.Status != "error" || !healthHasIssue(health, "invalid entity record "+brokenKey) {
		t.Fatalf("health before rebuild=%#v", health)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild repair: %v", err)
	}
	health, err = store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health after rebuild: %v", err)
	}
	if health.Status != "ready" {
		t.Fatalf("health after rebuild=%#v", health)
	}
}

func healthHasIssue(health IndexHealth, issue string) bool {
	for _, got := range health.Issues {
		if got == issue {
			return true
		}
	}
	return false
}

type takeoverDuringIncrementalIndexStore struct {
	ObjectStore
	base     *MemoryStore
	tenantID string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverDuringIncrementalIndexStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *takeoverDuringIncrementalIndexStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		time.Sleep(time.Millisecond)
		takeover := newParquetIndexTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.Commit(ctx, s.tenantID, graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
		}}}, CommitOptions{}); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

type missingHeadStore struct {
	ObjectStore
	key string
}

func (s *missingHeadStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if key == s.key {
		return ObjectMeta{Key: key}, ErrNotFound
	}
	if head, ok := s.ObjectStore.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err := s.ObjectStore.GetWithMeta(ctx, key)
	return meta, err
}

type deleteListedObjectStore struct {
	ObjectStore
	key     string
	deleted bool
}

func (s *deleteListedObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items, err := s.ObjectStore.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	if s.deleted {
		return items, nil
	}
	for _, item := range items {
		if item.Key != s.key {
			continue
		}
		if err := s.ObjectStore.Delete(ctx, item.Key); err != nil {
			return nil, err
		}
		s.deleted = true
		break
	}
	return items, nil
}

func (s *takeoverDuringIncrementalIndexStore) shouldTrigger(key string) bool {
	if !strings.Contains(key, "/indexes/") || strings.HasSuffix(key, "/catalog.parquet") {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *takeoverDuringIncrementalIndexStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

type takeoverDuringIncrementalDeleteStore struct {
	ObjectStore
	base     *MemoryStore
	tenantID string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverDuringIncrementalDeleteStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		time.Sleep(time.Millisecond)
		takeover := newParquetIndexTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.Commit(ctx, s.tenantID, graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
		}}}, CommitOptions{}); err != nil {
			return ObjectMeta{}, err
		}
		if _, err := takeover.RebuildIndexes(ctx, s.tenantID); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *takeoverDuringIncrementalDeleteStore) shouldTrigger(key string) bool {
	if !strings.Contains(key, "/indexes/entities/by-id/") {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *takeoverDuringIncrementalDeleteStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

type takeoverDuringStaleRecordCleanupStore struct {
	ObjectStore
	base       *MemoryStore
	tenantID   string
	triggerKey string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverDuringStaleRecordCleanupStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		time.Sleep(time.Millisecond)
		takeover := newParquetIndexTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.Commit(ctx, s.tenantID, graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:stale", Kind: "host", Fields: graph.Fields{"hostname": "stale"},
		}}}, CommitOptions{}); err != nil {
			return ObjectMeta{}, err
		}
		if _, err := takeover.RebuildIndexes(ctx, s.tenantID); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *takeoverDuringStaleRecordCleanupStore) shouldTrigger(key string) bool {
	if key != s.triggerKey {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *takeoverDuringStaleRecordCleanupStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

type failConditionalDeleteStore struct {
	ObjectStore
}

func (s *failConditionalDeleteStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	if condition.IfMatch != "" {
		return ErrConflict
	}
	return s.ObjectStore.DeleteConditional(ctx, key, condition)
}
