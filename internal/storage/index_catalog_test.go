package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"graphdb/internal/graph"
	"graphdb/internal/query"
)

func TestRebuildIndexesWritesCatalogSecondaryIndexesAndEdgeShards(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if catalog.Version != 1 || len(catalog.Indexes) != 1 || len(catalog.EdgeShards) != 1 || len(catalog.EntityPages) != 2 {
		t.Fatalf("catalog = %#v", catalog)
	}
	if catalog.TenantID != "tenant-a" {
		t.Fatalf("catalog tenant = %q, want tenant-a", catalog.TenantID)
	}
	loaded, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get catalog: %v", err)
	}
	if loaded.TenantID != "tenant-a" {
		t.Fatalf("loaded catalog tenant = %q, want tenant-a", loaded.TenantID)
	}
	if loaded.Indexes[0].Name != "host.hostname" {
		t.Fatalf("loaded catalog = %#v", loaded)
	}
	if loaded.Indexes[0].EntryCount != 1 || loaded.Indexes[0].DistinctValues != 1 || len(loaded.Indexes[0].TopValues) != 1 {
		t.Fatalf("index stats = %#v", loaded.Indexes[0])
	}
	catalogData, err := objects.Get(ctx, store.indexCatalogKey("tenant-a"))
	if err != nil {
		t.Fatalf("get catalog object: %v", err)
	}
	if !strings.HasSuffix(store.indexCatalogKey("tenant-a"), ".parquet") || !isParquetBytes(catalogData) {
		t.Fatalf("catalog object key=%q parquet=%v", store.indexCatalogKey("tenant-a"), isParquetBytes(catalogData))
	}
	versionedCatalogData, err := objects.Get(ctx, store.indexCatalogVersionKey("tenant-a", catalog.Version))
	if err != nil {
		t.Fatalf("get versioned catalog object: %v", err)
	}
	if !strings.HasSuffix(store.indexCatalogVersionKey("tenant-a", catalog.Version), ".parquet") || !isParquetBytes(versionedCatalogData) {
		t.Fatalf("versioned catalog object key=%q parquet=%v", store.indexCatalogVersionKey("tenant-a", catalog.Version), isParquetBytes(versionedCatalogData))
	}
	objectsWritten, err := objects.List(ctx, "test/tenants/tenant-a/indexes/")
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	if !hasObject(objectsWritten, "/indexes/parquet/versions/v1/fields/host/hostname/shards/") {
		t.Fatalf("secondary index shard object missing: %#v", objectsWritten)
	}
	if !hasObject(objectsWritten, "/indexes/parquet/versions/v1/edges/runs_on/") {
		t.Fatalf("edge shard object missing: %#v", objectsWritten)
	}
	if !hasObject(objectsWritten, "/indexes/parquet/versions/v1/entities/pages/") || !hasObject(objectsWritten, "/entities/by-id/") {
		t.Fatalf("entity page objects missing: %#v", objectsWritten)
	}
}

func TestRebuildIndexesDoesNotPublishStaleCatalogAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	writer.LeaseTTL = time.Millisecond
	if _, err := writer.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, err := writer.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild v1: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	objects := &takeoverDuringIndexWriteStore{ObjectStore: base, base: base, tenantID: "tenant-a"}
	rebuilder := NewTenantStore(objects, "test")
	rebuilder.LeaseTTL = time.Nanosecond
	_, err := rebuilder.RebuildIndexes(ctx, "tenant-a")
	if !errors.Is(err, ErrLeaseHeld) && !errors.Is(err, ErrConflict) {
		t.Fatalf("rebuild err = %v, want ErrLeaseHeld or ErrConflict", err)
	}
	if !objects.Triggered() {
		t.Fatal("test store did not trigger takeover")
	}

	catalog, err := writer.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get catalog: %v", err)
	}
	if catalog.Version != 2 {
		t.Fatalf("catalog version = %d, want takeover version 2", catalog.Version)
	}
	lookup := PersistedIndexLookup{Store: writer, TenantID: "tenant-a", Version: 2, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-02"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-02" {
		t.Fatalf("takeover index ids=%#v ok=%v err=%v", ids, ok, err)
	}
}

func TestRebuildIndexesDoesNotOverwriteCatalogAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	writer.LeaseTTL = time.Millisecond
	if _, err := writer.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	if _, err := writer.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild v1: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	objects := &takeoverBeforeCatalogPublishStore{ObjectStore: base, base: base, tenantID: "tenant-a"}
	rebuilder := NewTenantStore(objects, "test")
	rebuilder.LeaseTTL = time.Nanosecond
	_, err := rebuilder.RebuildIndexes(ctx, "tenant-a")
	if objects.Triggered() {
		if !errors.Is(err, ErrConflict) && !errors.Is(err, ErrLeaseHeld) {
			t.Fatalf("rebuild err = %v, want ErrConflict or ErrLeaseHeld", err)
		}
	} else if err != nil {
		t.Fatalf("no-op rebuild err = %v", err)
	}

	catalog, err := writer.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get catalog: %v", err)
	}
	if objects.Triggered() {
		if catalog.Version != 2 {
			t.Fatalf("catalog version = %d, want takeover version 2", catalog.Version)
		}
		lookup := PersistedIndexLookup{Store: writer, TenantID: "tenant-a", Version: 2, Catalog: catalog}
		ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-02"})
		if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-02" {
			t.Fatalf("takeover index ids=%#v ok=%v err=%v", ids, ok, err)
		}
	} else if catalog.Version != 1 {
		t.Fatalf("catalog version = %d, want unchanged version 1", catalog.Version)
	}
}

func TestGetIndexCatalogRejectsCrossTenantObject(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	data, err := marshalParquetIndexCatalog(ctx, IndexCatalog{
		TenantID:  "tenant-b",
		Version:   7,
		UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := store.Objects.Put(ctx, store.indexCatalogKey("tenant-a"), data); err != nil {
		t.Fatalf("put catalog: %v", err)
	}
	if _, err := store.GetIndexCatalog(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "index catalog tenant mismatch") {
		t.Fatalf("catalog err = %v, want tenant mismatch", err)
	}
}

func TestGetIndexCatalogAcceptsParquetCatalogWithoutTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	data, err := marshalParquetIndexCatalog(ctx, IndexCatalog{
		Version:   7,
		UpdatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("marshal catalog: %v", err)
	}
	if err := store.Objects.Put(ctx, store.indexCatalogKey("tenant-a"), data); err != nil {
		t.Fatalf("put catalog: %v", err)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get catalog: %v", err)
	}
	if catalog.TenantID != "tenant-a" || catalog.Version != 7 {
		t.Fatalf("catalog = %#v", catalog)
	}
}

func TestRebuildIndexesWritesObjectsConcurrently(t *testing.T) {
	ctx := context.Background()
	objects := newCountingIndexStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	mutations := graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}}}},
	}
	for i := 0; i < 64; i++ {
		id := "host:app-" + string(rune('a'+i%26)) + "-" + string(rune('a'+i/26))
		mutations.UpsertEntities = append(mutations.UpsertEntities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"hostname": id}})
	}
	if _, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	objects.TrackIndexes = true
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if objects.MaxInflight() < 2 {
		t.Fatalf("index writes were serialized, max inflight = %d", objects.MaxInflight())
	}
}

func TestRebuildIndexesSkipsUnchangedIndexObjectWrites(t *testing.T) {
	ctx := context.Background()
	objects := newCountingIndexStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}

	objects.TrackIndexes = true
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("first rebuild indexes: %v", err)
	}
	firstWrites := objects.PutCount("/indexes/")
	if firstWrites == 0 {
		t.Fatal("first rebuild did not write index data objects")
	}

	objects.ResetPutCounts()
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("second rebuild indexes: %v", err)
	}
	if writes := objects.PutCount("/indexes/"); writes != 0 {
		t.Fatalf("second rebuild wrote %d unchanged index data objects", writes)
	}
}

func TestSavedQueryRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	saved, err := store.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:    "hosts-by-region",
		Request: query.Request{Op: "match", Kind: "host", Where: []query.Filter{{Field: "region", Op: "eq", Value: "us-east-1"}}},
	})
	if err != nil {
		t.Fatalf("save query: %v", err)
	}
	loaded, err := store.GetSavedQuery(ctx, "tenant-a", saved.Name)
	if err != nil {
		t.Fatalf("get query: %v", err)
	}
	if loaded.TenantID != "tenant-a" || loaded.Request.Kind != "host" || loaded.CreatedAt.IsZero() || loaded.UpdatedAt.IsZero() {
		t.Fatalf("loaded query = %#v", loaded)
	}
	data, err := store.Objects.Get(ctx, store.savedQueryKey("tenant-a", saved.Name))
	if err != nil {
		t.Fatalf("get saved query object: %v", err)
	}
	if !isParquetBytes(data) {
		t.Fatal("saved query object is not parquet")
	}
	listed, err := store.ListSavedQueries(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("list queries: %v", err)
	}
	if len(listed) != 1 || listed[0].Name != saved.Name || listed[0].TenantID != "tenant-a" {
		t.Fatalf("listed queries = %#v", listed)
	}
}

func TestListSavedQueriesSkipsInvalidObjects(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:    "hosts",
		Request: query.Request{Op: "match", Kind: "host"},
	}); err != nil {
		t.Fatalf("save query: %v", err)
	}
	if err := store.Objects.Put(ctx, store.savedQueryKey("tenant-a", "broken-json"), []byte(`{"name":`)); err != nil {
		t.Fatalf("put broken query: %v", err)
	}
	if err := store.Objects.Put(ctx, store.savedQueryKey("tenant-a", "wrong-type"), []byte(`[]`)); err != nil {
		t.Fatalf("put wrong-type query: %v", err)
	}
	if err := putSavedQueryFixture(ctx, store, store.savedQueryKey("tenant-a", "empty-name"), SavedQuery{}); err != nil {
		t.Fatalf("put empty-name query: %v", err)
	}
	if err := putSavedQueryFixture(ctx, store, store.savedQueryKey("tenant-a", "wrong-name"), SavedQuery{Name: "other-name"}); err != nil {
		t.Fatalf("put wrong-name query: %v", err)
	}
	if err := putSavedQueryFixture(ctx, store, store.savedQueryKey("tenant-a", "wrong-tenant"), SavedQuery{TenantID: "tenant-b", Name: "wrong-tenant"}); err != nil {
		t.Fatalf("put wrong-tenant query: %v", err)
	}
	listed, err := store.ListSavedQueries(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("list queries: %v", err)
	}
	if len(listed) != 1 || listed[0].Name != "hosts" {
		t.Fatalf("listed queries = %#v", listed)
	}
}

func TestGetSavedQueryRejectsMismatchedObjectName(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putSavedQueryFixture(ctx, store, store.savedQueryKey("tenant-a", "hosts"), SavedQuery{Name: "other-name"}); err != nil {
		t.Fatalf("put query: %v", err)
	}
	if _, err := store.GetSavedQuery(ctx, "tenant-a", " hosts "); err == nil || !strings.Contains(err.Error(), "saved query identity mismatch") {
		t.Fatalf("get query err = %v, want identity mismatch", err)
	}
}

func TestGetSavedQueryRejectsMismatchedTenant(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putSavedQueryFixture(ctx, store, store.savedQueryKey("tenant-a", "hosts"), SavedQuery{TenantID: "tenant-b", Name: "hosts"}); err != nil {
		t.Fatalf("put query: %v", err)
	}
	if _, err := store.GetSavedQuery(ctx, "tenant-a", "hosts"); err == nil || !strings.Contains(err.Error(), "saved query tenant mismatch") {
		t.Fatalf("get query err = %v, want tenant mismatch", err)
	}
}

func TestSavedQueryAcceptsRecordWithoutTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putSavedQueryFixture(ctx, store, store.savedQueryKey("tenant-a", "hosts"), SavedQuery{
		Name:    "hosts",
		Request: query.Request{Op: "match", Kind: "host"},
	}); err != nil {
		t.Fatalf("put query: %v", err)
	}
	loaded, err := store.GetSavedQuery(ctx, "tenant-a", "hosts")
	if err != nil {
		t.Fatalf("get legacy query: %v", err)
	}
	if loaded.TenantID != "tenant-a" || loaded.Name != "hosts" {
		t.Fatalf("loaded legacy query = %#v", loaded)
	}
	listed, err := store.ListSavedQueries(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("list legacy queries: %v", err)
	}
	if len(listed) != 1 || listed[0].TenantID != "tenant-a" || listed[0].Name != "hosts" {
		t.Fatalf("listed legacy queries = %#v", listed)
	}
}

func putSavedQueryFixture(ctx context.Context, store *TenantStore, key string, saved SavedQuery) error {
	data, err := marshalParquetSavedQuery(ctx, saved)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, key, data)
}

func TestListSavedQueriesSkipsVanishedListedObject(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(&deleteListedSavedQueryStore{ObjectStore: base}, "test")
	if _, err := store.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:    "hosts",
		Request: query.Request{Op: "match", Kind: "host"},
	}); err != nil {
		t.Fatalf("save hosts query: %v", err)
	}
	if _, err := store.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:    "vanish",
		Request: query.Request{Op: "match", Kind: "host"},
	}); err != nil {
		t.Fatalf("save vanishing query: %v", err)
	}
	listed, err := store.ListSavedQueries(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("list queries: %v", err)
	}
	if len(listed) != 1 || listed[0].Name != "hosts" {
		t.Fatalf("listed queries = %#v", listed)
	}
}

func TestIndexRebuildTaskRecoversPanicAndMarksFailed(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	task := IndexTask{ID: "task-panic", TenantID: "tenant-a", Type: "rebuild", Status: "running", StartedAt: time.Now().UTC()}
	if err := store.saveIndexTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	store.Objects = &panicOnIndexWriteStore{ObjectStore: base, panicFragment: "/indexes/parquet/"}
	store.runIndexRebuildTask("tenant-a", task)

	loaded, err := store.GetIndexTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Status != "failed" || !strings.Contains(loaded.Error, "panic:") || loaded.FinishedAt.IsZero() {
		t.Fatalf("task = %#v", loaded)
	}
}

func TestIndexRebuildTaskRetriesFinalStatusSave(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	task := IndexTask{ID: "task-flaky", TenantID: "tenant-a", Type: "rebuild", Status: "running", StartedAt: time.Now().UTC()}
	if err := store.saveIndexTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	store.Objects = &failOnceTaskStatusStore{ObjectStore: base, fragment: "/indexes/tasks/task-flaky.parquet"}
	store.runIndexRebuildTask("tenant-a", task)

	loaded, err := store.GetIndexTask(ctx, "tenant-a", task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Status != "succeeded" || loaded.CatalogVersion == 0 || loaded.FinishedAt.IsZero() {
		t.Fatalf("task = %#v", loaded)
	}
}

func TestGetIndexTaskRejectsMismatchedTaskObject(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putIndexTaskFixture(ctx, store, store.indexTaskKey("tenant-a", "task-a"), IndexTask{
		ID:        "task-a",
		TenantID:  "tenant-b",
		Type:      "rebuild",
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("put cross-tenant task: %v", err)
	}
	if _, err := store.GetIndexTask(ctx, "tenant-a", "task-a"); err == nil || !strings.Contains(err.Error(), "tenant mismatch") {
		t.Fatalf("cross-tenant task err = %v, want tenant mismatch", err)
	}
	if err := putIndexTaskFixture(ctx, store, store.indexTaskKey("tenant-a", "task-a"), IndexTask{
		ID:        "task-b",
		TenantID:  "tenant-a",
		Type:      "rebuild",
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("put mismatched task id: %v", err)
	}
	if _, err := store.GetIndexTask(ctx, "tenant-a", "task-a"); err == nil || !strings.Contains(err.Error(), "id mismatch") {
		t.Fatalf("mismatched task id err = %v, want id mismatch", err)
	}
}

func TestStartIndexRebuildDeduplicatesRunningTenantTask(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	blocking := &blockOncePutStore{
		ObjectStore: base,
		substring:   "/indexes/parquet/",
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	store := NewTenantStore(blocking, "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	first, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start first rebuild: %v", err)
	}
	select {
	case <-blocking.paused:
	case <-time.After(time.Second):
		t.Fatal("first rebuild did not reach blocked index write")
	}
	second, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start duplicate rebuild: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate rebuild task id = %q, want existing %q", second.ID, first.ID)
	}
	close(blocking.resume)
	finished := waitIndexTask(t, store, first.ID)
	if finished.Status != "succeeded" {
		t.Fatalf("first task = %#v", finished)
	}

	third, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start rebuild after finish: %v", err)
	}
	if third.ID == first.ID {
		t.Fatalf("new rebuild reused finished task id %q", third.ID)
	}
	finished = waitIndexTask(t, store, third.ID)
	if finished.Status != "succeeded" {
		t.Fatalf("third task = %#v", finished)
	}
}

func TestIndexRebuildTaskSkipsEntityRecordCleanup(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("initial rebuild: %v", err)
	}
	if _, err := store.loadEntityRecord(ctx, "tenant-a", "host:app-01"); err != nil {
		t.Fatalf("entity record before task: %v", err)
	}
	task, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start rebuild: %v", err)
	}
	finished := waitIndexTask(t, store, task.ID)
	if finished.Status != "succeeded" {
		t.Fatalf("task = %#v", finished)
	}
	if _, err := store.loadEntityRecord(ctx, "tenant-a", "host:app-01"); err != nil {
		t.Fatalf("entity record after task: %v", err)
	}
}

func TestStartIndexRebuildDeduplicatesPersistedRunningTenantTask(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	blocking := &blockOncePutStore{
		ObjectStore: base,
		substring:   "/indexes/parquet/",
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	firstStore := NewTenantStore(blocking, "test")
	secondStore := NewTenantStore(blocking, "test")
	if _, err := firstStore.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	first, err := firstStore.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start first rebuild: %v", err)
	}
	select {
	case <-blocking.paused:
	case <-time.After(time.Second):
		t.Fatal("first rebuild did not reach blocked index write")
	}
	second, err := secondStore.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start duplicate rebuild from second store: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate persisted rebuild task id = %q, want existing %q", second.ID, first.ID)
	}
	close(blocking.resume)
	finished := waitIndexTask(t, firstStore, first.ID)
	if finished.Status != "succeeded" {
		t.Fatalf("first task = %#v", finished)
	}
}

func TestStartIndexRebuildIgnoresStalePersistedRunningTask(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	stale := IndexTask{
		ID:        "stale-task",
		TenantID:  "tenant-a",
		Type:      "rebuild",
		Status:    "running",
		OwnerID:   "dead-owner",
		StartedAt: time.Now().Add(-time.Hour).UTC(),
		UpdatedAt: time.Now().Add(-time.Hour).UTC(),
	}
	if err := store.saveIndexTask(ctx, stale); err != nil {
		t.Fatalf("save stale task: %v", err)
	}
	task, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start rebuild: %v", err)
	}
	if task.ID == stale.ID {
		t.Fatalf("reused stale task id %q", task.ID)
	}
	finished := waitIndexTask(t, store, task.ID)
	if finished.Status != "succeeded" || finished.OwnerID != store.InstanceID || finished.UpdatedAt.IsZero() {
		t.Fatalf("new task = %#v", finished)
	}
}

func TestStartIndexRebuildIgnoresInvalidPersistedTaskObjects(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", indexMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if err := store.Objects.Put(ctx, store.indexTaskKey("tenant-a", "broken-json"), []byte(`{"id":`)); err != nil {
		t.Fatalf("put broken task: %v", err)
	}
	if err := putIndexTaskFixture(ctx, store, store.indexTaskKey("tenant-a", "cross-tenant"), IndexTask{
		ID:        "cross-tenant",
		TenantID:  "tenant-b",
		Type:      "rebuild",
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("put cross tenant task: %v", err)
	}
	if err := putIndexTaskFixture(ctx, store, store.indexTaskKey("tenant-a", "wrong-id"), IndexTask{
		ID:        "other-id",
		TenantID:  "tenant-a",
		Type:      "rebuild",
		Status:    "running",
		StartedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("put wrong id task: %v", err)
	}

	task, err := store.StartIndexRebuild(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("start rebuild: %v", err)
	}
	if task.ID == "broken-json" || task.ID == "cross-tenant" || task.ID == "wrong-id" {
		t.Fatalf("reused invalid task: %#v", task)
	}
	finished := waitIndexTask(t, store, task.ID)
	if finished.Status != "succeeded" || finished.OwnerID != store.InstanceID {
		t.Fatalf("new task = %#v", finished)
	}
}

func putIndexTaskFixture(ctx context.Context, store *TenantStore, key string, task IndexTask) error {
	data, err := marshalParquetIndexTask(ctx, task)
	if err != nil {
		return err
	}
	return store.Objects.Put(ctx, key, data)
}

type panicOnIndexWriteStore struct {
	ObjectStore
	panicFragment string
}

func (s *panicOnIndexWriteStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *panicOnIndexWriteStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if strings.Contains(key, s.panicFragment) {
		panic("panic writing " + key)
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

type failOnceTaskStatusStore struct {
	ObjectStore
	fragment string

	mu     sync.Mutex
	failed bool
}

func (s *failOnceTaskStatusStore) Put(ctx context.Context, key string, data []byte) error {
	if s.shouldFail(key) {
		return errors.New("injected task status write failure")
	}
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *failOnceTaskStatusStore) shouldFail(key string) bool {
	if !strings.Contains(key, s.fragment) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed {
		return false
	}
	s.failed = true
	return true
}

type countingIndexStore struct {
	ObjectStore
	TrackIndexes bool

	mu          sync.Mutex
	inflight    int
	maxInflight int
	putCounts   map[string]int
}

func newCountingIndexStore(base ObjectStore) *countingIndexStore {
	return &countingIndexStore{ObjectStore: base, putCounts: map[string]int{}}
}

func (s *countingIndexStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *countingIndexStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.TrackIndexes && isIndexDataObject(key) {
		s.mu.Lock()
		s.putCounts[key]++
		s.inflight++
		if s.inflight > s.maxInflight {
			s.maxInflight = s.inflight
		}
		s.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		defer func() {
			s.mu.Lock()
			s.inflight--
			s.mu.Unlock()
		}()
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *countingIndexStore) MaxInflight() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInflight
}

func (s *countingIndexStore) PutCount(fragment string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for key, writes := range s.putCounts {
		if strings.Contains(key, fragment) {
			count += writes
		}
	}
	return count
}

func (s *countingIndexStore) ResetPutCounts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putCounts = map[string]int{}
}

func isIndexDataObject(key string) bool {
	return strings.Contains(key, "/indexes/") &&
		!strings.HasSuffix(key, "/catalog.parquet") &&
		!strings.Contains(key, "/indexes/tasks/")
}

func indexMutations() graph.Mutations {
	return graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
			},
		}, {Name: "service"}},
		UpsertRelationTypes: []graph.RelationType{{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
			{ID: "service:api", Kind: "service"},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:api-host", Type: "runs_on", From: "service:api", To: "host:app-01"}},
	}
}

func hasObject(objects []ObjectInfo, fragment string) bool {
	for _, object := range objects {
		if strings.Contains(object.Key, fragment) {
			return true
		}
	}
	return false
}

type blockOncePutStore struct {
	ObjectStore
	substring string
	paused    chan struct{}
	resume    chan struct{}

	mu      sync.Mutex
	blocked bool
}

func (s *blockOncePutStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *blockOncePutStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldBlock(key) {
		close(s.paused)
		select {
		case <-s.resume:
		case <-ctx.Done():
			return ObjectMeta{}, ctx.Err()
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *blockOncePutStore) shouldBlock(key string) bool {
	if !strings.Contains(key, s.substring) {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blocked {
		return false
	}
	s.blocked = true
	return true
}

type takeoverDuringIndexWriteStore struct {
	ObjectStore
	base     *MemoryStore
	tenantID string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverDuringIndexWriteStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *takeoverDuringIndexWriteStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		if err := s.takeover(ctx); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *takeoverDuringIndexWriteStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if s.shouldTrigger(key) {
		if err := s.takeover(ctx); err != nil {
			return nil, ObjectMeta{}, err
		}
	}
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *takeoverDuringIndexWriteStore) takeover(ctx context.Context) error {
	time.Sleep(time.Millisecond)
	takeover := NewTenantStore(s.base, "test")
	takeover.LeaseTTL = time.Hour
	_, err := takeover.Commit(ctx, s.tenantID, graph.Mutations{
		UpsertEntities: []graph.Entity{{
			ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
		}},
	}, CommitOptions{})
	return err
}

func (s *takeoverDuringIndexWriteStore) shouldTrigger(key string) bool {
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

func (s *takeoverDuringIndexWriteStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

type takeoverBeforeCatalogPublishStore struct {
	ObjectStore
	base     *MemoryStore
	tenantID string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverBeforeCatalogPublishStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *takeoverBeforeCatalogPublishStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		time.Sleep(time.Millisecond)
		takeover := NewTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.Commit(ctx, s.tenantID, graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02"},
			}},
		}, CommitOptions{}); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *takeoverBeforeCatalogPublishStore) shouldTrigger(key string) bool {
	if !strings.HasSuffix(key, "/indexes/catalog.parquet") {
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

func (s *takeoverBeforeCatalogPublishStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}

type deleteListedSavedQueryStore struct {
	ObjectStore
}

func (s *deleteListedSavedQueryStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items, err := s.ObjectStore.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if strings.Contains(item.Key, "vanish.parquet") {
			if err := s.ObjectStore.Delete(ctx, item.Key); err != nil {
				return nil, err
			}
			break
		}
	}
	return items, nil
}

func waitIndexTask(t *testing.T, store *TenantStore, taskID string) IndexTask {
	t.Helper()
	for i := 0; i < 50; i++ {
		task, err := store.GetIndexTask(context.Background(), "tenant-a", taskID)
		if err == nil && task.Status != "running" {
			return task
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			t.Fatalf("get task: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	task, err := store.GetIndexTask(context.Background(), "tenant-a", taskID)
	if err != nil {
		t.Fatalf("get final task: %v", err)
	}
	return task
}
