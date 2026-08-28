package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestTenantStoreCommitLoadAndCompact(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	manifest, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if manifest.Version != 1 || len(manifest.CommitKeys) != 1 {
		t.Fatalf("unexpected manifest after commit: %#v", manifest)
	}

	g, loaded, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Version != 1 {
		t.Fatalf("loaded version = %d, want 1", loaded.Version)
	}
	if _, ok := g.GetEntity("person:alice"); !ok {
		t.Fatal("person:alice missing after load")
	}

	compacted, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if compacted.SnapshotKey == "" || len(compacted.CommitKeys) != 0 {
		t.Fatalf("unexpected compacted manifest: %#v", compacted)
	}
	if compacted.SnapshotCatalogKey == "" {
		t.Fatalf("compacted manifest missing sharded snapshot catalog: %#v", compacted)
	}
	catalog, _, err := store.CurrentShardedSnapshotCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("snapshot catalog: %v", err)
	}
	if catalog.Format != snapshotFormatParquetSharded {
		t.Fatalf("snapshot catalog format = %q, want %s", catalog.Format, snapshotFormatParquetSharded)
	}
	if !strings.HasSuffix(catalog.Key, ".parquet") || !strings.HasSuffix(catalog.Schema.Key, ".parquet") || catalog.Schema.Format != snapshotSchemaFormatParquet {
		t.Fatalf("snapshot catalog/schema = %#v", catalog)
	}
	catalogBytes, err := store.Objects.Get(ctx, catalog.Key)
	if err != nil {
		t.Fatalf("get snapshot catalog: %v", err)
	}
	schemaBytes, err := store.Objects.Get(ctx, catalog.Schema.Key)
	if err != nil {
		t.Fatalf("get snapshot schema: %v", err)
	}
	if !isParquetBytes(catalogBytes) || !isParquetBytes(schemaBytes) {
		t.Fatalf("snapshot catalog/schema parquet = %v/%v", isParquetBytes(catalogBytes), isParquetBytes(schemaBytes))
	}
	if !strings.HasSuffix(compacted.SnapshotKey, ".parquet") {
		t.Fatalf("snapshot key = %q, want parquet", compacted.SnapshotKey)
	}
	snapshotBytes, err := store.Objects.Get(ctx, compacted.SnapshotKey)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if !isParquetBytes(snapshotBytes) {
		t.Fatal("snapshot record is not parquet")
	}
	for _, page := range catalog.EntityPages {
		if page.Format != IndexFormatParquet || !strings.HasSuffix(page.Key, ".parquet") {
			t.Fatalf("snapshot entity page spec = %#v, want parquet", page)
		}
	}
	for _, shard := range catalog.EdgeShards {
		if shard.Format != IndexFormatParquet || !strings.HasSuffix(shard.Key, ".parquet") {
			t.Fatalf("snapshot edge shard spec = %#v, want parquet", shard)
		}
	}
	record, err := store.loadSnapshotRecord(ctx, compacted.SnapshotKey)
	if err != nil {
		t.Fatalf("load raw snapshot: %v", err)
	}
	if record.TenantID != "tenant-a" || record.Version != compacted.SnapshotVersion {
		t.Fatalf("snapshot record = %#v, want tenant-a version %d", record, compacted.SnapshotVersion)
	}
	recompacted, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("repeat compact: %v", err)
	}
	if recompacted.SnapshotKey != compacted.SnapshotKey || recompacted.SnapshotVersion != compacted.SnapshotVersion {
		t.Fatalf("repeat compacted manifest = %#v, want snapshot %q version %d", recompacted, compacted.SnapshotKey, compacted.SnapshotVersion)
	}
	reloaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load compacted: %v", err)
	}
	if _, ok := reloaded.GetEntity("company:acme"); !ok {
		t.Fatal("company:acme missing after compact load")
	}
}

func TestCompactSnapshotBuildDoesNotBlockCommit(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	blocking := &blockOncePutStore{
		ObjectStore: base,
		substring:   "/snapshots/sharded/",
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	store := NewTenantStore(blocking, "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	compactDone := make(chan error, 1)
	go func() {
		_, err := store.Compact(ctx, "tenant-a")
		compactDone <- err
	}()
	select {
	case <-blocking.paused:
	case <-time.After(time.Second):
		t.Fatal("compact did not reach blocked snapshot write")
	}

	commitCtx, cancel := context.WithTimeout(ctx, time.Second)
	_, commitErr := store.Commit(commitCtx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:bob", Kind: "person"}},
	}, CommitOptions{})
	cancel()
	close(blocking.resume)
	compactErr := <-compactDone
	if commitErr != nil {
		t.Fatalf("commit was blocked by compact snapshot build: %v", commitErr)
	}
	if !errors.Is(compactErr, ErrConflict) {
		t.Fatalf("stale compact err = %v, want ErrConflict", compactErr)
	}

	g, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load after concurrent commit: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("manifest version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("person:bob"); !ok {
		t.Fatal("concurrent commit entity missing")
	}
}

func TestLoadUsesShardedSnapshotWhenFullSnapshotMissing(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	manifest, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if manifest.SnapshotCatalogKey == "" {
		t.Fatalf("snapshot catalog key is empty: %#v", manifest)
	}
	if err := store.Objects.Delete(ctx, manifest.SnapshotKey); err != nil {
		t.Fatalf("delete full snapshot: %v", err)
	}
	store.deleteWriteCache("tenant-a")

	g, loaded, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load from sharded snapshot: %v", err)
	}
	if loaded.Version != manifest.Version {
		t.Fatalf("loaded version = %d, want %d", loaded.Version, manifest.Version)
	}
	if _, ok := g.GetEntity("company:acme"); !ok {
		t.Fatal("company:acme missing after sharded snapshot load")
	}
}

func TestLoadRetriesWhenSnapshotObjectDisappearsAfterManifestRead(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	if _, err := writer.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	first, err := writer.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact v1: %v", err)
	}
	first.SnapshotCatalogKey = ""
	_, firstMeta, err := writer.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get v1 manifest: %v", err)
	}
	if _, err := writer.putManifestMeta(ctx, "tenant-a", first, firstMeta); err != nil {
		t.Fatalf("downgrade v1 manifest to full snapshot record: %v", err)
	}
	writer.deleteWriteCache("tenant-a")
	pausing := &pauseOnceReadStore{
		ObjectStore: base,
		substring:   first.SnapshotKey,
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	reader := NewTenantStore(pausing, "test")

	done := make(chan struct {
		g        *graph.Graph
		manifest Manifest
		err      error
	}, 1)
	go func() {
		g, manifest, err := reader.Load(ctx, "tenant-a")
		done <- struct {
			g        *graph.Graph
			manifest Manifest
			err      error
		}{g: g, manifest: manifest, err: err}
	}()
	select {
	case <-pausing.paused:
	case <-time.After(time.Second):
		t.Fatal("reader did not pause on first snapshot read")
	}

	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:bob", Kind: "person"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	second, err := writer.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact v2: %v", err)
	}
	if second.SnapshotKey == first.SnapshotKey {
		t.Fatalf("snapshot key did not advance: %q", second.SnapshotKey)
	}
	if _, err := writer.RunGC(ctx, "tenant-a", GCOptions{KeepSnapshots: 1}); err != nil {
		t.Fatalf("gc: %v", err)
	}
	if _, err := base.Get(ctx, first.SnapshotKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old snapshot err = %v, want ErrNotFound", err)
	}
	close(pausing.resume)

	result := <-done
	if result.err != nil {
		t.Fatalf("load after snapshot GC: %v", result.err)
	}
	if result.manifest.Version != second.Version {
		t.Fatalf("loaded version = %d, want %d", result.manifest.Version, second.Version)
	}
	if _, ok := result.g.GetEntity("person:bob"); !ok {
		t.Fatal("retried load did not include entity from latest snapshot")
	}
}

func TestLoadDoesNotHideMissingObjectFromCurrentManifest(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	key := store.snapshotKey("tenant-a", 1)
	manifest := Manifest{
		TenantID:        "tenant-a",
		Version:         1,
		SnapshotKey:     key,
		SnapshotVersion: 1,
		UpdatedAt:       time.Now().UTC(),
	}
	if _, err := store.putManifestMeta(ctx, "tenant-a", manifest, ObjectMeta{Key: store.manifestKey("tenant-a")}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	_, _, err := store.Load(ctx, "tenant-a")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("load err = %v, want ErrNotFound", err)
	}
}

func TestCompactDoesNotOverwriteDifferentSnapshotObject(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	manifest, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	key := store.snapshotKey("tenant-a", manifest.Version)
	conflicting := snapshotRecord{
		TenantID: "tenant-a",
		Snapshot: graph.Snapshot{
			Version: manifest.Version,
			Entities: []graph.Entity{
				{ID: "host:conflict", Kind: "host"},
			},
		},
	}
	if err := store.putSnapshotRecordIfAbsentOrEquivalent(ctx, key, conflicting); err != nil {
		t.Fatalf("put conflicting snapshot: %v", err)
	}
	_, err = store.Compact(ctx, "tenant-a")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("compact err = %v, want ErrConflict", err)
	}
	current, _, err := store.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if current.SnapshotKey != "" || len(current.CommitKeys) != len(manifest.CommitKeys) {
		t.Fatalf("manifest changed after failed compact: %#v", current)
	}
}

func TestCompactAcceptsExistingEquivalentParquetSnapshotObject(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	manifest, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	key := store.snapshotKey("tenant-a", manifest.Version)
	record := snapshotRecord{TenantID: "tenant-a", Snapshot: g.Snapshot()}
	data, err := marshalParquetSnapshotRecord(ctx, record)
	if err != nil {
		t.Fatalf("marshal parquet snapshot: %v", err)
	}
	if err := store.Objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put parquet snapshot: %v", err)
	}
	compacted, err := store.Compact(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if compacted.SnapshotKey != key || compacted.SnapshotVersion != manifest.Version {
		t.Fatalf("compacted = %#v, want snapshot %q version %d", compacted, key, manifest.Version)
	}
}

func TestTenantStoreMergeSplitPersistFieldSources(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{
		{ID: "host:a", Kind: "host", Source: "manual", SourceRank: 1000, Fields: graph.Fields{"owner": "platform"}},
		{ID: "host:b", Kind: "host", Source: "agent", SourceRank: 100, Fields: graph.Fields{"region": "us-east-1"}},
	}}, CommitOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{MergeEntities: []graph.MergeRequest{{
		TargetID:  "host:a",
		SourceIDs: []string{"host:b"},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("merge: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load merged: %v", err)
	}
	merged, _ := g.GetEntity("host:a")
	if merged.FieldSources["region"].Source != "agent" || merged.FieldSources["region"].Priority != 100 {
		t.Fatalf("merged field source = %#v", merged.FieldSources["region"])
	}

	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{SplitEntities: []graph.SplitRequest{{
		SourceID: "host:a",
		Entities: []graph.Entity{{
			ID: "host:a1", Kind: "host", Source: "manual", SourceRank: 1000, Fields: graph.Fields{"owner": "platform"},
		}},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("split: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	g, _, err = store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load split: %v", err)
	}
	split, _ := g.GetEntity("host:a1")
	if split.FieldSources["owner"].Source != "manual" || split.FieldSources["owner"].Priority != 1000 {
		t.Fatalf("split field source = %#v", split.FieldSources["owner"])
	}
}

func TestTenantStoreIsolatesTenants(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit tenant-a: %v", err)
	}
	g, manifest, err := store.Load(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("load tenant-b: %v", err)
	}
	if manifest.Version != 0 {
		t.Fatalf("tenant-b version = %d, want 0", manifest.Version)
	}
	if _, ok := g.GetEntity("person:alice"); ok {
		t.Fatal("tenant-b can see tenant-a entity")
	}
}

func TestLoadRejectsCrossTenantManifest(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{TenantID: "tenant-b"}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "tenant mismatch") {
		t.Fatalf("load err = %v, want tenant mismatch", err)
	}
}

func TestLoadRejectsManifestWithoutTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{Version: 0}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "tenant mismatch") {
		t.Fatalf("load err = %v, want tenant mismatch", err)
	}
}

func TestPutManifestStampsTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := store.putManifest(ctx, "tenant-a", Manifest{Version: 0}, ObjectMeta{Key: store.manifestKey("tenant-a")}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	manifest, _, err := store.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if manifest.TenantID != "tenant-a" {
		t.Fatalf("manifest tenant = %q, want tenant-a", manifest.TenantID)
	}
}

func TestLoadRejectsCrossTenantCommitKey(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-b", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit tenant-b: %v", err)
	}
	manifestB, _, err := store.getManifest(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("manifest tenant-b: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:   "tenant-a",
		Version:    1,
		CommitKeys: append([]string(nil), manifestB.CommitKeys...),
	}); err != nil {
		t.Fatalf("put manifest tenant-a: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "outside tenant prefix") {
		t.Fatalf("load err = %v, want outside tenant prefix", err)
	}
}

func TestLoadRejectsCrossTenantSnapshotKey(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	snapshot := graph.Snapshot{
		Version: 1,
		Entities: []graph.Entity{
			{ID: "host:b", Kind: "host"},
		},
	}
	snapshotKey := store.snapshotKey("tenant-b", snapshot.Version)
	if err := store.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, snapshotRecord{TenantID: "tenant-b", Snapshot: snapshot}); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:        "tenant-a",
		Version:         1,
		SnapshotKey:     snapshotKey,
		SnapshotVersion: 1,
	}); err != nil {
		t.Fatalf("put manifest tenant-a: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "outside tenant prefix") {
		t.Fatalf("load err = %v, want outside tenant prefix", err)
	}
}

func TestLoadRejectsCrossTenantSnapshotObject(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	snapshot := graph.Snapshot{
		Version: 1,
		Entities: []graph.Entity{
			{ID: "host:b", Kind: "host"},
		},
	}
	snapshotKey := store.snapshotKey("tenant-a", snapshot.Version)
	if err := store.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, snapshotRecord{TenantID: "tenant-b", Snapshot: snapshot}); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:        "tenant-a",
		Version:         1,
		SnapshotKey:     snapshotKey,
		SnapshotVersion: 1,
	}); err != nil {
		t.Fatalf("put manifest tenant-a: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "snapshot tenant mismatch") {
		t.Fatalf("load err = %v, want snapshot tenant mismatch", err)
	}
}

func TestLoadRejectsSnapshotKeyIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	snapshot := graph.Snapshot{
		Version: 2,
		Entities: []graph.Entity{
			{ID: "host:b", Kind: "host"},
		},
	}
	snapshotKey := store.snapshotKey("tenant-a", 1)
	if err := store.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, snapshotRecord{TenantID: "tenant-a", Snapshot: snapshot}); err != nil {
		t.Fatalf("put snapshot: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:        "tenant-a",
		Version:         2,
		SnapshotKey:     snapshotKey,
		SnapshotVersion: 2,
	}); err != nil {
		t.Fatalf("put manifest tenant-a: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "snapshot identity mismatch") {
		t.Fatalf("load err = %v, want snapshot identity mismatch", err)
	}
}

func TestLoadRejectsNonParquetSnapshotRecord(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	snapshot := graph.Snapshot{
		Version: 1,
		Entities: []graph.Entity{
			{ID: "host:a", Kind: "host"},
		},
	}
	snapshotKey := store.snapshotKey("tenant-a", snapshot.Version)
	if err := store.Objects.Put(ctx, snapshotKey, []byte(`{"version":1,"entities":[{"id":"host:a","kind":"host"}]}`)); err != nil {
		t.Fatalf("put non-parquet snapshot: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:        "tenant-a",
		Version:         1,
		SnapshotKey:     snapshotKey,
		SnapshotVersion: 1,
	}); err != nil {
		t.Fatalf("put manifest tenant-a: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "only parquet snapshots") {
		t.Fatalf("load err = %v, want parquet-only snapshot rejection", err)
	}
}

func TestLoadRejectsManifestCommitWithoutTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	commit := graph.Commit{
		ID:      "missing-tenant-v1",
		Version: 1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
		},
	}
	commitKey := store.commitKey("tenant-a", commit.Version, commit.ID)
	if err := store.putCommitObjectIfAbsent(ctx, commitKey, commit); err != nil {
		t.Fatalf("put commit: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:   "tenant-a",
		Version:    1,
		CommitKeys: []string{commitKey},
	}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "tenant mismatch") {
		t.Fatalf("load err = %v, want tenant mismatch", err)
	}
}

func TestLoadRejectsManifestCommitKeyIdentityMismatch(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	commit := graph.Commit{
		ID:       "object-v1",
		TenantID: "tenant-a",
		Version:  1,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
		},
	}
	commitKey := store.commitKey("tenant-a", commit.Version, "path-v1")
	if err := store.putCommitObjectIfAbsent(ctx, commitKey, commit); err != nil {
		t.Fatalf("put commit: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:   "tenant-a",
		Version:    1,
		CommitKeys: []string{commitKey},
	}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "commit identity mismatch") {
		t.Fatalf("load err = %v, want commit identity mismatch", err)
	}
}

func TestLoadRejectsManifestVersionAheadOfGraph(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID: "tenant-a",
		Version:  2,
	}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "manifest version mismatch") {
		t.Fatalf("load err = %v, want manifest version mismatch", err)
	}
}

func TestLoadRejectsNonContiguousManifestCommitKeys(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	commit := graph.Commit{
		ID:       "v2",
		TenantID: "tenant-a",
		Version:  2,
		Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
		},
	}
	commitKey := store.commitKey("tenant-a", commit.Version, commit.ID)
	if err := store.putCommitObjectIfAbsent(ctx, commitKey, commit); err != nil {
		t.Fatalf("put commit: %v", err)
	}
	if err := putManifestFixture(ctx, store, "tenant-a", Manifest{
		TenantID:   "tenant-a",
		Version:    2,
		CommitKeys: []string{commitKey},
	}); err != nil {
		t.Fatalf("put manifest: %v", err)
	}
	if _, _, err := store.Load(ctx, "tenant-a"); err == nil || !strings.Contains(err.Error(), "non-contiguous commit version") {
		t.Fatalf("load err = %v, want non-contiguous commit version", err)
	}
}

func TestPublicReadAPIsRejectInvalidTenantID(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	invalidTenant := "../tenant-a"
	checks := []struct {
		name string
		fn   func() error
	}{
		{name: "get index catalog", fn: func() error {
			_, err := store.GetIndexCatalog(ctx, invalidTenant)
			return err
		}},
		{name: "get saved query", fn: func() error {
			_, err := store.GetSavedQuery(ctx, invalidTenant, "hosts")
			return err
		}},
		{name: "list saved queries", fn: func() error {
			_, err := store.ListSavedQueries(ctx, invalidTenant)
			return err
		}},
		{name: "get collector status", fn: func() error {
			_, err := store.GetCollectorStatus(ctx, invalidTenant, "aws", "collector-a")
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			err := check.fn()
			if err == nil || !strings.Contains(err.Error(), "invalid tenant id") {
				t.Fatalf("err = %v, want invalid tenant id", err)
			}
		})
	}
}

func TestExpectedVersion(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	expected := int64(7)
	_, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{ExpectedVersion: &expected})
	if err == nil {
		t.Fatal("expected version mismatch error")
	}
}

func TestManifestConditionalWriteConflict(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	manifest, staleMeta, err := store.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:bob", Kind: "person"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	manifest.Version = 99
	err = store.putManifest(ctx, "tenant-a", manifest, staleMeta)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale manifest write error = %v, want ErrConflict", err)
	}
}

func TestTenantStoreLoadBypassesStaleWriteCache(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	first := NewTenantStore(objects, "test")
	if _, err := first.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	second := NewTenantStore(objects, "test")
	second.InstanceID = first.InstanceID
	if _, err := second.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	g, manifest, err := first.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("loaded version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("load returned stale write cache without host:b")
	}
	g, manifest, err = first.LoadAtLeast(ctx, "tenant-a", 2)
	if err != nil {
		t.Fatalf("load at least: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("load at least version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("host:b"); !ok {
		t.Fatal("load at least returned stale write cache without host:b")
	}
}

func TestCommitExpectedVersionBypassesStaleWriteCache(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	first := NewTenantStore(objects, "test")
	if _, err := first.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	second := NewTenantStore(objects, "test")
	second.InstanceID = first.InstanceID
	if _, err := second.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	expected := int64(2)
	manifest, err := first.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:c", Kind: "host"}},
	}, CommitOptions{ExpectedVersion: &expected})
	if err != nil {
		t.Fatalf("commit with expected version after external advance: %v", err)
	}
	if manifest.Version != 3 {
		t.Fatalf("manifest version = %d, want 3", manifest.Version)
	}
	g, _, err := first.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	for _, id := range []string{"host:a", "host:b", "host:c"} {
		if _, ok := g.GetEntity(id); !ok {
			t.Fatalf("%s missing after expected-version commit", id)
		}
	}
}

func TestReaderCacheKeepsHotGraphAndRefreshesAfterTTL(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	cache := NewReaderCache(store, time.Nanosecond)
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	_, manifest, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cache load v1: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("cached version = %d, want 1", manifest.Version)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:bob", Kind: "person"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	time.Sleep(time.Millisecond)
	g, manifest, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cache load v2: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("refreshed version = %d, want 2", manifest.Version)
	}
	if _, ok := g.GetEntity("person:bob"); !ok {
		t.Fatal("refreshed cache missing person:bob")
	}
}

func TestReaderCacheReturnsIsolatedGraphCopies(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	cache := NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	g, _, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	delete(g.Entities, "person:alice")
	g.Entities["person:poison"] = graph.Entity{ID: "person:poison", Kind: "person"}

	reloaded, _, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if _, ok := reloaded.GetEntity("person:alice"); !ok {
		t.Fatal("cached graph was mutated by caller")
	}
	if _, ok := reloaded.GetEntity("person:poison"); ok {
		t.Fatal("caller mutation leaked into cached graph")
	}
}

func TestReaderCacheRefreshCachedEvictsIdleTenants(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	cache := NewReaderCache(store, time.Minute)
	cache.IdleTTL = time.Minute
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("load: %v", err)
	}
	cache.mu.Lock()
	entry := cache.entries["tenant-a"]
	entry.lastAccess = time.Now().Add(-2 * time.Minute)
	entry.expiresAt = time.Now().Add(time.Minute)
	cache.entries["tenant-a"] = entry
	cache.mu.Unlock()

	cache.RefreshCached(ctx)
	if cacheHasTenant(cache, "tenant-a") {
		t.Fatal("idle tenant remained in reader cache")
	}
}

func TestReaderCacheRefreshCachedKeepsActiveStaleTenant(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	cache := NewReaderCache(store, time.Minute)
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("load: %v", err)
	}
	cache.mu.Lock()
	entry := cache.entries["tenant-a"]
	entry.lastAccess = time.Now()
	entry.expiresAt = time.Now().Add(-time.Minute)
	cache.entries["tenant-a"] = entry
	cache.mu.Unlock()

	cache.RefreshCached(ctx)
	cache.mu.RLock()
	entry, ok := cache.entries["tenant-a"]
	cache.mu.RUnlock()
	if !ok {
		t.Fatal("active tenant was evicted from reader cache")
	}
	if !entry.expiresAt.After(time.Now()) {
		t.Fatalf("active stale tenant was not refreshed: expires_at=%s", entry.expiresAt.Format(time.RFC3339Nano))
	}
}

func TestReaderCacheExpirationStartsAfterSlowLoad(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	store.Objects = &slowReadStore{ObjectStore: base, delay: 20 * time.Millisecond}
	cache := NewReaderCache(store, 5*time.Millisecond)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("cache load: %v", err)
	}
	cache.mu.RLock()
	entry := cache.entries["tenant-a"]
	cache.mu.RUnlock()
	if !entry.expiresAt.After(time.Now()) {
		t.Fatalf("cache entry expired during load: expires_at=%s now=%s", entry.expiresAt.Format(time.RFC3339Nano), time.Now().Format(time.RFC3339Nano))
	}
}

func TestReaderCacheSlowMissDoesNotBlockOtherTenantHit(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-hot", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit hot tenant: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-slow", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit slow tenant: %v", err)
	}
	cache := NewReaderCache(store, time.Minute)
	if _, _, err := cache.Load(ctx, "tenant-hot"); err != nil {
		t.Fatalf("warm hot tenant: %v", err)
	}
	started := make(chan struct{})
	store.Objects = &slowReadStore{
		ObjectStore:     base,
		delay:           100 * time.Millisecond,
		notifySubstring: "/tenants/tenant-slow/manifest.parquet",
		started:         started,
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := cache.Load(ctx, "tenant-slow")
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow tenant load did not start")
	}
	start := time.Now()
	if _, manifest, err := cache.Load(ctx, "tenant-hot"); err != nil || manifest.Version != 1 {
		t.Fatalf("hot tenant cache load manifest=%#v err=%v", manifest, err)
	}
	if elapsed := time.Since(start); elapsed > 30*time.Millisecond {
		t.Fatalf("hot tenant cache hit was blocked by slow miss: %s", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatalf("slow tenant load: %v", err)
	}
}

func TestReaderCacheConcurrentTenantMissesShareLoad(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	counting := &countingDelayReadStore{
		ObjectStore: base,
		substring:   "/tenants/tenant-a/commits/",
		delay:       50 * time.Millisecond,
	}
	store.Objects = counting
	cache := NewReaderCache(store, time.Minute)

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			g, manifest, err := cache.Load(ctx, "tenant-a")
			if err != nil {
				errs <- err
				return
			}
			if manifest.Version != 1 {
				errs <- fmt.Errorf("manifest version = %d, want 1", manifest.Version)
				return
			}
			if _, ok := g.GetEntity("person:alice"); !ok {
				errs <- fmt.Errorf("loaded graph missing person:alice")
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := counting.Count(); got != 1 {
		t.Fatalf("commit object reads = %d, want one shared cache load", got)
	}
}

func TestReaderCacheRefreshAndLoadShareTenantLoad(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	cache := NewReaderCache(store, time.Minute)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:bob", Kind: "person"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	started := make(chan struct{})
	counting := &countingDelayReadStore{
		ObjectStore: base,
		substring:   "/tenants/tenant-a/commits/",
		delay:       50 * time.Millisecond,
		started:     started,
	}
	store.Objects = counting
	cache.mu.Lock()
	entry := cache.entries["tenant-a"]
	entry.expiresAt = time.Now().Add(-time.Minute)
	entry.lastAccess = time.Now()
	cache.entries["tenant-a"] = entry
	cache.mu.Unlock()

	refreshDone := make(chan struct{})
	go func() {
		cache.RefreshCached(ctx)
		close(refreshDone)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start loading commit objects")
	}
	g, manifest, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load during refresh: %v", err)
	}
	if manifest.Version != 2 {
		t.Fatalf("load version = %d, want refreshed v2", manifest.Version)
	}
	if _, ok := g.GetEntity("person:bob"); !ok {
		t.Fatal("load during refresh did not observe refreshed graph")
	}
	<-refreshDone
	if got := counting.Count(); got != 2 {
		t.Fatalf("commit object reads = %d, want one shared refresh/load of two commits", got)
	}
}

func TestReaderCacheInvalidationDuringSlowLoadPreventsStalePublish(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	if _, err := store.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	cache := NewReaderCache(store, time.Nanosecond)
	if _, _, err := cache.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("warm cache: %v", err)
	}
	cache.Invalidate("tenant-a")
	store.deleteWriteCache("tenant-a")
	pausing := &pauseOnceReadStore{ObjectStore: base, substring: "/commits/", paused: make(chan struct{}), resume: make(chan struct{})}
	store.Objects = pausing

	done := make(chan struct {
		manifest Manifest
		err      error
	}, 1)
	go func() {
		_, manifest, err := cache.Load(ctx, "tenant-a")
		done <- struct {
			manifest Manifest
			err      error
		}{manifest: manifest, err: err}
	}()
	select {
	case <-pausing.paused:
	case <-time.After(time.Second):
		t.Fatal("cache load did not pause on commit read")
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "person:bob", Kind: "person"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit v2: %v", err)
	}
	cache.Invalidate("tenant-a")
	close(pausing.resume)

	result := <-done
	if result.err != nil {
		t.Fatalf("load after invalidate: %v", result.err)
	}
	if result.manifest.Version != 2 {
		t.Fatalf("load returned stale version %d, want 2", result.manifest.Version)
	}
	cache.mu.RLock()
	entry := cache.entries["tenant-a"]
	cache.mu.RUnlock()
	if entry.manifest.Version != 2 {
		t.Fatalf("cache published stale version %d, want 2", entry.manifest.Version)
	}
}

func TestReaderCacheRepeatedInvalidationsDuringLoadUseLatest(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	writer := NewTenantStore(base, "test")
	if _, err := writer.Commit(ctx, "tenant-a", sampleMutations(), CommitOptions{}); err != nil {
		t.Fatalf("commit v1: %v", err)
	}
	interceptor := &invalidateOnCommitReadStore{
		ObjectStore: base,
		substring:   "/commits/",
		max:         12,
	}
	reader := NewTenantStore(interceptor, "test")
	cache := NewReaderCache(reader, time.Minute)
	interceptor.callback = func(n int) error {
		_, err := writer.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: fmt.Sprintf("person:reload-%02d", n), Kind: "person"}},
		}, CommitOptions{})
		if err == nil {
			cache.Invalidate("tenant-a")
		}
		return err
	}

	g, manifest, err := cache.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cache load: %v", err)
	}
	if manifest.Version != int64(interceptor.max+1) {
		t.Fatalf("loaded version = %d, want %d", manifest.Version, interceptor.max+1)
	}
	if _, ok := g.GetEntity(fmt.Sprintf("person:reload-%02d", interceptor.max)); !ok {
		t.Fatalf("latest invalidation entity missing from loaded graph")
	}
	if interceptor.Count() != interceptor.max {
		t.Fatalf("invalidations = %d, want %d", interceptor.Count(), interceptor.max)
	}
}

func cacheHasTenant(cache *ReaderCache, tenantID string) bool {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	_, ok := cache.entries[tenantID]
	return ok
}

type slowReadStore struct {
	ObjectStore
	delay           time.Duration
	notifySubstring string
	started         chan struct{}
	once            sync.Once
}

func (s *slowReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.notify(key)
	time.Sleep(s.delay)
	return s.ObjectStore.Get(ctx, key)
}

func (s *slowReadStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.notify(key)
	time.Sleep(s.delay)
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *slowReadStore) notify(key string) {
	if s.started == nil || s.notifySubstring == "" || !strings.Contains(key, s.notifySubstring) {
		return
	}
	s.once.Do(func() {
		close(s.started)
	})
}

type countingDelayReadStore struct {
	ObjectStore
	substring string
	delay     time.Duration
	started   chan struct{}
	once      sync.Once

	mu    sync.Mutex
	count int
}

func (s *countingDelayReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, s.substring) {
		s.mu.Lock()
		s.count++
		s.mu.Unlock()
		if s.started != nil {
			s.once.Do(func() {
				close(s.started)
			})
		}
		time.Sleep(s.delay)
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *countingDelayReadStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

type pauseOnceReadStore struct {
	ObjectStore
	substring string
	paused    chan struct{}
	resume    chan struct{}
	once      sync.Once
}

func (s *pauseOnceReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, s.substring) {
		var wait bool
		s.once.Do(func() {
			wait = true
			close(s.paused)
		})
		if wait {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-s.resume:
			}
		}
	}
	return s.ObjectStore.Get(ctx, key)
}

type invalidateOnCommitReadStore struct {
	ObjectStore
	substring string
	max       int
	callback  func(int) error

	mu    sync.Mutex
	count int
}

func (s *invalidateOnCommitReadStore) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.Contains(key, s.substring) {
		n, ok := s.nextInvalidation()
		if ok && s.callback != nil {
			if err := s.callback(n); err != nil {
				return nil, err
			}
		}
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *invalidateOnCommitReadStore) nextInvalidation() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.count >= s.max {
		return 0, false
	}
	s.count++
	return s.count, true
}

func (s *invalidateOnCommitReadStore) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func sampleMutations() graph.Mutations {
	return graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name:        "works_at",
			FromKind:    "person",
			ToKind:      "company",
			Directed:    true,
			Cardinality: graph.ManyToOne,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "person:alice", Kind: "person", Fields: graph.Fields{"name": "Alice"}},
			{ID: "company:acme", Kind: "company", Fields: graph.Fields{"name": "ACME"}},
		},
		UpsertEdges: []graph.Edge{{
			ID:   "edge:alice-acme",
			Type: "works_at",
			From: "person:alice",
			To:   "company:acme",
		}},
	}
}
