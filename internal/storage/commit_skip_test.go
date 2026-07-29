package storage

import (
	"context"
	"sync"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestInitTenantConcurrentFirstLoadAndCommit(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, _, err := store.Load(ctx, "tenant-a")
		errs <- err
	}()
	go func() {
		defer wg.Done()
		<-start
		_, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{}, CommitOptions{})
		errs <- err
	}()
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent first operation: %v", err)
		}
	}
}

func TestCommitSkipsWhenContentMD5Unchanged(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{
		Kind: "host", Source: "agent", ExternalID: "i-1", Fields: graph.Fields{"hostname": "app-01"},
	}}}
	first, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("first commit: %v", err)
	}
	if first.Skipped || first.Version != 1 || first.DataMD5 == "" {
		t.Fatalf("first result = %#v, want committed version 1 with data md5", first)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load committed graph: %v", err)
	}
	legacyMD5, err := g.ContentMD5()
	if err != nil {
		t.Fatalf("legacy content md5: %v", err)
	}
	if first.DataMD5 != legacyMD5 {
		t.Fatalf("data_md5 = %q, want legacy logical md5 %q", first.DataMD5, legacyMD5)
	}

	second, err := store.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if !second.Skipped {
		t.Fatalf("second result = %#v, want skipped", second)
	}
	if second.Version != 1 || second.ReadableVersion != 1 || second.ReadAfterCommitID != "" {
		t.Fatalf("skipped result = %#v, want current version 1 without commit id", second)
	}
	if second.DataMD5 != first.DataMD5 {
		t.Fatalf("skipped md5 = %q, want %q", second.DataMD5, first.DataMD5)
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if manifest.Version != 1 || len(manifest.CommitKeys) != 1 {
		t.Fatalf("manifest after skip = %#v, want original single commit", manifest)
	}
	if manifest.DataMD5 != first.DataMD5 {
		t.Fatalf("manifest data md5 = %q, want %q", manifest.DataMD5, first.DataMD5)
	}

	changed, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		Kind: "host", Source: "agent", ExternalID: "i-1", Fields: graph.Fields{"hostname": "app-02"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("changed commit: %v", err)
	}
	if changed.Skipped || changed.Version != 2 || changed.DataMD5 == first.DataMD5 {
		t.Fatalf("changed result = %#v, want committed version 2 with new md5", changed)
	}
}

func TestCommitPreservesDataMD5WhenLoadingLegacyManifest(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "test")
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}}
	first, err := writer.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	manifest, meta, err := writer.getManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get manifest: %v", err)
	}
	manifest.DataMD5 = ""
	if _, err := writer.putManifestMeta(ctx, "tenant-a", manifest, meta); err != nil {
		t.Fatalf("write legacy manifest: %v", err)
	}

	cold := NewTenantStore(objects, "test")
	cold.InstanceID = writer.InstanceID
	replay, err := cold.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("cold no-op commit: %v", err)
	}
	if !replay.Skipped || replay.DataMD5 != first.DataMD5 {
		t.Fatalf("legacy manifest replay = %#v, want skipped md5 %q", replay, first.DataMD5)
	}
}

func TestCommitSkipsWhenAppendUniqueAddsNoElements(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{
			"tags": {Type: "array", MergeStrategy: graph.FieldMergeAppendUnique},
		}}},
		UpsertEntities: []graph.Entity{{
			ID: "host:a", Kind: "host", Fields: graph.Fields{"tags": []any{"abc"}},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	second, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"tags": []any{"abc"}},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if !second.Skipped || second.Version != 1 {
		t.Fatalf("second result = %#v, want skipped at version 1", second)
	}
}

func TestCommitFingerprintStableAfterColdSnapshotLoad(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "test")
	mutations := graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
	}}}
	first, err := writer.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := writer.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}

	cold := NewTenantStore(objects, "test")
	cold.InstanceID = writer.InstanceID
	second, err := cold.CommitWithReport(ctx, "tenant-a", mutations, CommitOptions{})
	if err != nil {
		t.Fatalf("cold no-op commit: %v", err)
	}
	if !second.Skipped || second.DataMD5 != first.DataMD5 {
		t.Fatalf("cold result = %#v, want skipped fingerprint %q", second, first.DataMD5)
	}
}

func TestIngestMarksSkippedWhenCommitContentUnchanged(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	request := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "batch-1",
		Items: []IngestItem{{
			ExternalID: "i-1",
			Entity:     &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "app-01"}},
		}},
	}
	first, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if first.Skipped || first.Version != 1 {
		t.Fatalf("first ingest = %#v, want version 1", first)
	}
	request.BatchID = "batch-2"
	second, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("second ingest: %v", err)
	}
	if !second.Skipped || second.SkipReason != IngestSkipReasonLogicalNoop || second.Version != 1 || second.Failed != 0 {
		t.Fatalf("second ingest = %#v, want skipped current version", second)
	}
	replayed, err := store.Ingest(ctx, "tenant-a", request)
	if err != nil {
		t.Fatalf("replay ingest: %v", err)
	}
	if !replayed.Skipped || replayed.SkipReason != IngestSkipReasonIdempotentReplay || replayed.Version != second.Version {
		t.Fatalf("replayed ingest = %#v, want idempotent replay", replayed)
	}
}
