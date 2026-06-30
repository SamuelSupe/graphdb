package storage

import (
	"context"
	"testing"

	"graphdb/internal/graph"
)

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
	if !second.Skipped || second.Version != 1 || second.Failed != 0 {
		t.Fatalf("second ingest = %#v, want skipped current version", second)
	}
}
