package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestTenantStoreSegmentsCommitTailAndLoadsAfterLooseCleanup(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host", Fields: graph.Fields{"seq": i},
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(manifest.CommitSegments) != 1 || len(manifest.CommitKeys) != 0 {
		t.Fatalf("manifest segments=%#v keys=%#v", manifest.CommitSegments, manifest.CommitKeys)
	}
	if got := ManifestCommitTailLength(manifest); got != commitSegmentTargetCount {
		t.Fatalf("tail length=%d want %d", got, commitSegmentTargetCount)
	}
	if manifest.CommitSegments[0].Codec != commitSegmentCodecParquet ||
		!strings.HasSuffix(manifest.CommitSegments[0].Key, ".parquet") ||
		manifest.CommitSegments[0].Count != commitSegmentTargetCount ||
		manifest.CommitSegments[0].ContentHash == "" {
		t.Fatalf("segment ref=%#v", manifest.CommitSegments[0])
	}
	gc, err := store.RunGC(ctx, "tenant-a", GCOptions{})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	if gc.CommitCleanup.Deleted == 0 {
		t.Fatalf("gc did not remove loose commit objects: %#v", gc.CommitCleanup)
	}
	store.deleteWriteCache("tenant-a")
	loaded, loadedManifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loadedManifest.Version != int64(commitSegmentTargetCount) {
		t.Fatalf("loaded version=%d", loadedManifest.Version)
	}
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, ok := loaded.GetEntity(fmt.Sprintf("host:%03d", i)); !ok {
			t.Fatalf("missing host:%03d", i)
		}
	}
}

func TestNonParquetCommitSegmentIsRejected(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < 2; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	manifest, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	nonParquetKey := store.commitSegmentPrefix("tenant-a") + "non-parquet-segment.parquet"
	data := []byte(`{"kind":"commit-segment","codec":"commit-segment-ndjson-v1"}`)
	if err := store.Objects.Put(ctx, nonParquetKey, data); err != nil {
		t.Fatalf("put non-parquet segment: %v", err)
	}
	_, err = store.loadCommitSegment(ctx, "tenant-a", CommitSegmentRef{
		Key:          nonParquetKey,
		Codec:        "commit-segment-ndjson-v1",
		FirstVersion: manifest.Version - 1,
		LastVersion:  manifest.Version,
		Count:        len(manifest.CommitKeys),
	})
	if err == nil || !strings.Contains(err.Error(), "only parquet segments") {
		t.Fatalf("load non-parquet segment err=%v", err)
	}
}

func TestIndexInspectIncludesCommitSegmentsWithoutCatalog(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host", Fields: graph.Fields{"seq": i},
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	inspection, err := store.InspectIndex(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if inspection.TenantID != "tenant-a" || inspection.Version != int64(commitSegmentTargetCount) {
		t.Fatalf("inspection header = %#v", inspection)
	}
	var segment IndexInspectionObject
	for _, object := range inspection.Objects {
		if object.Role == "commit_segment" {
			segment = object
			break
		}
	}
	if segment.Key == "" {
		t.Fatalf("inspection has no commit segment: %#v", inspection.Objects)
	}
	if segment.ObjectKind != "commit-segment" ||
		segment.Format != IndexFormatParquet ||
		segment.Codec != commitSegmentCodecParquet ||
		segment.RowCount != commitSegmentTargetCount ||
		segment.FirstVersion != 1 ||
		segment.LastVersion != int64(commitSegmentTargetCount) ||
		segment.ExpectedHash == "" ||
		segment.ContentHash != segment.ExpectedHash ||
		!segment.HashMatches ||
		segment.PayloadBytes == 0 {
		t.Fatalf("segment inspection = %#v", segment)
	}
}

func TestCompactClearsCommitSegmentsAndGCDeletesSegment(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i < commitSegmentTargetCount; i++ {
		if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: fmt.Sprintf("host:%03d", i), Kind: "host",
		}}}, CommitOptions{}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}
	before, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest: %v", err)
	}
	if len(before.CommitSegments) != 1 {
		t.Fatalf("segments before compact=%#v", before.CommitSegments)
	}
	if _, err := store.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact: %v", err)
	}
	after, err := store.CurrentManifest(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("manifest after compact: %v", err)
	}
	if len(after.CommitSegments) != 0 || len(after.CommitKeys) != 0 {
		t.Fatalf("tail after compact segments=%#v keys=%#v", after.CommitSegments, after.CommitKeys)
	}
	gc, err := store.RunGC(ctx, "tenant-a", GCOptions{})
	if err != nil {
		t.Fatalf("gc: %v", err)
	}
	foundSegmentDelete := false
	for _, key := range gc.DeletedKeys {
		if key == before.CommitSegments[0].Key {
			foundSegmentDelete = true
			break
		}
	}
	if !foundSegmentDelete {
		t.Fatalf("segment %q was not deleted by gc: %#v", before.CommitSegments[0].Key, gc.DeletedKeys)
	}
}
