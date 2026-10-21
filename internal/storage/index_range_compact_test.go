package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestRangeCompactionOnePairPerSliceAndResourceRelease(t *testing.T) {
	ctx := context.Background()
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	s := NewTenantStore(files, "test")
	s.WriteIndexRangePages = true
	s.WriteEntityRecords = false
	defer s.ShutdownTasks(ctx)
	var entities []graph.Entity
	for i := 0; len(entities) < 800; i++ {
		id := fmt.Sprintf("host:compact-%07d", i)
		if entityShardID(id) == "00" {
			entities = append(entities, graph.Entity{ID: id, Kind: "host"})
		}
	}
	if _, err := s.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := s.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	var pages []EntityPageData
	for i := 0; i < 4; i++ {
		lower := ""
		if i > 0 {
			lower = entities[i*200].ID
		}
		pages = append(pages, EntityPageData{Shard: "00", Version: catalog.Version, Entities: entities[i*200 : (i+1)*200], pageRole: rangeRole(lower)})
	}
	pages, err = prepareBoundedEntityPages(ctx, "tenant-a", pages)
	if err != nil {
		t.Fatal(err)
	}
	catalog.EntityPages = entityPageSpecs(pages)
	s.decorateIndexCatalog(&catalog, "tenant-a")
	if err := s.writeParquetEntityPages(ctx, "tenant-a", pages, catalog.Version); err != nil {
		t.Fatal(err)
	}
	_, meta, err := s.getIndexCatalogWithMeta(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.putIndexCatalogWithMeta(ctx, "tenant-a", catalog, meta); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < cap(s.maintenance.encodes); i++ {
		s.maintenance.encodes <- struct{}{}
	}
	pair := catalog.EntityPages[0].Objects[:2]
	// Warm the unrelated source reads before putting the deadline on encoder
	// admission; under race, a cold Parquet read can consume the whole window.
	for _, object := range pair {
		if _, _, ok, err := s.borrowValidatedParquetEntityPageObject(ctx, "tenant-a", catalog.Version, entityRangeSpec(catalog.EntityPages[0], object)); err != nil || !ok {
			t.Fatalf("warm merge source: %v %v", ok, err)
		}
	}
	eligible := map[string]bool{pair[0].ContentHash: true}
	unchanged, _, err := s.mergeEntityRanges(ctx, "tenant-a", catalog.Version, catalog.EntityPages[0], pair, nil, eligible)
	if err != nil || len(unchanged) != 2 {
		t.Fatalf("busy standalone merge blocked or mutated: %v", err)
	}
	s.ReplicationMode = true
	deadline, cancelDeadline := context.WithTimeout(ctx, 20*time.Millisecond)
	_, _, err = s.mergeEntityRanges(deadline, "tenant-a", catalog.Version, catalog.EntityPages[0], pair, nil, eligible)
	cancelDeadline()
	if err != context.DeadlineExceeded {
		t.Fatalf("replicated merge ignored encoder deadline: %v", err)
	}
	s.ReplicationMode = false
	for i := 0; i < cap(s.maintenance.encodes); i++ {
		<-s.maintenance.encodes
	}
	skipped := map[string]bool{}
	changed, done, bytes, err := s.compactRangeSlice(ctx, "tenant-a", skipped)
	if err != nil || !changed || done || bytes == 0 {
		t.Fatalf("first slice: %v %v %d %v", changed, done, bytes, err)
	}
	next, err := s.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(next.EntityPages[0].Objects) != 3 {
		t.Fatal("one slice merged more than one pair")
	}
	if s.maintenance.bytes != 0 || len(s.maintenance.builds) != 0 || len(s.maintenance.encodes) != 0 {
		t.Fatal("slice retained maintenance resources")
	}
	for i := 0; i < 4; i++ {
		_, done, _, err = s.compactRangeSlice(ctx, "tenant-a", skipped)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("small-page sweep did not converge")
	}
	result, err := s.ListEntities(ctx, "tenant-a", EntityScanOptions{Limit: 1000})
	if err != nil || len(result.Entities) != 800 {
		t.Fatalf("compaction lost rows: %d %v", len(result.Entities), err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, _, err := s.compactRangeSlice(canceled, "tenant-a", skipped); err != context.Canceled {
		t.Fatalf("canceled slice: %v", err)
	}
	if s.maintenance.bytes != 0 || len(s.maintenance.builds) != 0 {
		t.Fatal("canceled slice retained resources")
	}
}
