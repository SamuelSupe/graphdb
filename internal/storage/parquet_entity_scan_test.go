package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"graphdb/internal/graph"
)

func TestParquetEntityCandidateScanPrunesAbsentKind(t *testing.T) {
	ctx := context.Background()
	page := EntityPageData{
		TenantID:  "tenant-a",
		Shard:     "aa",
		Version:   1,
		UpdatedAt: time.Now().UTC(),
	}
	for i := 0; i < int(parquetEntityPageRowGroupSize)+10; i++ {
		page.Entities = append(page.Entities, graph.Entity{
			ID:     fmt.Sprintf("host:%03d", i),
			Kind:   "host",
			Source: "agent",
		})
	}
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	scan, err := scanParquetEntityPageCandidates(ctx, data, page.Shard, EntityScanOptions{Kind: "service"}, scanCursor{})
	if err != nil {
		t.Fatalf("candidate scan: %v", err)
	}
	if len(scan.IDs) != 0 {
		t.Fatalf("candidate IDs = %#v, want none", scan.IDs)
	}
	if scan.RowGroupsSkipped == 0 {
		t.Fatalf("row groups skipped = %d, want stats pruning", scan.RowGroupsSkipped)
	}
}

func TestParquetEntityCandidateScanFiltersSourceAndCursor(t *testing.T) {
	ctx := context.Background()
	page := EntityPageData{
		TenantID:  "tenant-a",
		Shard:     "aa",
		Version:   1,
		UpdatedAt: time.Now().UTC(),
		Entities: []graph.Entity{
			{ID: "host:a", Kind: "host", Source: "manual"},
			{
				ID:     "host:b",
				Kind:   "host",
				Source: "agent",
				Sources: []graph.EntitySource{{
					Source:     "manual",
					ExternalID: "manual-host-b",
					ObservedAt: time.Now().UTC(),
				}},
			},
			{ID: "svc:a", Kind: "service", Source: "manual"},
		},
	}
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		t.Fatalf("marshal page: %v", err)
	}
	scan, err := scanParquetEntityPageCandidates(ctx, data, page.Shard, EntityScanOptions{
		Kind:   "host",
		Source: "manual",
	}, scanCursor{After: scanKey(page.Shard, "host:a")})
	if err != nil {
		t.Fatalf("candidate scan: %v", err)
	}
	if _, ok := scan.IDs["host:a"]; ok {
		t.Fatalf("host:a should be excluded by cursor: %#v", scan.IDs)
	}
	if _, ok := scan.IDs["svc:a"]; ok {
		t.Fatalf("svc:a should be excluded by kind: %#v", scan.IDs)
	}
	if _, ok := scan.IDs["host:b"]; !ok || len(scan.IDs) != 1 {
		t.Fatalf("candidate IDs = %#v, want only host:b", scan.IDs)
	}
}
