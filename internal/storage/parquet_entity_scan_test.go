package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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

func TestParquetEntityCandidateScanFiltersPackedPageByLogicalShard(t *testing.T) {
	ctx := context.Background()
	firstID, secondID := entityIDsInDifferentShards(t)
	firstShard := entityShardID(firstID)
	page := EntityPageData{
		TenantID:  "tenant-a",
		Shard:     "pack_test",
		Version:   1,
		UpdatedAt: time.Now().UTC(),
		Entities: []graph.Entity{
			{ID: firstID, Kind: "system", Source: "agent"},
			{ID: secondID, Kind: "system", Source: "agent"},
		},
	}
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		t.Fatalf("marshal packed page: %v", err)
	}
	scan, err := scanParquetEntityPageCandidates(ctx, data, firstShard, EntityScanOptions{Kind: "system"}, scanCursor{})
	if err != nil {
		t.Fatalf("candidate scan: %v", err)
	}
	if _, ok := scan.IDs[firstID]; !ok {
		t.Fatalf("candidate IDs = %#v, want %q", scan.IDs, firstID)
	}
	if _, ok := scan.IDs[secondID]; ok {
		t.Fatalf("candidate IDs = %#v, packed entity from another shard %q must be excluded", scan.IDs, secondID)
	}
}

func TestDecodeParquetEntityPageReadsPackedShardAcrossRowGroups(t *testing.T) {
	ctx := context.Background()
	targetIDs, targetShard := parquetEntityIDsInShard(t, "system:rowgroup-target", 128, "")
	otherIDs, otherShard := parquetEntityIDsInShard(t, "system:rowgroup-other", 64, targetShard)
	if targetShard == otherShard {
		t.Fatalf("test shards collided: %q", targetShard)
	}
	entities := make([]graph.Entity, 0, len(targetIDs)+len(otherIDs))
	for _, id := range targetIDs[:64] {
		entities = append(entities, parquetShardTestEntity(id))
	}
	for _, id := range otherIDs {
		entities = append(entities, parquetShardTestEntity(id))
	}
	for _, id := range targetIDs[64:] {
		entities = append(entities, parquetShardTestEntity(id))
	}
	data, err := marshalParquetEntityPage(ctx, EntityPageData{
		TenantID: "tenant-a",
		Shard:    "pack_test",
		Version:  1,
		Entities: entities,
	})
	if err != nil {
		t.Fatalf("marshal packed page: %v", err)
	}
	reader, err := pqfile.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("open packed page: %v", err)
	}
	rowGroups := parquetEntityShardRowGroups(reader, targetShard)
	reader.Close()
	if len(rowGroups) != 2 || rowGroups[0] != 0 || rowGroups[1] != 2 {
		t.Fatalf("target row groups = %#v, want [0 2] across packed shards", rowGroups)
	}

	decoded, err := decodeParquetEntityPage(ctx, data, "tenant-a", targetShard, 1)
	if err != nil {
		t.Fatalf("decode packed target shard: %v", err)
	}
	if len(decoded.Entities) != len(targetIDs) {
		t.Fatalf("decoded target entities = %d, want %d", len(decoded.Entities), len(targetIDs))
	}
	for _, entity := range decoded.Entities {
		if entityShardID(entity.ID) != targetShard {
			t.Fatalf("decoded entity %q from shard %q, want %q", entity.ID, entityShardID(entity.ID), targetShard)
		}
		if got := entity.Fields["field-3"]; got != entity.ID+":value-3" {
			t.Fatalf("decoded entity %q field-3 = %#v, want complete field value", entity.ID, got)
		}
	}
}

func TestDecodeParquetEntityPageKeepsLegacyEmptyShardRows(t *testing.T) {
	ctx := context.Background()
	ids, shard := parquetEntityIDsInShard(t, "system:legacy-empty-shard", 2, "")
	entities := []graph.Entity{parquetShardTestEntity(ids[0]), parquetShardTestEntity(ids[1])}
	data, err := marshalParquetEntityPage(ctx, EntityPageData{
		TenantID: "tenant-a",
		Shard:    "",
		Version:  1,
		Entities: entities,
	})
	if err != nil {
		t.Fatalf("marshal legacy page: %v", err)
	}
	decoded, err := decodeParquetEntityPage(ctx, data, "tenant-a", shard, 1)
	if err != nil {
		t.Fatalf("decode legacy page: %v", err)
	}
	if len(decoded.Entities) != len(entities) {
		t.Fatalf("decoded legacy entities = %d, want %d", len(decoded.Entities), len(entities))
	}
	for _, entity := range decoded.Entities {
		if got := entity.Fields["field-3"]; got != entity.ID+":value-3" {
			t.Fatalf("decoded legacy entity %q field-3 = %#v, want complete field value", entity.ID, got)
		}
	}
}

func parquetEntityIDsInShard(t *testing.T, prefix string, count int, avoid string) ([]string, string) {
	t.Helper()
	ids := make([]string, 0, count)
	shard := ""
	for i := 0; i < 1000000 && len(ids) < count; i++ {
		id := fmt.Sprintf("%s-%06d", prefix, i)
		candidate := entityShardID(id)
		if shard == "" {
			if candidate == avoid {
				continue
			}
			shard = candidate
		}
		if candidate == shard {
			ids = append(ids, id)
		}
	}
	if len(ids) != count {
		t.Fatalf("found %d/%d IDs for shard %q with prefix %q", len(ids), count, shard, prefix)
	}
	return ids, shard
}

func parquetShardTestEntity(id string) graph.Entity {
	fields := make(graph.Fields, 8)
	for i := 0; i < 8; i++ {
		fields[fmt.Sprintf("field-%d", i)] = fmt.Sprintf("%s:value-%d", id, i)
	}
	return graph.Entity{ID: id, Kind: "system", Fields: fields}
}

func entityIDsInDifferentShards(t *testing.T) (string, string) {
	t.Helper()
	firstID := "system:packed-0"
	firstShard := entityShardID(firstID)
	for i := 1; i < 1000; i++ {
		candidate := fmt.Sprintf("system:packed-%d", i)
		if entityShardID(candidate) != firstShard {
			return firstID, candidate
		}
	}
	t.Fatal("failed to find entity IDs in different shards")
	return "", ""
}
