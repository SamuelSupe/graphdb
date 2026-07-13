package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func BenchmarkMarshalParquetIndexCatalog10K(b *testing.B) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "catalog-bench")
	entities := make([]graph.Entity, 10_000)
	for i := range entities {
		entities[i] = graph.Entity{
			ID:     fmt.Sprintf("host:%05d", i),
			Kind:   "host",
			Fields: graph.Fields{"state": "ready"},
		}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name:   "host",
			Fields: map[string]graph.FieldSpec{"state": {Type: "string", Indexed: true}},
		}},
		UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalParquetIndexCatalog(ctx, catalog); err != nil {
			b.Fatal(err)
		}
	}
}

func TestMarshalParquetIndexCatalogBatchesRows(t *testing.T) {
	catalog := IndexCatalog{
		TenantID:  "tenant-a",
		Version:   1,
		UpdatedAt: time.Unix(1, 0).UTC(),
	}
	for i := 0; i < 64; i++ {
		catalog.EntityPages = append(catalog.EntityPages, EntityPageSpec{
			Shard:       fmt.Sprintf("%02x", i),
			EntityCount: 1,
			Objects: []IndexObject{{
				Role: "page",
				Key:  fmt.Sprintf("page-%02x.parquet", i),
			}},
		})
	}
	data, err := marshalParquetIndexCatalog(context.Background(), catalog)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if reader.NumRowGroups() != 1 {
		t.Fatalf("row groups = %d, want 1", reader.NumRowGroups())
	}
}
