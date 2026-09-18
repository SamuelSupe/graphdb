package storage

import (
	"context"
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func BenchmarkTenantBackup10K(b *testing.B) {
	ctx := context.Background()
	files, err := OpenFileStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { files.Close() })
	store := NewTenantStore(files, "bench")
	entities := make([]graph.Entity, 10_000)
	for i := range entities {
		id := fmt.Sprintf("host:%05d", i)
		entities[i] = graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"hostname": id, "region": "east", "sequence": i}}
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}}, UpsertEntities: entities,
	}, CommitOptions{}); err != nil {
		b.Fatal(err)
	}
	b.Run("capture", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, _, _, err := store.captureTenantBackup(ctx, "tenant-a"); err != nil {
				b.Fatal(err)
			}
		}
	})
	_, record, _, err := store.captureTenantBackup(ctx, "tenant-a")
	if err != nil {
		b.Fatal(err)
	}
	result := taskResult(record)
	data, err := marshalParquetTaskResult(ctx, "tenant-a", "backup", result)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for i := 0; i < b.N; i++ {
			if _, err := decodeParquetTaskResult(ctx, data, "tenant-a", "backup"); err != nil {
				b.Fatal(err)
			}
		}
	})
}
