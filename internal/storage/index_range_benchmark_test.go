package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// This isolates data-page and flat-directory I/O. Graph mutation, secondary
// indexes and writer fencing are deliberately outside this benchmark.
func BenchmarkIncrementalRangeDataAndDirectoryIO(b *testing.B) {
	for _, size := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			ctx := context.Background()
			files, err := OpenFileStore(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer files.Close()
			store := NewTenantStore(files, "bench")
			store.WriteEntityRecords = false
			store.WriteIndexRangePages = true
			defer store.ShutdownTasks(ctx)
			// Hold the touched bucket at exactly 1024 records for every tenant size.
			entities := make([]graph.Entity, 0, size)
			for i := 0; len(entities) < 1024; i++ {
				id := fmt.Sprintf("host:target-%08d", i)
				if entityShardID(id) == "00" {
					entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"value": "original", "payload": "same-width-payload"}})
				}
			}
			id := entities[0].ID
			for i := 0; len(entities) < size; i++ {
				id := fmt.Sprintf("host:other-%08d", i)
				if entityShardID(id) != "00" {
					entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"value": "original", "payload": "same-width-payload"}})
				}
			}
			before, err := graph.FromSnapshot(graph.Snapshot{Version: 1, Entities: entities})
			if err != nil {
				b.Fatal(err)
			}
			artifacts, err := buildTenantIndexArtifacts(ctx, "tenant-a", before, 1, nil, nil, time.Unix(100, 0))
			if err != nil {
				b.Fatal(err)
			}
			store.decorateIndexCatalog(&artifacts.Catalog, "tenant-a")
			if err := store.writeParquetEntityPages(ctx, "tenant-a", artifacts.EntityPages, 1); err != nil {
				b.Fatal(err)
			}
			after, _, err := before.ApplyCommitStorageCopyWithOptions(graph.Commit{ID: "change", Version: 2, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: id, Kind: "host", Fields: graph.Fields{"value": "modified", "payload": "same-width-payload"}}}}}, graph.ApplyOptions{})
			if err != nil {
				b.Fatal(err)
			}
			nextVersion := int64(2)
			for _, warm := range []bool{false, true} {
				b.Run(fmt.Sprintf("warm=%v", warm), func(b *testing.B) {
					baseVersion := nextVersion
					nextVersion += int64(b.N) + 1
					after.Version = baseVersion
					reads := &countingMetaReadStore{ObjectStore: files}
					writer := NewTenantStore(reads, "bench")
					writer.WriteIndexRangePages = true
					writer.WriteEntityRecords = false
					defer writer.ShutdownTasks(ctx)
					writer.ConfigureIndexObjectCache(IndexObjectCacheConfig{})
					if warm {
						writer.ConfigureIndexObjectCache(IndexObjectCacheConfig{MaxEntries: 4096, MaxBytes: 64 << 20, RevalidateTTL: time.Hour})
						if _, _, err := writer.buildIncrementalEntityPages(ctx, "tenant-a", 1, artifacts.Catalog.EntityPages, before, after, []string{id}, baseVersion, time.Unix(101, 0)); err != nil {
							b.Fatal(err)
						}
					}
					reads.Reset()
					b.ReportAllocs()
					b.ResetTimer()
					var dataBytes, dirBytes int64
					var rows int
					for n := 0; n < b.N; n++ {
						version := baseVersion + int64(n)
						after.Version = version
						pages, specs, err := writer.buildIncrementalEntityPages(ctx, "tenant-a", 1, artifacts.Catalog.EntityPages, before, after, []string{id}, version, time.Unix(101, 0))
						if err != nil {
							b.Fatal(err)
						}
						catalog := IndexCatalog{Version: version, TenantID: "tenant-a", EntityPages: specs}
						if err := writer.writeChangedParquetEntityPagesFast(ctx, "tenant-a", pages, catalog, before, after, []string{id}, version); err != nil {
							b.Fatal(err)
						}
						encoded, err := marshalParquetIndexCatalog(ctx, catalog)
						if err != nil {
							b.Fatal(err)
						}
						if err := files.Put(ctx, "bench/directory.parquet", encoded); err != nil {
							b.Fatal(err)
						}
						dirBytes = int64(len(encoded))
						dataBytes = 0
						rows = 0
						for _, page := range pages {
							r, _ := decodeIndexRange(page.pageRole)
							dataBytes += r.Bytes
							rows += len(page.Entities)
						}
					}
					b.StopTimer()
					b.ReportMetric(float64(dataBytes), "data-bytes/op")
					b.ReportMetric(float64(dirBytes), "directory-bytes/op")
					b.ReportMetric(float64(rows), "data-rows/op")
					b.ReportMetric(float64(reads.GetWithMetaCount(artifacts.Catalog.EntityPages[0].Objects[0].Key))/float64(b.N), "source-page-reads/op")
					b.ReportMetric(2, "object-writes/op")
				})
			}
		})
	}
}
