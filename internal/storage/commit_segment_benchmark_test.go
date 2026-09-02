package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const benchmarkCommitTenantID = "tenant-a"

type benchmarkCommitFixture struct {
	baseStore        *MemoryStore
	objects          []benchmarkObject
	manifest         Manifest
	writerInstanceID string
	entityCount      int
	storedBytes      int64
	readBytes        int64
	segmentBytes     int64
	tailBytes        int64
}

type benchmarkObject struct {
	key  string
	data []byte
}

func BenchmarkTenantStoreLoadManifestGraphCommitSegments(b *testing.B) {
	for _, segmentCount := range []int{1, 4, 16} {
		segmentCount := segmentCount
		b.Run(fmt.Sprintf("segments-%d", segmentCount), func(b *testing.B) {
			ctx := context.Background()
			fixture := buildBenchmarkCommitFixture(b, segmentCount, 0)
			reportBenchmarkCommitFixture(b, fixture)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reader := NewTenantStore(fixture.baseStore, "bench")
				loaded, manifest, err := reader.Load(ctx, benchmarkCommitTenantID)
				if err != nil {
					b.Fatal(err)
				}
				if manifest.Version != fixture.manifest.Version ||
					len(manifest.CommitSegments) != len(fixture.manifest.CommitSegments) ||
					len(manifest.CommitKeys) != 0 ||
					len(loaded.Entities) != fixture.entityCount {
					b.Fatalf(
						"loaded version/segments/tail/entities = %d/%d/%d/%d, want %d/%d/0/%d",
						manifest.Version,
						len(manifest.CommitSegments),
						len(manifest.CommitKeys),
						len(loaded.Entities),
						fixture.manifest.Version,
						len(fixture.manifest.CommitSegments),
						fixture.entityCount,
					)
				}
			}
		})
	}
}

func BenchmarkTenantStoreLoadCommitTail(b *testing.B) {
	for _, tailCount := range []int{1, 31, 63} {
		tailCount := tailCount
		b.Run(fmt.Sprintf("tail-%d", tailCount), func(b *testing.B) {
			benchmarkTenantStoreLoadCommitTail(b, tailCount)
		})
	}
}

func BenchmarkTenantStoreLoadCommitTailWithParquetDecodeAdmission2(b *testing.B) {
	ConfigureParquetDecodeMaxConcurrent(2)
	b.Cleanup(func() { ConfigureParquetDecodeMaxConcurrent(0) })

	for _, tailCount := range []int{31, 63} {
		tailCount := tailCount
		b.Run(fmt.Sprintf("tail-%d", tailCount), func(b *testing.B) {
			benchmarkTenantStoreLoadCommitTail(b, tailCount)
		})
	}
}

func benchmarkTenantStoreLoadCommitTail(b *testing.B, tailCount int) {
	b.Helper()
	ctx := context.Background()
	fixture := buildBenchmarkCommitFixture(b, 0, tailCount)
	reportBenchmarkCommitFixture(b, fixture)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		reader := NewTenantStore(fixture.baseStore, "bench")
		loaded, manifest, err := reader.Load(ctx, benchmarkCommitTenantID)
		if err != nil {
			b.Fatal(err)
		}
		if manifest.Version != fixture.manifest.Version ||
			len(manifest.CommitSegments) != 0 ||
			len(manifest.CommitKeys) != len(fixture.manifest.CommitKeys) ||
			len(loaded.Entities) != fixture.entityCount {
			b.Fatalf(
				"loaded version/segments/tail/entities = %d/%d/%d/%d, want %d/0/%d/%d",
				manifest.Version,
				len(manifest.CommitSegments),
				len(manifest.CommitKeys),
				len(loaded.Entities),
				fixture.manifest.Version,
				len(fixture.manifest.CommitKeys),
				fixture.entityCount,
			)
		}
	}
}

func BenchmarkTenantStoreCompactCommitTail(b *testing.B) {
	ctx := context.Background()
	fixture := buildBenchmarkCommitFixture(b, 4, commitSegmentTargetCount-1)
	reportBenchmarkCommitFixture(b, fixture)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Compact mutates the manifest and writes a versioned snapshot. Rebuild
		// the same persisted input while the timer is stopped so each timed
		// operation exercises the public production compaction boundary.
		b.StopTimer()
		objects := cloneBenchmarkObjectStore(b, fixture)
		store := NewTenantStore(objects, "bench")
		// The copied lease belongs to the fixture writer. Reusing its identity
		// lets the fresh store renew that lease without adding coordination or
		// a test-only object-store behavior to the timed path.
		store.InstanceID = fixture.writerInstanceID
		store.ReaderID = fixture.writerInstanceID
		b.StartTimer()

		compacted, err := store.Compact(ctx, benchmarkCommitTenantID)
		if err != nil {
			b.Fatal(err)
		}
		if compacted.Version != fixture.manifest.Version ||
			compacted.SnapshotVersion != fixture.manifest.Version ||
			compacted.SnapshotCatalogKey == "" ||
			len(compacted.CommitSegments) != 0 ||
			len(compacted.CommitKeys) != 0 {
			b.Fatalf("unexpected compacted manifest: %#v", compacted)
		}
	}
}

func buildBenchmarkCommitFixture(
	b *testing.B,
	segmentCount int,
	tailCount int,
) benchmarkCommitFixture {
	b.Helper()
	if segmentCount < 0 || tailCount < 0 {
		b.Fatalf("invalid fixture sizes segments=%d tail=%d", segmentCount, tailCount)
	}
	totalCommits := segmentCount*commitSegmentTargetCount + tailCount
	if totalCommits == 0 {
		b.Fatal("benchmark fixture must contain at least one commit")
	}

	ctx := context.Background()
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "bench")
	for i := 0; i < totalCommits; i++ {
		id := fmt.Sprintf("host:%05d", i)
		if _, err := writer.Commit(ctx, benchmarkCommitTenantID, graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID:     id,
				Kind:   "host",
				Fields: graph.Fields{"sequence": i},
			}},
		}, CommitOptions{}); err != nil {
			b.Fatalf("fixture commit %d: %v", i, err)
		}
	}

	manifest, err := writer.CurrentManifest(ctx, benchmarkCommitTenantID)
	if err != nil {
		b.Fatalf("fixture manifest: %v", err)
	}
	if manifest.Version != int64(totalCommits) ||
		len(manifest.CommitSegments) != segmentCount ||
		len(manifest.CommitKeys) != tailCount {
		b.Fatalf(
			"fixture manifest version/segments/tail = %d/%d/%d, want %d/%d/%d",
			manifest.Version,
			len(manifest.CommitSegments),
			len(manifest.CommitKeys),
			totalCommits,
			segmentCount,
			tailCount,
		)
	}

	fixtureObjects, storedBytes, readBytes, segmentBytes, tailBytes :=
		snapshotBenchmarkObjects(b, ctx, objects)
	return benchmarkCommitFixture{
		baseStore:        objects,
		objects:          fixtureObjects,
		manifest:         manifest,
		writerInstanceID: writer.InstanceID,
		entityCount:      totalCommits,
		storedBytes:      storedBytes,
		readBytes:        readBytes,
		segmentBytes:     segmentBytes,
		tailBytes:        tailBytes,
	}
}

func snapshotBenchmarkObjects(
	b *testing.B,
	ctx context.Context,
	objects ObjectStore,
) ([]benchmarkObject, int64, int64, int64, int64) {
	b.Helper()
	entries, err := objects.List(ctx, "")
	if err != nil {
		b.Fatalf("list fixture objects: %v", err)
	}
	fixtureObjects := make([]benchmarkObject, 0, len(entries))
	var storedBytes, readBytes, segmentBytes, tailBytes int64
	for _, entry := range entries {
		data, err := objects.Get(ctx, entry.Key)
		if err != nil {
			b.Fatalf("get fixture object %q: %v", entry.Key, err)
		}
		fixtureObjects = append(fixtureObjects, benchmarkObject{
			key:  entry.Key,
			data: data,
		})
		storedBytes += int64(len(data))
		switch {
		case strings.Contains(entry.Key, "/commits/segments/"):
			readBytes += int64(len(data))
			segmentBytes += int64(len(data))
		case strings.Contains(entry.Key, "/commits/"):
			readBytes += int64(len(data))
			tailBytes += int64(len(data))
		case strings.HasSuffix(entry.Key, "/manifest.parquet"):
			readBytes += int64(len(data))
		}
	}
	return fixtureObjects, storedBytes, readBytes, segmentBytes, tailBytes
}

func cloneBenchmarkObjectStore(b *testing.B, fixture benchmarkCommitFixture) *MemoryStore {
	b.Helper()
	objects := NewMemoryStore()
	for _, object := range fixture.objects {
		if err := objects.Put(context.Background(), object.key, object.data); err != nil {
			b.Fatalf("copy fixture object %q: %v", object.key, err)
		}
	}
	return objects
}

func reportBenchmarkCommitFixture(b *testing.B, fixture benchmarkCommitFixture) {
	b.Helper()
	b.ReportAllocs()
	b.SetBytes(fixture.readBytes)
	b.ReportMetric(float64(fixture.manifest.Version), "commits/op")
	b.ReportMetric(float64(fixture.entityCount), "entities/op")
	b.ReportMetric(float64(len(fixture.manifest.CommitSegments)), "segments/op")
	b.ReportMetric(float64(len(fixture.manifest.CommitKeys)), "tail_commits/op")
	b.ReportMetric(float64(fixture.readBytes), "read_bytes/op")
	b.ReportMetric(float64(fixture.storedBytes), "stored_bytes/op")
	b.ReportMetric(float64(fixture.segmentBytes), "segment_bytes/op")
	b.ReportMetric(float64(fixture.tailBytes), "tail_bytes/op")
}
