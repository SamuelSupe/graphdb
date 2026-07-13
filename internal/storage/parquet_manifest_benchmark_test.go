package storage

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/parquet/file"
)

func BenchmarkMarshalParquetManifestTail(b *testing.B) {
	manifest := benchmarkManifestTail()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := marshalParquetManifest(context.Background(), manifest); err != nil {
			b.Fatal(err)
		}
	}
}

func TestMarshalParquetManifestBatchesTailRows(t *testing.T) {
	data, err := marshalParquetManifest(context.Background(), benchmarkManifestTail())
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

func benchmarkManifestTail() Manifest {
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      "tenant-a",
		Version:       319,
		HeadCommitID:  "head",
		UpdatedAt:     time.Unix(319, 0).UTC(),
	}
	for i := 0; i < 4; i++ {
		manifest.CommitSegments = append(manifest.CommitSegments, CommitSegmentRef{
			Key:          fmt.Sprintf("segments/%d.parquet", i),
			Codec:        commitSegmentCodecParquet,
			FirstVersion: int64(i*64 + 1),
			LastVersion:  int64((i + 1) * 64),
			Count:        64,
			ContentHash:  fmt.Sprintf("segment-hash-%d", i),
		})
	}
	for i := 0; i < commitSegmentTargetCount-1; i++ {
		manifest.CommitKeys = append(manifest.CommitKeys, fmt.Sprintf("commits/%d.parquet", i+257))
	}
	return manifest
}
