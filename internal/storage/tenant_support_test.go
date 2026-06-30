package storage

import (
	"strings"
	"testing"
)

func TestObjectSegmentEscapesPathControlSegments(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")

	batchKey := store.ingestBatchKey("tenant-a", ".", "collector-a", "batch-1")
	if !strings.Contains(batchKey, "/ingest/%2E/batches/collector-a/") {
		t.Fatalf("batch key = %q, want escaped dot source segment", batchKey)
	}

	shardKey := store.parquetEdgeShardVersionKey("tenant-a", 1, "..", "aa")
	if !strings.Contains(shardKey, "/indexes/parquet/versions/v1/edges/%2E%2E/aa.parquet") {
		t.Fatalf("edge shard key = %q, want escaped dot-dot relation segment", shardKey)
	}
}
