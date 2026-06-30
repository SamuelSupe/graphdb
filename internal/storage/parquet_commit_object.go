package storage

import (
	"context"
	"fmt"

	"graphdb/internal/graph"
)

const commitObjectCodecParquet = "commit-arrow-parquet-v1"

func marshalParquetCommitObject(ctx context.Context, commit graph.Commit) ([]byte, error) {
	return marshalParquetCommitItems(ctx, commit.TenantID, []parquetCommitTableItem{{Commit: commit}})
}

func decodeParquetCommitObject(ctx context.Context, data []byte) (graph.Commit, error) {
	items, err := decodeParquetCommitItems(ctx, data)
	if err != nil {
		return graph.Commit{}, err
	}
	if len(items) != 1 {
		return graph.Commit{}, fmt.Errorf("parquet commit object has %d commits, want 1", len(items))
	}
	return items[0].Commit, nil
}

func parquetCommitObjectSchemaHash() string {
	return parquetCommitSchemaHash(commitObjectCodecParquet)
}
