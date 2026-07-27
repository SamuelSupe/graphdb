package storage

import (
	"context"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const commitObjectCodecParquet = "commit-arrow-parquet-v1"

func marshalParquetCommitObject(ctx context.Context, commit graph.Commit) ([]byte, error) {
	data, _, err := marshalParquetCommitObjectNormalized(ctx, commit)
	return data, err
}

func marshalParquetCommitObjectNormalized(
	ctx context.Context,
	commit graph.Commit,
) ([]byte, graph.Commit, error) {
	normalized, hash, err := normalizeCommitForParquet(commit)
	if err != nil {
		return nil, graph.Commit{}, err
	}
	data, err := marshalNormalizedParquetCommitItems(
		ctx,
		normalized.TenantID,
		[]parquetCommitTableItem{{
			Commit:      normalized,
			ContentHash: hash,
		}},
	)
	return data, normalized, err
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
