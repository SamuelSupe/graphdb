// Package codec encodes persisted graph records without publishing storage state.
package codec

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func CommitPayloadHash(commit graph.Commit) (string, error) {
	data, err := CommitPayloadJSON(commit)
	if err != nil {
		return "", err
	}
	return ContentHash(data), nil
}

func CommitPayloadJSON(commit graph.Commit) ([]byte, error) {
	commit.LayoutVersion = CurrentObjectLayoutVersion
	return json.Marshal(legacyCommitWire(commit))
}

const commitObjectCodecParquet = "commit-arrow-parquet-v1"

func MarshalCommit(ctx context.Context, commit graph.Commit) ([]byte, error) {
	data, _, err := MarshalCommitNormalized(ctx, commit)
	return data, err
}

func MarshalCommitNormalized(
	ctx context.Context,
	commit graph.Commit,
) ([]byte, graph.Commit, error) {
	normalized, hash, err := NormalizeCommitForParquet(commit)
	if err != nil {
		return nil, graph.Commit{}, err
	}
	data, err := MarshalNormalizedCommitItems(
		ctx,
		normalized.TenantID,
		[]CommitTableItem{{
			Commit:      normalized,
			ContentHash: hash,
		}},
	)
	return data, normalized, err
}

func DecodeCommit(ctx context.Context, data []byte) (graph.Commit, error) {
	items, err := DecodeCommitItems(ctx, data)
	if err != nil {
		return graph.Commit{}, err
	}
	if len(items) != 1 {
		return graph.Commit{}, fmt.Errorf("parquet commit object has %d commits, want 1", len(items))
	}
	return items[0].Commit, nil
}
