package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) putCommitObjectIfAbsent(ctx context.Context, key string, commit graph.Commit) error {
	_, err := s.putCommitObjectIfAbsentMeta(ctx, key, commit)
	return err
}

func (s *TenantStore) putCommitObjectIfAbsentMeta(ctx context.Context, key string, commit graph.Commit) (ObjectMeta, error) {
	meta, _, err := s.putCommitObjectIfAbsentMetaNormalized(
		ctx, key, commit,
	)
	return meta, err
}

func (s *TenantStore) putCommitObjectIfAbsentMetaNormalized(
	ctx context.Context,
	key string,
	commit graph.Commit,
) (ObjectMeta, graph.Commit, error) {
	data, normalized, err := marshalParquetCommitObjectNormalized(ctx, commit)
	if err != nil {
		return ObjectMeta{}, graph.Commit{}, err
	}
	meta, err := s.Objects.PutConditional(
		ctx, key, data, PutCondition{IfNoneMatch: true},
	)
	return meta, normalized, err
}

func (s *TenantStore) getCommitObject(ctx context.Context, key string) (graph.Commit, error) {
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return graph.Commit{}, err
	}
	return unmarshalCommitObjectWithContext(ctx, data)
}

func marshalCommitObject(commit graph.Commit) ([]byte, error) {
	return marshalParquetCommitObject(context.Background(), commit)
}

func unmarshalCommitObject(data []byte) (graph.Commit, error) {
	return unmarshalCommitObjectWithContext(context.Background(), data)
}

func unmarshalCommitObjectWithContext(ctx context.Context, data []byte) (graph.Commit, error) {
	if !isParquetBytes(data) {
		return graph.Commit{}, fmt.Errorf("unsupported commit object: only parquet commits are readable")
	}
	return decodeParquetCommitObject(ctx, data)
}

func isParquetBytes(data []byte) bool {
	return len(data) >= 4 && string(data[:4]) == "PAR1"
}

func commitPayloadHash(commit graph.Commit) (string, error) {
	data, err := commitPayloadJSON(commit)
	if err != nil {
		return "", err
	}
	return objectContentHash(data), nil
}

func commitPayloadJSON(commit graph.Commit) ([]byte, error) {
	commit.LayoutVersion = CurrentObjectLayoutVersion
	return json.Marshal(legacyCommitWire(commit))
}
