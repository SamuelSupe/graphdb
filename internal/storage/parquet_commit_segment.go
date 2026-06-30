package storage

import (
	"context"
	"fmt"
)

func marshalParquetCommitSegment(ctx context.Context, tenantID string, items []commitSegmentItem) ([]byte, error) {
	parquetItems := make([]parquetCommitTableItem, 0, len(items))
	for _, item := range items {
		parquetItems = append(parquetItems, parquetCommitTableItem{Key: item.Key, Commit: item.Commit})
	}
	return marshalParquetCommitItems(ctx, tenantID, parquetItems)
}

func decodeParquetCommitSegmentObject(ctx context.Context, data []byte, tenantID string, ref CommitSegmentRef) (storedCommitSegment, []commitSegmentItem, error) {
	parquetItems, err := decodeParquetCommitItems(ctx, data)
	if err != nil {
		return storedCommitSegment{}, nil, err
	}
	object := storedCommitSegment{
		LayoutVersion: CurrentObjectLayoutVersion,
		Kind:          "commit-segment",
		Codec:         commitSegmentCodecParquet,
		TenantID:      tenantID,
		PayloadBytes:  len(data),
	}
	items := make([]commitSegmentItem, 0, len(parquetItems))
	for _, item := range parquetItems {
		items = append(items, commitSegmentItem{Key: item.Key, Commit: item.Commit})
	}
	if len(items) == 0 {
		return storedCommitSegment{}, nil, fmt.Errorf("empty commit segment")
	}
	payload, err := marshalCommitSegmentPayload(items)
	if err != nil {
		return storedCommitSegment{}, nil, err
	}
	object.FirstVersion = items[0].Commit.Version
	object.LastVersion = items[len(items)-1].Commit.Version
	object.Count = len(items)
	object.ContentHash = objectContentHash(payload)
	object.CreatedAt = items[len(items)-1].Commit.CreatedAt
	if ref.ContentHash != "" && ref.ContentHash != object.ContentHash {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment content hash mismatch")
	}
	return object, items, nil
}
