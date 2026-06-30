package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"graphdb/internal/graph"
)

func (s *TenantStore) putSnapshotRecordIfAbsentOrEquivalent(ctx context.Context, key string, record snapshotRecord) error {
	data, err := marshalParquetSnapshotRecord(ctx, record)
	if err != nil {
		return err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return err
	}
	if bytes.Equal(existing, data) {
		return nil
	}
	if snapshotRecordEquivalent(ctx, existing, key, record) {
		return s.putBytesWithMeta(ctx, key, data, meta)
	}
	return fmt.Errorf("%w: object %q already exists with different content", ErrConflict, key)
}

func (s *TenantStore) loadSnapshotRecord(ctx context.Context, key string) (snapshotRecord, error) {
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return snapshotRecord{}, err
	}
	return decodeSnapshotRecordObject(ctx, data)
}

func decodeSnapshotRecordObject(ctx context.Context, data []byte) (snapshotRecord, error) {
	if !isParquetBytes(data) {
		return snapshotRecord{}, fmt.Errorf("unsupported snapshot record: only parquet snapshots are readable")
	}
	return decodeParquetSnapshotRecord(ctx, data)
}

func snapshotRecordEquivalent(ctx context.Context, existing []byte, key string, want snapshotRecord) bool {
	got, err := decodeSnapshotRecordObject(ctx, existing)
	if err != nil {
		return false
	}
	if err := validateSnapshotObjectIdentity(key, got.Snapshot); err != nil {
		return false
	}
	gotGraph, err := graph.FromSnapshot(got.Snapshot)
	if err != nil {
		return false
	}
	wantGraph, err := graph.FromSnapshot(want.Snapshot)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(gotGraph.Snapshot(), wantGraph.Snapshot())
}

func snapshotRecordPayloadJSON(record snapshotRecord) ([]byte, error) {
	record.LayoutVersion = CurrentObjectLayoutVersion
	record.Snapshot.Index = nil
	if record.Snapshot.Entities == nil {
		record.Snapshot.Entities = []graph.Entity{}
	}
	if record.Snapshot.RelationTypes == nil {
		record.Snapshot.RelationTypes = []graph.RelationType{}
	}
	if record.Snapshot.Edges == nil {
		record.Snapshot.Edges = []graph.Edge{}
	}
	return json.Marshal(record)
}

func snapshotRecordContentHash(record snapshotRecord) (string, error) {
	data, err := snapshotRecordPayloadJSON(record)
	if err != nil {
		return "", err
	}
	return objectContentHash(data), nil
}
