package storage

import (
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
)

// legacySnapshotRecord preserves the 1.0 JSON field order and entity wire
// shape used by snapshot content hashes.
type legacySnapshotRecord struct {
	LayoutVersion int                  `json:"layout_version,omitempty"`
	TenantID      string               `json:"tenant_id,omitempty"`
	Version       int64                `json:"version"`
	CITypes       []graph.CIType       `json:"ci_types,omitempty"`
	Entities      []legacyEntity       `json:"entities"`
	RelationTypes []graph.RelationType `json:"relation_types"`
	Edges         []graph.Edge         `json:"edges"`
	Index         *graph.IndexSnapshot `json:"index,omitempty"`
}

func legacySnapshotRecordWire(record snapshotRecord) legacySnapshotRecord {
	return legacySnapshotRecord{
		LayoutVersion: record.LayoutVersion,
		TenantID:      record.TenantID,
		Version:       record.Snapshot.Version,
		CITypes:       record.Snapshot.CITypes,
		Entities:      codec.LegacyEntities(record.Snapshot.Entities),
		RelationTypes: record.Snapshot.RelationTypes,
		Edges:         codec.EdgeSliceTimesUTC(record.Snapshot.Edges),
		Index:         record.Snapshot.Index,
	}
}
