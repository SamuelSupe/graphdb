package storage

import (
	"time"

	"graphdb/internal/graph"
)

func newEntityRecord(tenantID string, entity graph.Entity, page string, pageHash string, pageETag string, version int64, updatedAt time.Time) EntityRecord {
	record := EntityRecord{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      tenantID,
		ID:            entity.ID,
		Page:          page,
		PageHash:      pageHash,
		PageETag:      pageETag,
		Entity:        entity,
		Version:       version,
		UpdatedAt:     updatedAt,
	}
	stampEntityRecordHash(&record)
	return record
}

func stampEntityRecordHash(record *EntityRecord) {
	record.ContentHash = ""
	record.ContentHash = entityRecordContentHash(*record)
}
