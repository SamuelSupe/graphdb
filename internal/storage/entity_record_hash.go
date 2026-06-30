package storage

import "graphdb/internal/graph"

func entityRecordContentHash(record EntityRecord) string {
	return indexContentHash(struct {
		ID       string       `json:"id"`
		Page     string       `json:"page"`
		PageHash string       `json:"page_hash,omitempty"`
		PageETag string       `json:"page_etag,omitempty"`
		Entity   graph.Entity `json:"entity"`
		Deleted  bool         `json:"deleted,omitempty"`
	}{
		ID:       record.ID,
		Page:     record.Page,
		PageHash: record.PageHash,
		PageETag: record.PageETag,
		Entity:   record.Entity,
		Deleted:  record.Deleted,
	})
}
