// Package ingest owns ingestion contracts, validation and mutation conversion.
package ingest

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

type IngestRequest struct {
	Source          string               `json:"source"`
	CollectorID     string               `json:"collector_id"`
	BatchID         string               `json:"batch_id,omitempty"`
	IdempotencyKey  string               `json:"idempotency_key,omitempty"`
	Cursor          string               `json:"cursor,omitempty"`
	FullSync        bool                 `json:"full_sync,omitempty"`
	StaleAction     string               `json:"stale_action,omitempty"`
	StaleKind       string               `json:"stale_kind,omitempty"`
	ExpectedVersion *int64               `json:"expected_version,omitempty"`
	FailureMode     string               `json:"failure_mode,omitempty"`
	Preconditions   []IngestPrecondition `json:"preconditions,omitempty"`
	Items           []IngestItem         `json:"items"`
}

type IngestItem struct {
	ExternalID   string                     `json:"external_id"`
	Entity       *graph.Entity              `json:"entity,omitempty"`
	Edge         *graph.Edge                `json:"edge,omitempty"`
	DeleteEntity *graph.EntityDeleteRequest `json:"delete_entity,omitempty"`
	DeleteEdge   *graph.EdgeDeleteRequest   `json:"delete_edge,omitempty"`
	Relation     *graph.RelationType        `json:"relation_type,omitempty"`
	CIType       *graph.CIType              `json:"ci_type,omitempty"`
}

type IngestResult struct {
	BatchID    string           `json:"batch_id"`
	Version    int64            `json:"version"`
	Applied    int              `json:"applied"`
	Failed     int              `json:"failed"`
	ErrorCode  string           `json:"error_code,omitempty"`
	Suppressed int              `json:"suppressed,omitempty"`
	Skipped    bool             `json:"skipped"`
	SkipReason string           `json:"skip_reason,omitempty"`
	Cursor     string           `json:"cursor,omitempty"`
	Failures   []IngestFailure  `json:"failures,omitempty"`
	Conflicts  []IngestConflict `json:"conflicts,omitempty"`
}

const (
	IngestSkipReasonIdempotentReplay = "idempotent_replay"
	IngestSkipReasonLogicalNoop      = "logical_noop"
)

type IngestFailure struct {
	Index      int    `json:"index"`
	ExternalID string `json:"external_id,omitempty"`
	Error      string `json:"error"`
}

type IngestConflict struct {
	ResourceType     string `json:"resource_type,omitempty"`
	Index            int    `json:"index"`
	ExternalID       string `json:"external_id,omitempty"`
	ExistingID       string `json:"existing_id,omitempty"`
	EntityID         string `json:"entity_id,omitempty"`
	EdgeID           string `json:"edge_id,omitempty"`
	CanonicalID      string `json:"canonical_id,omitempty"`
	IncomingID       string `json:"incoming_id,omitempty"`
	Field            string `json:"field,omitempty"`
	AliasField       string `json:"alias_field,omitempty"`
	ExistingSource   string `json:"existing_source,omitempty"`
	ExistingPriority int    `json:"existing_priority,omitempty"`
	IncomingSource   string `json:"incoming_source,omitempty"`
	IncomingPriority int    `json:"incoming_priority,omitempty"`
	ExistingValue    any    `json:"existing_value,omitempty"`
	IncomingValue    any    `json:"incoming_value,omitempty"`
	Message          string `json:"message"`
}

type CollectorStatus struct {
	TenantID       string    `json:"tenant_id"`
	Source         string    `json:"source"`
	CollectorID    string    `json:"collector_id"`
	LastBatchID    string    `json:"last_batch_id,omitempty"`
	LastCursor     string    `json:"last_cursor,omitempty"`
	LastVersion    int64     `json:"last_version"`
	LastStartedAt  time.Time `json:"last_started_at,omitempty"`
	LastFinishedAt time.Time `json:"last_finished_at,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	AppliedTotal   int       `json:"applied_total"`
	FailedTotal    int       `json:"failed_total"`
}

type IngestBatchRecord struct {
	TenantID   string        `json:"tenant_id,omitempty"`
	Request    IngestRequest `json:"request"`
	Result     IngestResult  `json:"result"`
	StartedAt  time.Time     `json:"started_at"`
	FinishedAt time.Time     `json:"finished_at"`
}

func (item *IngestItem) UnmarshalJSON(data []byte) error {
	type ingestItemAlias IngestItem
	var wire struct {
		*ingestItemAlias
		EntityType *graph.EntityType `json:"entity_type"`
	}
	wire.ingestItemAlias = (*ingestItemAlias)(item)
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.EntityType == nil {
		return nil
	}
	if item.CIType != nil {
		return fmt.Errorf("entity_type and ci_type cannot be used together")
	}
	item.CIType = wire.EntityType
	return nil
}
