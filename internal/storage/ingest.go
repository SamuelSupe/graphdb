package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"graphdb/internal/graph"
)

type IngestRequest struct {
	Source         string       `json:"source"`
	CollectorID    string       `json:"collector_id"`
	BatchID        string       `json:"batch_id,omitempty"`
	IdempotencyKey string       `json:"idempotency_key,omitempty"`
	Cursor         string       `json:"cursor,omitempty"`
	FullSync       bool         `json:"full_sync,omitempty"`
	StaleAction    string       `json:"stale_action,omitempty"`
	StaleKind      string       `json:"stale_kind,omitempty"`
	Items          []IngestItem `json:"items"`
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
	Suppressed int              `json:"suppressed,omitempty"`
	Skipped    bool             `json:"skipped"`
	Cursor     string           `json:"cursor,omitempty"`
	Failures   []IngestFailure  `json:"failures,omitempty"`
	Conflicts  []IngestConflict `json:"conflicts,omitempty"`
}

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

func (s *TenantStore) Ingest(ctx context.Context, tenantID string, request IngestRequest) (IngestResult, error) {
	return s.ingest(ctx, tenantID, request, true)
}

func (s *TenantStore) ingest(ctx context.Context, tenantID string, request IngestRequest, saveFailures bool) (IngestResult, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IngestResult{}, err
	}
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return IngestResult{}, pressure
		}
		return IngestResult{}, err
	}
	request = normalizeIngestRequest(request)
	if request.Source == "" || request.CollectorID == "" {
		return IngestResult{}, fmt.Errorf("source and collector_id are required")
	}
	if request.BatchID == "" {
		request.BatchID = defaultIngestBatchID(request)
	}
	if err := s.CheckWriteBackpressure(ctx, tenantID); err != nil {
		return IngestResult{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	started := time.Now().UTC()
	if previousRecord, ok, err := s.loadIngestRecord(ctx, tenantID, request); err != nil {
		if pressure := s.objectStoreBackpressureError(err); pressure != nil {
			return IngestResult{}, pressure
		}
		return IngestResult{}, err
	} else if ok {
		previous := previousRecord.Result
		previous.Skipped = true
		if err := s.repairIngestMetadataAfterSkip(ctx, tenantID, previousRecord, saveFailures); err != nil {
			return previous, err
		}
		return previous, nil
	}
	result, mutations, appliedIndices := buildIngestMutations(request)
	result.BatchID = request.BatchID
	result.Cursor = request.Cursor
	pendingApplied := result.Applied
	if pendingApplied > 0 {
		if err := s.acquireWriterLease(ctx, tenantID); err != nil {
			if pressure := s.objectStoreBackpressureError(err); pressure != nil {
				return IngestResult{}, pressure
			}
			return IngestResult{}, err
		}
		if err := s.CheckWriteBackpressure(ctx, tenantID); err != nil {
			return IngestResult{}, err
		}
		commitResult, err := s.commitWithRetryLocked(ctx, tenantID, mutations, CommitOptions{})
		if err != nil {
			if errors.Is(err, ErrBackpressure) {
				return IngestResult{}, err
			}
			if pressure := s.objectStoreBackpressureError(err); pressure != nil {
				return IngestResult{}, pressure
			}
			result.Failed += pendingApplied
			result.Applied = 0
			for _, index := range appliedIndices {
				result.Failures = append(result.Failures, IngestFailure{Index: index, ExternalID: request.Items[index].ExternalID, Error: err.Error()})
			}
			result.Conflicts = append(result.Conflicts, IngestConflict{Message: err.Error()})
		} else {
			result.Version = commitResult.Version
			result.Skipped = commitResult.Skipped
			result.Suppressed = len(commitResult.Suppressed)
			result.Conflicts = append(result.Conflicts, ingestConflicts(request, commitResult.Suppressed)...)
		}
	}
	finished := time.Now().UTC()
	var metadataErr error
	if err := s.saveIngestBatch(ctx, tenantID, IngestBatchRecord{Request: request, Result: result, StartedAt: started, FinishedAt: finished}); err != nil {
		metadataErr = errors.Join(metadataErr, fmt.Errorf("save ingest batch: %w", err))
	}
	if err := s.saveCollectorStatus(ctx, tenantID, request, result, started, finished); err != nil {
		metadataErr = errors.Join(metadataErr, fmt.Errorf("save collector status: %w", err))
	}
	if saveFailures && result.Failed > 0 {
		if err := s.saveDeadLetter(ctx, tenantID, request, result); err != nil {
			metadataErr = errors.Join(metadataErr, fmt.Errorf("save dead letter: %w", err))
		}
	}
	if metadataErr != nil {
		return result, metadataErr
	}
	if result.Failed > 0 {
		return result, nil
	}
	return result, nil
}

func normalizeIngestRequest(request IngestRequest) IngestRequest {
	request.Source = strings.TrimSpace(request.Source)
	request.CollectorID = strings.TrimSpace(request.CollectorID)
	request.BatchID = strings.TrimSpace(request.BatchID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.StaleAction = strings.TrimSpace(request.StaleAction)
	request.StaleKind = strings.TrimSpace(request.StaleKind)
	return request
}

func ingestConflicts(request IngestRequest, suppressed []graph.FieldConflict) []IngestConflict {
	out := make([]IngestConflict, 0, len(suppressed))
	for _, conflict := range suppressed {
		index, externalID, _ := findIngestConflictItem(request, conflict)
		out = append(out, IngestConflict{
			ResourceType:     conflict.ResourceType,
			Index:            index,
			ExternalID:       externalID,
			EntityID:         conflict.EntityID,
			EdgeID:           conflict.EdgeID,
			CanonicalID:      conflict.CanonicalID,
			IncomingID:       conflict.IncomingID,
			Field:            conflict.Field,
			ExistingSource:   conflict.ExistingSource,
			ExistingPriority: conflict.ExistingPriority,
			IncomingSource:   conflict.IncomingSource,
			IncomingPriority: conflict.IncomingPriority,
			ExistingValue:    conflict.ExistingValue,
			IncomingValue:    conflict.IncomingValue,
			Message:          conflict.Message,
		})
	}
	return out
}

func findIngestConflictItem(request IngestRequest, conflict graph.FieldConflict) (int, string, bool) {
	for index, item := range request.Items {
		if ingestConflictMatchesItem(conflict, item) {
			return index, item.ExternalID, true
		}
	}
	return 0, "", false
}

func ingestConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	if conflict.ResourceType == "edge" {
		return edgeConflictMatchesItem(conflict, item)
	}
	return entityConflictMatchesItem(conflict, item)
}

func entityConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	if item.Entity != nil {
		entityID := strings.TrimSpace(item.Entity.ID)
		entityExternalID := strings.TrimSpace(item.Entity.ExternalID)
		itemExternalID := strings.TrimSpace(item.ExternalID)
		if conflict.IncomingID != "" {
			return conflict.IncomingID == entityExternalID ||
				conflict.IncomingID == itemExternalID ||
				conflict.IncomingID == entityID
		}
		return conflict.EntityID != "" && conflict.EntityID == entityID
	}
	if item.DeleteEntity == nil {
		return false
	}
	entityID := strings.TrimSpace(item.DeleteEntity.ID)
	entityExternalID := strings.TrimSpace(item.DeleteEntity.ExternalID)
	itemExternalID := strings.TrimSpace(item.ExternalID)
	if conflict.IncomingID != "" {
		return conflict.IncomingID == entityExternalID ||
			conflict.IncomingID == itemExternalID ||
			conflict.IncomingID == entityID
	}
	return conflict.EntityID != "" && conflict.EntityID == entityID
}

func edgeConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	if item.Edge != nil && edgeUpsertConflictMatchesItem(conflict, item) {
		return true
	}
	if item.DeleteEdge != nil && edgeDeleteConflictMatchesItem(conflict, item) {
		return true
	}
	return false
}

func edgeUpsertConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	edge := item.Edge
	incomingID := firstTrimmed(edge.ID, edge.ExternalID, item.ExternalID)
	if conflict.IncomingID != "" && conflict.IncomingID == incomingID {
		return true
	}
	canonicalID, ok := canonicalEdgeIDFromParts(edge.Type, edge.From, edge.To)
	if !ok {
		return false
	}
	return conflict.CanonicalID == canonicalID || conflict.EdgeID == canonicalID
}

func edgeDeleteConflictMatchesItem(conflict graph.FieldConflict, item IngestItem) bool {
	request := item.DeleteEdge
	incomingID := firstTrimmed(request.ID, item.ExternalID)
	if conflict.IncomingID != "" && conflict.IncomingID == incomingID {
		return true
	}
	canonicalID, ok := canonicalEdgeIDFromParts(request.Type, request.From, request.To)
	if !ok {
		return false
	}
	return conflict.IncomingID == canonicalID || conflict.CanonicalID == canonicalID || conflict.EdgeID == canonicalID
}

func canonicalEdgeIDFromParts(edgeType string, from string, to string) (string, bool) {
	edgeType = strings.TrimSpace(edgeType)
	from = strings.TrimSpace(from)
	to = strings.TrimSpace(to)
	if edgeType == "" || from == "" || to == "" {
		return "", false
	}
	return graph.CanonicalEdgeIDParts(edgeType, from, to), true
}

func firstTrimmed(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func buildIngestMutations(request IngestRequest) (IngestResult, graph.Mutations, []int) {
	var mutations graph.Mutations
	result := IngestResult{}
	appliedIndices := make([]int, 0, len(request.Items))
	observed := make([]string, 0, len(request.Items))
	for idx, item := range request.Items {
		if count := ingestItemPayloadCount(item); count != 1 {
			result.Failed++
			result.Failures = append(result.Failures, IngestFailure{Index: idx, ExternalID: item.ExternalID, Error: ingestItemPayloadError(count)})
			continue
		}
		if item.CIType != nil {
			mutations.UpsertCITypes = append(mutations.UpsertCITypes, *item.CIType)
			result.Applied++
			appliedIndices = append(appliedIndices, idx)
			continue
		}
		if item.Relation != nil {
			mutations.UpsertRelationTypes = append(mutations.UpsertRelationTypes, *item.Relation)
			result.Applied++
			appliedIndices = append(appliedIndices, idx)
			continue
		}
		if item.Entity != nil {
			entity := *item.Entity
			if entity.Source == "" {
				entity.Source = request.Source
			}
			if entity.ExternalID == "" {
				entity.ExternalID = firstValue(item.ExternalID, entity.ID)
			}
			if entity.ExternalID != "" {
				observed = append(observed, entity.ExternalID)
			}
			mutations.UpsertEntities = append(mutations.UpsertEntities, entity)
			result.Applied++
			appliedIndices = append(appliedIndices, idx)
			continue
		}
		if item.Edge != nil {
			edge := *item.Edge
			if edge.Source == "" {
				edge.Source = request.Source
			}
			if edge.ExternalID == "" {
				edge.ExternalID = firstValue(item.ExternalID, edge.ID)
			}
			mutations.UpsertEdges = append(mutations.UpsertEdges, edge)
			result.Applied++
			appliedIndices = append(appliedIndices, idx)
			continue
		}
		if item.DeleteEntity != nil {
			deleteEntity := *item.DeleteEntity
			if deleteEntity.Source == "" {
				deleteEntity.Source = request.Source
			}
			if deleteEntity.ID == "" {
				deleteEntity.ID = item.ExternalID
			}
			mutations.DeleteEntityRequests = append(mutations.DeleteEntityRequests, deleteEntity)
			result.Applied++
			appliedIndices = append(appliedIndices, idx)
			continue
		}
		if item.DeleteEdge != nil {
			deleteEdge := *item.DeleteEdge
			if deleteEdge.Source == "" {
				deleteEdge.Source = request.Source
			}
			if deleteEdge.ID == "" {
				deleteEdge.ID = item.ExternalID
			}
			mutations.DeleteEdgeRequests = append(mutations.DeleteEdgeRequests, deleteEdge)
			result.Applied++
			appliedIndices = append(appliedIndices, idx)
			continue
		}
	}
	if request.FullSync {
		mutations.MarkSourceStale = append(mutations.MarkSourceStale, graph.SourceStaleRequest{
			Source:              request.Source,
			Kind:                request.StaleKind,
			ObservedExternalIDs: observed,
			Action:              firstValue(request.StaleAction, "mark_stale"),
		})
		if result.Applied == 0 && result.Failed == 0 {
			result.Applied = 1
		}
	}
	return result, mutations, appliedIndices
}

func ingestItemPayloadCount(item IngestItem) int {
	count := 0
	if item.CIType != nil {
		count++
	}
	if item.Relation != nil {
		count++
	}
	if item.Entity != nil {
		count++
	}
	if item.Edge != nil {
		count++
	}
	if item.DeleteEntity != nil {
		count++
	}
	if item.DeleteEdge != nil {
		count++
	}
	return count
}

func ingestItemPayloadError(count int) string {
	if count == 0 {
		return "item must include exactly one of entity, edge, delete_entity, delete_edge, relation_type, ci_type"
	}
	return "item must not include more than one of entity, edge, delete_entity, delete_edge, relation_type, ci_type"
}

func firstValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func defaultIngestBatchID(request IngestRequest) string {
	if request.IdempotencyKey != "" {
		sum := sha256.Sum256([]byte(request.Source + "\x00" + request.CollectorID + "\x00" + request.IdempotencyKey))
		return request.Source + "-" + request.CollectorID + "-" + hex.EncodeToString(sum[:])[:16]
	}
	return request.Source + "-" + request.CollectorID + "-" + time.Now().UTC().Format("20060102150405.000000000")
}
