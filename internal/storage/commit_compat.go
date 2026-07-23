package storage

import (
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

// legacyEntity has the 1.0 Entity fields and JSON tags without 1.1's
// API-only MarshalJSON method.
type legacyEntity graph.Entity

type legacySplitRequest struct {
	SourceID string         `json:"source_id"`
	Entities []legacyEntity `json:"entities"`
}

type legacyMutations struct {
	UpsertCITypes        []graph.CIType              `json:"upsert_ci_types,omitempty"`
	DeleteCITypes        []string                    `json:"delete_ci_types,omitempty"`
	UpsertRelationTypes  []graph.RelationType        `json:"upsert_relation_types,omitempty"`
	DeleteRelationTypes  []string                    `json:"delete_relation_types,omitempty"`
	UpsertEntities       []legacyEntity              `json:"upsert_entities,omitempty"`
	DeleteEntities       []string                    `json:"delete_entities,omitempty"`
	DeleteEntityRequests []graph.EntityDeleteRequest `json:"delete_entity_requests,omitempty"`
	MarkSourceStale      []graph.SourceStaleRequest  `json:"mark_source_stale,omitempty"`
	UpsertEdges          []graph.Edge                `json:"upsert_edges,omitempty"`
	DeleteEdges          []string                    `json:"delete_edges,omitempty"`
	DeleteEdgeRequests   []graph.EdgeDeleteRequest   `json:"delete_edge_requests,omitempty"`
	MergeEntities        []graph.MergeRequest        `json:"merge_entities,omitempty"`
	SplitEntities        []legacySplitRequest        `json:"split_entities,omitempty"`
}

type legacyCommit struct {
	LayoutVersion int             `json:"layout_version,omitempty"`
	ID            string          `json:"id"`
	TenantID      string          `json:"tenant_id"`
	Version       int64           `json:"version"`
	CreatedAt     time.Time       `json:"created_at"`
	Mutations     legacyMutations `json:"mutations"`
}

func legacyCommitWire(commit graph.Commit) legacyCommit {
	return legacyCommit{
		LayoutVersion: commit.LayoutVersion,
		ID:            commit.ID,
		TenantID:      commit.TenantID,
		Version:       commit.Version,
		CreatedAt:     commit.CreatedAt,
		Mutations:     legacyMutationWire(commit.Mutations),
	}
}

func legacyMutationWire(mutations graph.Mutations) legacyMutations {
	return legacyMutations{
		UpsertCITypes:        mutations.UpsertCITypes,
		DeleteCITypes:        mutations.DeleteCITypes,
		UpsertRelationTypes:  mutations.UpsertRelationTypes,
		DeleteRelationTypes:  mutations.DeleteRelationTypes,
		UpsertEntities:       legacyEntities(mutations.UpsertEntities),
		DeleteEntities:       mutations.DeleteEntities,
		DeleteEntityRequests: mutations.DeleteEntityRequests,
		MarkSourceStale:      mutations.MarkSourceStale,
		UpsertEdges:          mutations.UpsertEdges,
		DeleteEdges:          mutations.DeleteEdges,
		DeleteEdgeRequests:   mutations.DeleteEdgeRequests,
		MergeEntities:        mutations.MergeEntities,
		SplitEntities:        legacySplits(mutations.SplitEntities),
	}
}

func legacyEntities(entities []graph.Entity) []legacyEntity {
	if entities == nil {
		return nil
	}
	result := make([]legacyEntity, len(entities))
	for i := range entities {
		result[i] = legacyEntity(entities[i])
	}
	return result
}

func legacySplits(splits []graph.SplitRequest) []legacySplitRequest {
	if splits == nil {
		return nil
	}
	result := make([]legacySplitRequest, len(splits))
	for i := range splits {
		result[i] = legacySplitRequest{
			SourceID: splits[i].SourceID,
			Entities: legacyEntities(splits[i].Entities),
		}
	}
	return result
}
