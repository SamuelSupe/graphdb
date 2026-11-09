package ingest

import (
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func BuildIngestMutations(request IngestRequest) (IngestResult, graph.Mutations, []int) {
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
				entity.ExternalID = FirstValue(item.ExternalID, entity.ID)
			}
			if entity.ExternalID != "" &&
				strings.TrimSpace(entity.Source) == request.Source &&
				(request.StaleKind == "" ||
					strings.TrimSpace(entity.Kind) == request.StaleKind) {
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
				edge.ExternalID = FirstValue(item.ExternalID, edge.ID)
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
	if request.FullSync && result.Failed == 0 {
		mutations.MarkSourceStale = append(mutations.MarkSourceStale, graph.SourceStaleRequest{
			Source:              request.Source,
			Kind:                request.StaleKind,
			ObservedExternalIDs: observed,
			Action:              FirstValue(request.StaleAction, "mark_stale"),
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

func FirstValue(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
