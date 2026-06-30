package graph

import (
	"fmt"
	"strings"
)

func NormalizeSourcePolicy(policy SourcePolicy) (SourcePolicy, error) {
	seen := map[string]struct{}{}
	items := make([]SourcePolicyItem, 0, len(policy.Sources))
	for _, item := range policy.Sources {
		item.Name = strings.TrimSpace(item.Name)
		if item.Name == "" {
			return SourcePolicy{}, fmt.Errorf("source policy source name is required")
		}
		if _, ok := seen[item.Name]; ok {
			return SourcePolicy{}, fmt.Errorf("duplicate source policy source %q", item.Name)
		}
		seen[item.Name] = struct{}{}
		items = append(items, item)
	}
	policy.Sources = items
	return policy, nil
}

func (p SourcePolicy) PriorityFor(source string, fallback int) int {
	source = strings.TrimSpace(source)
	for _, item := range p.Sources {
		if item.Name == source {
			return item.Priority
		}
	}
	return p.DefaultPriority
}

func ApplySourcePolicy(mutations Mutations, policy SourcePolicy) Mutations {
	if len(mutations.UpsertEntities) == 0 && len(mutations.UpsertEdges) == 0 && len(mutations.DeleteEdgeRequests) == 0 &&
		len(mutations.DeleteEntityRequests) == 0 && len(mutations.MarkSourceStale) == 0 {
		return mutations
	}
	mutations.UpsertEntities = append([]Entity(nil), mutations.UpsertEntities...)
	for i := range mutations.UpsertEntities {
		applyPolicyToEntity(&mutations.UpsertEntities[i], policy)
	}
	mutations.DeleteEntityRequests = append([]EntityDeleteRequest(nil), mutations.DeleteEntityRequests...)
	for i := range mutations.DeleteEntityRequests {
		applyPolicyToEntityDelete(&mutations.DeleteEntityRequests[i], policy)
	}
	mutations.MarkSourceStale = append([]SourceStaleRequest(nil), mutations.MarkSourceStale...)
	for i := range mutations.MarkSourceStale {
		applyPolicyToSourceStale(&mutations.MarkSourceStale[i], policy)
	}
	mutations.UpsertEdges = append([]Edge(nil), mutations.UpsertEdges...)
	for i := range mutations.UpsertEdges {
		applyPolicyToEdge(&mutations.UpsertEdges[i], policy)
	}
	mutations.DeleteEdgeRequests = append([]EdgeDeleteRequest(nil), mutations.DeleteEdgeRequests...)
	for i := range mutations.DeleteEdgeRequests {
		applyPolicyToEdgeDelete(&mutations.DeleteEdgeRequests[i], policy)
	}
	return mutations
}

func applyPolicyToEntity(entity *Entity, policy SourcePolicy) {
	entity.Source = strings.TrimSpace(entity.Source)
	entity.SourceRank = policy.PriorityFor(entity.Source, entity.SourceRank)
	entity.Sources = append([]EntitySource(nil), entity.Sources...)
	for i := range entity.Sources {
		entity.Sources[i].Source = strings.TrimSpace(entity.Sources[i].Source)
		entity.Sources[i].Priority = policy.PriorityFor(entity.Sources[i].Source, entity.Sources[i].Priority)
	}
}

func applyPolicyToEdge(edge *Edge, policy SourcePolicy) {
	edge.Source = strings.TrimSpace(edge.Source)
	edge.SourceRank = policy.PriorityFor(edge.Source, edge.SourceRank)
	edge.Sources = append([]EdgeSource(nil), edge.Sources...)
	for i := range edge.Sources {
		edge.Sources[i].Source = strings.TrimSpace(edge.Sources[i].Source)
		edge.Sources[i].Priority = policy.PriorityFor(edge.Sources[i].Source, edge.Sources[i].Priority)
	}
}

func applyPolicyToEdgeDelete(request *EdgeDeleteRequest, policy SourcePolicy) {
	request.Source = strings.TrimSpace(request.Source)
	request.SourceRank = policy.PriorityFor(request.Source, request.SourceRank)
}

func applyPolicyToEntityDelete(request *EntityDeleteRequest, policy SourcePolicy) {
	request.Source = strings.TrimSpace(request.Source)
	request.SourceRank = policy.PriorityFor(request.Source, request.SourceRank)
}

func applyPolicyToSourceStale(request *SourceStaleRequest, policy SourcePolicy) {
	request.Source = strings.TrimSpace(request.Source)
	request.SourceRank = policy.PriorityFor(request.Source, request.SourceRank)
}
