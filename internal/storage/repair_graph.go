package storage

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"graphdb/internal/graph"
)

func graphConsistencyIssues(g *graph.Graph) []RepairIssue {
	issues := make([]RepairIssue, 0)
	issues = append(issues, aliasConflictIssues(g)...)
	issues = append(issues, duplicateCIIdentityIssues(g)...)
	issues = append(issues, edgeEndpointIssues(g)...)
	issues = append(issues, staleSourceIssues(g)...)
	return issues
}

func aliasConflictIssues(g *graph.Graph) []RepairIssue {
	owners := map[string][]string{}
	for id, entity := range g.Entities {
		for _, alias := range entity.MergedFrom {
			if alias == "" {
				continue
			}
			owners[alias] = append(owners[alias], id)
		}
	}
	issues := make([]RepairIssue, 0)
	for alias, ids := range owners {
		sort.Strings(ids)
		if len(ids) > 1 {
			issues = append(issues, RepairIssue{
				Code:         "alias_conflict",
				Severity:     "error",
				ResourceType: "entity_alias",
				ResourceID:   alias,
				Message:      "entity alias is owned by multiple canonical entities",
				Repairable:   false,
				Details:      map[string]any{"owners": ids},
			})
			continue
		}
		if _, active := g.Entities[alias]; active && ids[0] != alias {
			issues = append(issues, RepairIssue{
				Code:         "alias_points_to_active_entity",
				Severity:     "error",
				ResourceType: "entity_alias",
				ResourceID:   alias,
				Message:      "entity alias also exists as an active entity id",
				Repairable:   false,
				Details:      map[string]any{"owner": ids[0]},
			})
		}
	}
	return issues
}

func duplicateCIIdentityIssues(g *graph.Graph) []RepairIssue {
	owners := map[string][]string{}
	for id, entity := range g.Entities {
		ciType := g.CITypes[entity.Kind]
		for _, key := range ciType.IdentityKeys {
			signature, ok := repairIdentitySignature(entity, key)
			if !ok {
				continue
			}
			owners[entity.Kind+"\x00"+key.Name+"\x00"+signature] = append(owners[entity.Kind+"\x00"+key.Name+"\x00"+signature], id)
		}
	}
	issues := make([]RepairIssue, 0)
	for signature, ids := range owners {
		if len(ids) < 2 {
			continue
		}
		sort.Strings(ids)
		parts := strings.SplitN(signature, "\x00", 3)
		issues = append(issues, RepairIssue{
			Code:         "duplicate_ci_identity",
			Severity:     "error",
			ResourceType: "ci_identity",
			ResourceID:   strings.ReplaceAll(signature, "\x00", "|"),
			Message:      "multiple entities share the same CI identity",
			Repairable:   false,
			Details:      map[string]any{"kind": parts[0], "identity_key": parts[1], "entities": ids},
		})
	}
	return issues
}

func repairIdentitySignature(entity graph.Entity, key graph.IdentityKey) (string, bool) {
	parts := make([]string, 0, len(key.Fields))
	for _, field := range key.Fields {
		value, ok := entity.Identity[field]
		if !ok {
			value, ok = entity.Fields[field]
		}
		if !ok {
			return "", false
		}
		scalar, ok := repairScalarKey(value)
		if !ok {
			return "", false
		}
		parts = append(parts, field+"="+scalar)
	}
	if len(parts) == 0 {
		return "", false
	}
	return strings.Join(parts, "|"), true
}

func repairScalarKey(value any) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "null", true
	case string:
		return "s:" + typed, true
	case bool:
		if typed {
			return "b:true", true
		}
		return "b:false", true
	case float64:
		return fmt.Sprintf("n:%g", typed), true
	case int:
		return fmt.Sprintf("n:%d", typed), true
	case int64:
		return fmt.Sprintf("n:%d", typed), true
	case json.Number:
		return "n:" + typed.String(), true
	default:
		return "", false
	}
}

func edgeEndpointIssues(g *graph.Graph) []RepairIssue {
	issues := make([]RepairIssue, 0)
	for _, edge := range g.Edges {
		relationType, ok := g.RelationTypes[edge.Type]
		if !ok {
			issues = append(issues, edgeIssue("missing_relation_type", edge, "edge references a missing relation type"))
			continue
		}
		from, ok := g.Entities[edge.From]
		if !ok {
			issues = append(issues, edgeIssue("orphan_edge_from", edge, "edge references a missing from entity"))
			continue
		}
		to, ok := g.Entities[edge.To]
		if !ok {
			issues = append(issues, edgeIssue("orphan_edge_to", edge, "edge references a missing to entity"))
			continue
		}
		if !repairKindAllowed(from.Kind, relationType.FromKind, relationType.FromKinds, relationType.AllowCrossKind) ||
			!repairKindAllowed(to.Kind, relationType.ToKind, relationType.ToKinds, relationType.AllowCrossKind) {
			issues = append(issues, edgeIssue("relation_endpoint_mismatch", edge, "edge endpoint kind does not match relation definition"))
		}
	}
	return issues
}

func edgeIssue(code string, edge graph.Edge, message string) RepairIssue {
	return RepairIssue{
		Code:         code,
		Severity:     "error",
		ResourceType: "edge",
		ResourceID:   edge.ID,
		Message:      message,
		Repairable:   false,
		Details:      map[string]any{"type": edge.Type, "from": edge.From, "to": edge.To},
	}
}

func repairKindAllowed(kind string, legacy string, allowed []string, allowCrossKind bool) bool {
	if len(allowed) == 0 && legacy == "" {
		return allowCrossKind
	}
	if legacy != "" && kind == legacy {
		return true
	}
	for _, candidate := range allowed {
		if candidate == kind {
			return true
		}
	}
	return false
}

func staleSourceIssues(g *graph.Graph) []RepairIssue {
	issues := make([]RepairIssue, 0)
	for id, entity := range g.Entities {
		stale := staleEntitySources(entity)
		if len(stale) == 0 {
			continue
		}
		if entity.ExistenceSource != nil {
			if _, ok := stale[entity.ExistenceSource.Source]; ok {
				issues = append(issues, RepairIssue{
					Code:         "stale_source_owns_entity_existence",
					Severity:     "warn",
					ResourceType: "entity",
					ResourceID:   id,
					Message:      "stale source still owns entity existence",
					Repairable:   false,
					Details:      map[string]any{"source": entity.ExistenceSource.Source},
				})
			}
		}
		for field, owner := range entity.FieldSources {
			if _, ok := stale[owner.Source]; !ok {
				continue
			}
			issues = append(issues, RepairIssue{
				Code:         "stale_source_owns_entity_field",
				Severity:     "warn",
				ResourceType: "entity_field",
				ResourceID:   id + "." + field,
				Message:      "stale source still owns an entity field",
				Repairable:   false,
				Details:      map[string]any{"source": owner.Source, "field": field},
			})
		}
	}
	return issues
}

func staleEntitySources(entity graph.Entity) map[string]struct{} {
	out := map[string]struct{}{}
	for _, source := range entity.Sources {
		if source.Stale && source.Source != "" {
			out[source.Source] = struct{}{}
		}
	}
	return out
}
