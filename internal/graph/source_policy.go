package graph

import (
	"fmt"
	"sort"
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
	aliasRules, err := normalizeFieldAliasRules(policy.FieldAliases)
	if err != nil {
		return SourcePolicy{}, err
	}
	policy.FieldAliases = aliasRules
	priorityRules, err := normalizeFieldPriorityRules(policy.FieldPriorities)
	if err != nil {
		return SourcePolicy{}, err
	}
	policy.FieldPriorities = priorityRules
	return policy, nil
}

func normalizeFieldAliasRules(rules []FieldAliasRule) ([]FieldAliasRule, error) {
	seen := map[string]struct{}{}
	out := make([]FieldAliasRule, 0, len(rules))
	for _, rule := range rules {
		rule.Source = strings.TrimSpace(rule.Source)
		rule.Kind = strings.TrimSpace(rule.Kind)
		if rule.Source == "" {
			return nil, fmt.Errorf("source policy field alias source is required")
		}
		aliases := make(map[string]string, len(rule.Aliases))
		for alias, canonical := range rule.Aliases {
			alias = strings.TrimSpace(alias)
			canonical = strings.TrimSpace(canonical)
			if alias == "" || canonical == "" {
				return nil, fmt.Errorf("source policy field alias and canonical field are required")
			}
			if alias == canonical {
				continue
			}
			key := rule.Source + "\x00" + rule.Kind + "\x00" + alias
			if _, ok := seen[key]; ok {
				return nil, fmt.Errorf("duplicate source policy field alias %q for source %q kind %q", alias, rule.Source, rule.Kind)
			}
			seen[key] = struct{}{}
			aliases[alias] = canonical
		}
		if len(aliases) == 0 {
			continue
		}
		rule.Aliases = aliases
		out = append(out, rule)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		return out[i].Kind < out[j].Kind
	})
	return out, nil
}

func normalizeFieldPriorityRules(rules []FieldPriorityRule) ([]FieldPriorityRule, error) {
	seen := map[string]struct{}{}
	out := make([]FieldPriorityRule, 0, len(rules))
	for _, rule := range rules {
		rule.Source = strings.TrimSpace(rule.Source)
		rule.Kind = strings.TrimSpace(rule.Kind)
		if rule.Source == "" {
			return nil, fmt.Errorf("source policy field priority source is required")
		}
		fields := make(map[string]int, len(rule.Fields))
		for field, priority := range rule.Fields {
			field = strings.TrimSpace(field)
			if field == "" {
				return nil, fmt.Errorf("source policy field priority field is required")
			}
			key := rule.Source + "\x00" + rule.Kind + "\x00" + field
			if _, ok := seen[key]; ok {
				return nil, fmt.Errorf("duplicate source policy field priority %q for source %q kind %q", field, rule.Source, rule.Kind)
			}
			seen[key] = struct{}{}
			fields[field] = priority
		}
		if len(fields) == 0 {
			continue
		}
		rule.Fields = fields
		out = append(out, rule)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Source != out[j].Source {
			return out[i].Source < out[j].Source
		}
		return out[i].Kind < out[j].Kind
	})
	return out, nil
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

func ApplySourcePolicy(mutations Mutations, policy SourcePolicy) (Mutations, ApplyReport, error) {
	if len(mutations.UpsertEntities) == 0 && len(mutations.SplitEntities) == 0 && len(mutations.UpsertEdges) == 0 && len(mutations.DeleteEdgeRequests) == 0 &&
		len(mutations.DeleteEntityRequests) == 0 && len(mutations.MarkSourceStale) == 0 {
		return mutations, ApplyReport{}, nil
	}
	var err error
	mutations, err = PrepareEntityFieldWrites(mutations)
	if err != nil {
		return Mutations{}, ApplyReport{}, err
	}
	policy, err = NormalizeSourcePolicy(policy)
	if err != nil {
		return Mutations{}, ApplyReport{}, err
	}
	report := ApplyReport{}
	mutations.UpsertEntities = append([]Entity(nil), mutations.UpsertEntities...)
	for i := range mutations.UpsertEntities {
		mutations.UpsertEntities[i].FieldSources = nil
		applyPolicyToEntity(&mutations.UpsertEntities[i], policy)
		report.Suppressed = append(report.Suppressed, applyFieldAliasesToEntity(&mutations.UpsertEntities[i], policy)...)
		applyFieldPrioritiesToEntity(&mutations.UpsertEntities[i], policy)
	}
	mutations.SplitEntities = append([]SplitRequest(nil), mutations.SplitEntities...)
	for i := range mutations.SplitEntities {
		mutations.SplitEntities[i].Entities = append([]Entity(nil), mutations.SplitEntities[i].Entities...)
		for j := range mutations.SplitEntities[i].Entities {
			mutations.SplitEntities[i].Entities[j].FieldSources = nil
			applyPolicyToEntity(&mutations.SplitEntities[i].Entities[j], policy)
			report.Suppressed = append(report.Suppressed, applyFieldAliasesToEntity(&mutations.SplitEntities[i].Entities[j], policy)...)
			applyFieldPrioritiesToEntity(&mutations.SplitEntities[i].Entities[j], policy)
		}
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
	return mutations, report, nil
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
