package graph

import (
	"fmt"
	"strings"
)

func normalizeRelationType(relationType RelationType) (RelationType, error) {
	relationType.Name = strings.TrimSpace(relationType.Name)
	relationType.FromKind = strings.TrimSpace(relationType.FromKind)
	relationType.ToKind = strings.TrimSpace(relationType.ToKind)
	if relationType.Name == "" {
		return RelationType{}, fmt.Errorf("relation type name is required")
	}
	if len(relationType.FromKinds) > 0 {
		trimmed := make([]string, len(relationType.FromKinds))
		for i, k := range relationType.FromKinds {
			trimmed[i] = strings.TrimSpace(k)
		}
		relationType.FromKinds = trimmed
	}
	if len(relationType.ToKinds) > 0 {
		trimmed := make([]string, len(relationType.ToKinds))
		for i, k := range relationType.ToKinds {
			trimmed[i] = strings.TrimSpace(k)
		}
		relationType.ToKinds = trimmed
	}
	if !relationType.AllowCrossKind && relationType.FromKind == "" && relationType.ToKind == "" && len(relationType.FromKinds) == 0 && len(relationType.ToKinds) == 0 {
		return RelationType{}, fmt.Errorf("relation type %q requires from_kind and to_kind", relationType.Name)
	}
	if relationType.Cardinality == "" {
		relationType.Cardinality = ManyToMany
	}
	if relationType.ImpactDirection == "" {
		relationType.ImpactDirection = "none"
	}
	switch relationType.ImpactDirection {
	case "none", "forward", "reverse", "both":
	default:
		return RelationType{}, fmt.Errorf("relation type %q has unsupported impact_direction %q", relationType.Name, relationType.ImpactDirection)
	}
	switch relationType.Cardinality {
	case ManyToMany, OneToMany, ManyToOne, OneToOne:
		return relationType, nil
	default:
		return RelationType{}, fmt.Errorf("relation type %q has unsupported cardinality %q", relationType.Name, relationType.Cardinality)
	}
}
