package graph

import (
	"sort"
)

func (g *Graph) MatchEntities(kind string, filters Fields) []Entity {
	results := make([]Entity, 0)
	candidates := g.matchCandidates(kind, filters)
	for _, entity := range candidates {
		if kind != "" && entity.Kind != kind {
			continue
		}
		if !fieldsMatch(entity.Fields, filters) {
			continue
		}
		results = append(results, copyEntity(entity))
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID < results[j].ID
	})
	return results
}

func (g *Graph) HasFieldIndex(kind string, field string) bool {
	if kind == "" || field == "" {
		return false
	}
	return g.fieldIndex[kind][field] != nil
}

func (g *Graph) FieldIndexCount(kind string, field string, values []any) int {
	if kind == "" || field == "" || len(values) == 0 {
		return 0
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		key, ok := scalarKey(value)
		if !ok {
			continue
		}
		for id := range g.fieldIndex[kind][field][key] {
			seen[id] = struct{}{}
		}
	}
	return len(seen)
}

func (g *Graph) MatchFieldIndex(kind string, field string, values []any) []Entity {
	ids := map[string]struct{}{}
	for _, value := range values {
		key, ok := scalarKey(value)
		if !ok {
			continue
		}
		for id := range g.fieldIndex[kind][field][key] {
			ids[id] = struct{}{}
		}
	}
	entities := make([]Entity, 0, len(ids))
	for id := range ids {
		if entity, ok := g.Entities[id]; ok {
			entities = append(entities, copyEntity(entity))
		}
	}
	sort.Slice(entities, func(i, j int) bool {
		return entities[i].ID < entities[j].ID
	})
	return entities
}

func (g *Graph) KindCount(kind string) int {
	if kind == "" {
		return len(g.Entities)
	}
	count := 0
	for _, entity := range g.Entities {
		if entity.Kind == kind {
			count++
		}
	}
	return count
}

func (g *Graph) matchCandidates(kind string, filters Fields) []Entity {
	if kind == "" || len(filters) == 0 {
		return g.allEntities()
	}
	var best map[string]struct{}
	for field, value := range filters {
		key, ok := scalarKey(value)
		if !ok {
			continue
		}
		ids := g.fieldIndex[kind][field][key]
		if len(ids) == 0 {
			return nil
		}
		if best == nil || len(ids) < len(best) {
			best = ids
		}
	}
	if best == nil {
		return g.allEntities()
	}
	entities := make([]Entity, 0, len(best))
	for id := range best {
		if entity, ok := g.Entities[id]; ok {
			entities = append(entities, entity)
		}
	}
	return entities
}

func (g *Graph) allEntities() []Entity {
	entities := make([]Entity, 0, len(g.Entities))
	for _, entity := range g.Entities {
		entities = append(entities, entity)
	}
	return entities
}

func (g *Graph) Neighbors(entityID, direction, relationType string) []Neighbor {
	allowed := map[string]struct{}{}
	if relationType != "" {
		allowed[relationType] = struct{}{}
	}
	neighbors, _ := g.FilteredNeighbors(entityID, direction, allowed, nil, false, nil)
	return neighbors
}

func (g *Graph) FilteredNeighbors(entityID, direction string, relationTypes map[string]struct{}, nodeKinds []string, impact bool, charge func() error) ([]Neighbor, error) {
	if direction == "" {
		direction = "both"
	}
	refs := make([]neighborRef, 0)
	if direction == "out" || direction == "both" {
		for edgeID := range g.out[entityID] {
			if charge != nil {
				if err := charge(); err != nil {
					return nil, err
				}
			}
			edge := g.Edges[edgeID]
			if !neighborRelationAllowed(edge.Type, relationTypes) || !neighborImpactAllowed(g, edge.Type, "out", impact) {
				continue
			}
			neighbor, ok := g.Entities[edge.To]
			if !ok {
				continue
			}
			if !neighborKindAllowed(neighbor.Kind, nodeKinds) {
				continue
			}
			refs = append(refs, neighborRef{edgeID: edge.ID, entityID: neighbor.ID, direction: "out"})
		}
	}
	if direction == "in" || direction == "both" {
		for edgeID := range g.in[entityID] {
			if charge != nil {
				if err := charge(); err != nil {
					return nil, err
				}
			}
			edge := g.Edges[edgeID]
			if !neighborRelationAllowed(edge.Type, relationTypes) || !neighborImpactAllowed(g, edge.Type, "in", impact) {
				continue
			}
			neighbor, ok := g.Entities[edge.From]
			if !ok {
				continue
			}
			if !neighborKindAllowed(neighbor.Kind, nodeKinds) {
				continue
			}
			refs = append(refs, neighborRef{edgeID: edge.ID, entityID: neighbor.ID, direction: "in"})
		}
	}
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].edgeID == refs[j].edgeID {
			if refs[i].entityID == refs[j].entityID {
				return refs[i].direction < refs[j].direction
			}
			return refs[i].entityID < refs[j].entityID
		}
		return refs[i].edgeID < refs[j].edgeID
	})
	results := make([]Neighbor, 0, len(refs))
	for _, ref := range refs {
		edge := g.Edges[ref.edgeID]
		entity := g.Entities[ref.entityID]
		results = append(results, Neighbor{Entity: copyEntity(entity), Edge: copyEdge(edge), Direction: ref.direction})
	}
	return results, nil
}

type neighborRef struct {
	edgeID    string
	entityID  string
	direction string
}

func fieldsMatch(fields Fields, filters Fields) bool {
	for key, expected := range filters {
		actual, ok := fields[key]
		if !ok || !fieldValuesEqual(actual, expected) {
			return false
		}
	}
	return true
}
