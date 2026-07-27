package graph

import "fmt"

func (g *Graph) validateAllEdges() error {
	for _, edge := range g.Edges {
		if err := g.validateEdge(edge); err != nil {
			return err
		}
	}
	return g.validateAllCardinalities()
}

func (g *Graph) validateRelationTypeEdges(
	relationTypes map[string]struct{},
) error {
	for relationType := range relationTypes {
		for edgeID := range g.edgeTypeIndex[relationType] {
			edge, ok := g.Edges[edgeID]
			if !ok {
				continue
			}
			if err := g.validateEdge(edge); err != nil {
				return err
			}
			if err := g.validateCardinality(edge); err != nil {
				return err
			}
		}
	}
	return nil
}

type cardinalityEndpoint struct {
	entityID     string
	relationType string
}

func (g *Graph) validateAllCardinalities() error {
	outgoing := make(map[cardinalityEndpoint]struct{})
	incoming := make(map[cardinalityEndpoint]struct{})
	for _, edge := range g.Edges {
		relationType := g.RelationTypes[edge.Type]
		outKey := cardinalityEndpoint{entityID: edge.From, relationType: edge.Type}
		inKey := cardinalityEndpoint{entityID: edge.To, relationType: edge.Type}
		switch relationType.Cardinality {
		case "", ManyToMany:
			continue
		case OneToOne:
			if _, exists := outgoing[outKey]; exists {
				return fmt.Errorf("edge %q violates one_to_one cardinality for relation %q", edge.ID, edge.Type)
			}
			if _, exists := incoming[inKey]; exists {
				return fmt.Errorf("edge %q violates one_to_one cardinality for relation %q", edge.ID, edge.Type)
			}
			outgoing[outKey] = struct{}{}
			incoming[inKey] = struct{}{}
		case OneToMany:
			if _, exists := incoming[inKey]; exists {
				return fmt.Errorf("edge %q violates one_to_many cardinality for relation %q", edge.ID, edge.Type)
			}
			incoming[inKey] = struct{}{}
		case ManyToOne:
			if _, exists := outgoing[outKey]; exists {
				return fmt.Errorf("edge %q violates many_to_one cardinality for relation %q", edge.ID, edge.Type)
			}
			outgoing[outKey] = struct{}{}
		}
	}
	return nil
}

func (g *Graph) validateEdge(edge Edge) error {
	relationType, ok := g.RelationTypes[edge.Type]
	if !ok {
		return fmt.Errorf("edge %q references missing relation type %q", edge.ID, edge.Type)
	}
	from, ok := g.Entities[edge.From]
	if !ok {
		return fmt.Errorf("edge %q references missing from entity %q", edge.ID, edge.From)
	}
	to, ok := g.Entities[edge.To]
	if !ok {
		return fmt.Errorf("edge %q references missing to entity %q", edge.ID, edge.To)
	}
	if !kindAllowed(from.Kind, relationType.FromKind, relationType.FromKinds, relationType.AllowCrossKind) {
		return fmt.Errorf("edge %q from entity kind %q is not allowed for relation %q", edge.ID, from.Kind, edge.Type)
	}
	if !kindAllowed(to.Kind, relationType.ToKind, relationType.ToKinds, relationType.AllowCrossKind) {
		return fmt.Errorf("edge %q to entity kind %q is not allowed for relation %q", edge.ID, to.Kind, edge.Type)
	}
	return nil
}

func (g *Graph) validateCardinality(edge Edge) error {
	relationType := g.RelationTypes[edge.Type]
	switch relationType.Cardinality {
	case "", ManyToMany:
		return nil
	case OneToOne:
		if g.hasCardinalityConflict(edge, g.out[edge.From]) || g.hasCardinalityConflict(edge, g.in[edge.To]) {
			return fmt.Errorf("edge %q violates one_to_one cardinality for relation %q", edge.ID, edge.Type)
		}
	case OneToMany:
		if g.hasCardinalityConflict(edge, g.in[edge.To]) {
			return fmt.Errorf("edge %q violates one_to_many cardinality for relation %q", edge.ID, edge.Type)
		}
	case ManyToOne:
		if g.hasCardinalityConflict(edge, g.out[edge.From]) {
			return fmt.Errorf("edge %q violates many_to_one cardinality for relation %q", edge.ID, edge.Type)
		}
	}
	return nil
}

func (g *Graph) hasCardinalityConflict(edge Edge, edgeIDs map[string]struct{}) bool {
	for edgeID := range edgeIDs {
		existing := g.Edges[edgeID]
		if existing.ID != edge.ID && existing.Type == edge.Type {
			return true
		}
	}
	return false
}

func kindAllowed(kind string, legacy string, allowed []string, allowCrossKind bool) bool {
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
