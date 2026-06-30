package graph

import (
	"fmt"
	"strings"
)

func (g *Graph) resolveEdgeDeleteRequest(request EdgeDeleteRequest) (string, error) {
	request.ID = strings.TrimSpace(request.ID)
	request.Type = strings.TrimSpace(request.Type)
	request.From = strings.TrimSpace(request.From)
	request.To = strings.TrimSpace(request.To)
	if request.Type != "" || request.From != "" || request.To != "" {
		if request.Type == "" || request.From == "" || request.To == "" {
			return "", fmt.Errorf("delete edge request requires type, from and to when using relation identity")
		}
		return CanonicalEdgeIDParts(request.Type, request.From, request.To), nil
	}
	if request.ID == "" {
		return "", fmt.Errorf("delete edge request requires id or type/from/to")
	}
	return g.resolveEdgeReference(request.ID)
}

func (g *Graph) resolveEdgeReference(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", nil
	}
	if _, ok := g.Edges[id]; ok {
		return id, nil
	}
	resolved := ""
	for edgeID, edge := range g.Edges {
		if edgeSourceAliasMatches(edge, id) {
			if resolved != "" && resolved != edgeID {
				return "", fmt.Errorf("edge reference %q is ambiguous; use canonical id or type/from/to", id)
			}
			resolved = edgeID
		}
	}
	return resolved, nil
}
