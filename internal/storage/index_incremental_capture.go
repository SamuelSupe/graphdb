package storage

import (
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func captureIndexBefore(g *graph.Graph, mutations graph.Mutations) *graph.Graph {
	before := graph.New()
	before.CITypes = g.CITypes
	before.RelationTypes = g.RelationTypes
	before.Entities = map[string]graph.Entity{}
	before.Edges = map[string]graph.Edge{}
	for _, entity := range mutations.UpsertEntities {
		if previous, ok := g.Entities[entity.ID]; ok {
			before.Entities[entity.ID] = previous
		}
	}
	for _, entityID := range mutations.DeleteEntities {
		if previous, ok := g.Entities[entityID]; ok {
			before.Entities[entityID] = previous
		}
		for edgeID, edge := range g.Edges {
			if edge.From == entityID || edge.To == entityID {
				before.Edges[edgeID] = edge
			}
		}
	}
	for _, edge := range mutations.UpsertEdges {
		edgeID := graph.CanonicalEdgeID(edge)
		if previous, ok := g.Edges[edgeID]; ok {
			before.Edges[edgeID] = previous
		}
	}
	for _, edgeID := range mutations.DeleteEdges {
		if resolved := resolveEdgeID(g, edgeID); resolved != "" {
			before.Edges[resolved] = g.Edges[resolved]
		}
	}
	for _, request := range mutations.DeleteEdgeRequests {
		if resolved := resolveDeleteEdgeRequestID(g, request); resolved != "" {
			before.Edges[resolved] = g.Edges[resolved]
		}
	}
	return before
}

func resolveDeleteEdgeRequestID(g *graph.Graph, request graph.EdgeDeleteRequest) string {
	request.Type = strings.TrimSpace(request.Type)
	request.From = strings.TrimSpace(request.From)
	request.To = strings.TrimSpace(request.To)
	request.ID = strings.TrimSpace(request.ID)
	if request.Type != "" || request.From != "" || request.To != "" {
		return graph.CanonicalEdgeIDParts(request.Type, request.From, request.To)
	}
	return resolveEdgeID(g, request.ID)
}

func resolveEdgeID(g *graph.Graph, id string) string {
	id = strings.TrimSpace(id)
	if id == "" {
		return ""
	}
	if _, ok := g.Edges[id]; ok {
		return id
	}
	for edgeID, edge := range g.Edges {
		if graph.EdgeSourceAliasMatches(edge, id) {
			return edgeID
		}
	}
	return ""
}

func deletedEntityEdgeIDs(before *graph.Graph, entityIDs []string) []string {
	deletedEntities := map[string]struct{}{}
	for _, entityID := range entityIDs {
		deletedEntities[entityID] = struct{}{}
	}
	edgeIDs := make([]string, 0)
	for edgeID, edge := range before.Edges {
		if _, ok := deletedEntities[edge.From]; ok {
			edgeIDs = append(edgeIDs, edgeID)
			continue
		}
		if _, ok := deletedEntities[edge.To]; ok {
			edgeIDs = append(edgeIDs, edgeID)
		}
	}
	return edgeIDs
}
