package graph

import (
	"fmt"
	"time"
)

func (g *Graph) redirectMergeEdges(
	sourceID string,
	targetID string,
	version int64,
	now time.Time,
	tracker *mutationFingerprintTracker,
	reportAffected *uniqueStringCollector,
	affectedMergeEdges map[string]struct{},
) error {
	edgeIDs := g.incidentEdgeIDs(sourceID)
	for _, edgeID := range edgeIDs {
		edge, ok := g.Edges.Get(edgeID)
		if !ok {
			continue
		}
		redirected := copyEdge(edge)
		if redirected.From == sourceID {
			redirected.From = targetID
		}
		if redirected.To == sourceID {
			redirected.To = targetID
		}
		nextID := CanonicalEdgeID(redirected)
		tracker.touchEdge(nextID)
		reportAffected.add(edgeID, nextID)

		g.removeEdgeFromIndexes(edgeID, edge)
		g.Edges.Delete(edgeID)
		candidates := []Edge{redirected}
		if existing, exists := g.Edges.Get(nextID); exists {
			g.removeEdgeFromIndexes(nextID, existing)
			g.Edges.Delete(nextID)
			candidates = append(candidates, existing)
		}
		merged, _ := mergeEdgeList(candidates, version, now)
		next, ok := merged.Get(nextID)
		if !ok {
			return fmt.Errorf("merge edge %q did not produce canonical edge %q", edgeID, nextID)
		}
		g.Edges.Set(nextID, next)
		g.addEdgeToIndexes(nextID, next)
		affectedMergeEdges[nextID] = struct{}{}
	}
	return nil
}

func (g *Graph) validateAffectedMergeEdges(affected map[string]struct{}) error {
	for edgeID := range affected {
		edge, ok := g.Edges.Get(edgeID)
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
	return nil
}
