package graph

import "fmt"

func (g *Graph) validateResolvedEntityTarget(
	incomingID string,
	targetID string,
) error {
	if incomingID == "" || incomingID == targetID {
		return nil
	}
	if _, exists := g.Entities[incomingID]; !exists {
		return nil
	}
	return fmt.Errorf(
		"entity %q already exists but its incoming identity resolves to %q; use merge_entities explicitly",
		incomingID,
		targetID,
	)
}
