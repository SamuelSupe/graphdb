package graph

import "fmt"

func (g *Graph) validateIdentityIndexAvailable(entity Entity) error {
	for _, signature := range g.identitySignatures(entity) {
		owner := g.identityIndex[entity.Kind][signature.Value]
		if owner != "" && owner != entity.ID {
			return fmt.Errorf(
				"entity %q duplicates identity %q owned by %q",
				entity.ID,
				signature.Value,
				owner,
			)
		}
	}
	return nil
}
