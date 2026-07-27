package graph

import "fmt"

func (g *Graph) applyCITypeMutations(
	deletes []string,
	upserts []CIType,
	tracker *mutationFingerprintTracker,
) error {
	if len(deletes) == 0 && len(upserts) == 0 {
		return nil
	}
	usedKinds := entityKindSet(g.Entities)
	changedKinds := make(map[string]struct{}, len(deletes)+len(upserts))
	for _, name := range deletes {
		if name == "" {
			return fmt.Errorf("delete ci type name is required")
		}
		if _, used := usedKinds[name]; used {
			return fmt.Errorf(
				"cannot delete ci type %q while entities of that kind exist",
				name,
			)
		}
		tracker.touchCIType(name)
		delete(g.CITypes, name)
		changedKinds[name] = struct{}{}
	}
	for _, ciType := range upserts {
		normalized, err := normalizeCIType(ciType)
		if err != nil {
			return err
		}
		tracker.touchCIType(normalized.Name)
		g.CITypes[normalized.Name] = normalized
		changedKinds[normalized.Name] = struct{}{}
	}
	if err := g.validateCITypes(); err != nil {
		return err
	}
	affectedKinds := g.affectedCITypeKinds(changedKinds)
	affectedUsedKinds := intersectStringSets(affectedKinds, usedKinds)
	if len(affectedUsedKinds) > 0 {
		if err := g.validateEntitiesAgainstCITypesForKinds(affectedUsedKinds); err != nil {
			return err
		}
	}
	g.rebuildIdentityIndexesForKinds(
		intersectStringSets(changedKinds, usedKinds),
	)
	return nil
}

func entityKindSet(entities map[string]Entity) map[string]struct{} {
	kinds := make(map[string]struct{})
	for _, entity := range entities {
		kinds[entity.Kind] = struct{}{}
	}
	return kinds
}

func (g *Graph) affectedCITypeKinds(
	changed map[string]struct{},
) map[string]struct{} {
	affected := copyStringSet(changed)
	children := make(map[string][]string)
	for name, ciType := range g.CITypes {
		for _, parent := range ciType.Extends {
			children[parent] = append(children[parent], name)
		}
	}
	queue := make([]string, 0, len(affected))
	for name := range affected {
		queue = append(queue, name)
	}
	for len(queue) > 0 {
		parent := queue[0]
		queue = queue[1:]
		for _, child := range children[parent] {
			if _, seen := affected[child]; seen {
				continue
			}
			affected[child] = struct{}{}
			queue = append(queue, child)
		}
	}
	return affected
}

func intersectStringSets(
	left map[string]struct{},
	right map[string]struct{},
) map[string]struct{} {
	intersection := make(map[string]struct{})
	for value := range left {
		if _, ok := right[value]; ok {
			intersection[value] = struct{}{}
		}
	}
	return intersection
}

func (g *Graph) rebuildIdentityIndexesForKinds(
	kinds map[string]struct{},
) {
	if len(kinds) == 0 {
		return
	}
	next := make(map[string]map[string]string, len(kinds))
	for kind := range kinds {
		next[kind] = map[string]string{}
	}
	for id, entity := range g.Entities {
		identities, affected := next[entity.Kind]
		if !affected {
			continue
		}
		for _, signature := range g.identitySignatures(entity) {
			identities[signature.Value] = id
		}
	}
	for kind, identities := range next {
		g.identityIndex[kind] = identities
		if g.cow != nil {
			g.cow.identityKinds[kind] = struct{}{}
		}
	}
}
