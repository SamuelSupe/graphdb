package partition

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func equalJSON(a, b any) bool {
	left, _ := json.Marshal(a)
	right, _ := json.Marshal(b)
	return string(left) == string(right)
}

type directoryEdit struct {
	bucket uint8
	key    string
	before map[string]bool
	after  map[string]bool
}
type changeBuilder struct {
	ctx         context.Context
	view        View
	changes     ChangeSet
	directories map[string]*directoryEdit
}

func (b *changeBuilder) directory(key, owner string, add bool) error {
	edit := b.directories[key]
	if edit == nil {
		var owners []string
		if _, err := b.view.Get(b.ctx, Bucket(key), key, &owners); err != nil {
			return err
		}
		edit = &directoryEdit{bucket: Bucket(key), key: key, before: map[string]bool{}, after: map[string]bool{}}
		for _, id := range owners {
			edit.before[id] = true
			edit.after[id] = true
		}
		b.directories[key] = edit
	}
	if add {
		edit.after[owner] = true
	} else {
		delete(edit.after, owner)
	}
	return nil
}

func (b *changeBuilder) entity(g *graph.Graph, entity graph.Entity, add bool) error {
	var value any
	if add {
		value = entity
	}
	if err := b.changes.Set(Bucket(entity.ID), EntityKey(entity.ID), value); err != nil {
		return err
	}
	var header any
	if add {
		header = entityScanEntry{ID: entity.ID, Kind: entity.Kind}
	}
	if err := b.changes.Set(Bucket(entity.ID), Key("entity_scan", entity.ID), header); err != nil {
		return err
	}
	if err := b.changes.Logical(1, entity.ID, value); err != nil {
		return err
	}
	_, refs, err := g.PartitionEntityReferences(entity, false)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		if err := b.directory(ConstraintKey(ref), entity.ID, add); err != nil {
			return err
		}
	}
	for _, alias := range graph.PartitionEntityAliases(entity) {
		if err := b.directory(EntityAliasKey(alias), entity.ID, add); err != nil {
			return err
		}
	}
	return nil
}

func (b *changeBuilder) edge(g *graph.Graph, edge graph.Edge, add bool) error {
	loc := edgeLocation{ID: edge.ID, Partition: Bucket(edge.From)}
	var value, reference any
	if add {
		value = edge
		reference = loc
	}
	if err := b.changes.Set(loc.Partition, EdgeKey(edge.ID), value); err != nil {
		return err
	}
	for _, row := range []struct {
		bucket uint8
		key    string
	}{{Bucket(edge.From), AdjacencyKey(edge.From, "out", edge.ID)}, {Bucket(edge.To), AdjacencyKey(edge.To, "in", edge.ID)}, {Bucket(EdgeLocatorKey(edge.ID)), EdgeLocatorKey(edge.ID)}} {
		if err := b.changes.Set(row.bucket, row.key, reference); err != nil {
			return err
		}
	}
	if err := b.changes.Logical(3, edge.ID, value); err != nil {
		return err
	}
	for _, alias := range graph.PartitionEdgeAliases(edge) {
		if err := b.directory(EdgeAliasKey(alias), edge.ID, add); err != nil {
			return err
		}
	}
	for _, key := range cardinalityKeys(edge, g.RelationTypes) {
		if err := b.directory(key, edge.ID, add); err != nil {
			return err
		}
	}
	return nil
}

func buildChanges(ctx context.Context, view View, before, after *graph.Graph, report graph.ApplyReport) (Plan, error) {
	b := &changeBuilder{ctx: ctx, view: view, changes: ChangeSet{}, directories: map[string]*directoryEdit{}}
	plan := Plan{Changes: b.changes, CITypes: after.ListCITypes(), RelationTypes: after.ListRelationTypes(), Report: report, EntityCount: view.Graph.EntityCount, EdgeCount: view.Graph.EdgeCount}
	for id, entity := range before.Entities.All() {
		next, ok := after.Entities.Get(id)
		if !ok || !equalJSON(entity, next) {
			if err := b.entity(before, entity, false); err != nil {
				return Plan{}, err
			}
			if !ok {
				plan.EntityCount--
			}
		}
	}
	for id, entity := range after.Entities.All() {
		old, ok := before.Entities.Get(id)
		if !ok || !equalJSON(entity, old) {
			if err := b.entity(after, entity, true); err != nil {
				return Plan{}, err
			}
			if !ok {
				plan.EntityCount++
			}
		}
	}
	for id, edge := range before.Edges.All() {
		next, ok := after.Edges.Get(id)
		if !ok || !equalJSON(edge, next) {
			if err := b.edge(before, edge, false); err != nil {
				return Plan{}, err
			}
			if !ok {
				plan.EdgeCount--
			}
		}
	}
	for id, edge := range after.Edges.All() {
		old, ok := before.Edges.Get(id)
		if !ok || !equalJSON(edge, old) {
			if err := b.edge(after, edge, true); err != nil {
				return Plan{}, err
			}
			if !ok {
				plan.EdgeCount++
			}
		}
	}
	for _, edit := range b.directories {
		if len(edit.after) > 1 && (strings.HasPrefix(edit.key, Key("constraint")) || strings.HasPrefix(edit.key, Key("cardinality"))) {
			return Plan{}, invalid("mutation violates a global identity, unique or cardinality constraint")
		}

		if equalJSON(edit.before, edit.after) {
			continue
		}
		ids := make([]string, 0, len(edit.after))
		for id := range edit.after {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		var value any
		if len(ids) > 0 {
			value = ids
		}
		if err := b.changes.Set(edit.bucket, edit.key, value); err != nil {
			return Plan{}, err
		}
	}
	oldTypes := map[string]graph.CIType{}
	for _, v := range view.Graph.CITypes {
		oldTypes[v.Name] = v
	}
	for name, old := range oldTypes {
		if _, ok := after.CITypes[name]; !ok {
			if err := b.changes.Logical(0, old.Name, nil); err != nil {
				return Plan{}, err
			}
		}
	}
	for name, next := range after.CITypes {
		old, ok := oldTypes[name]
		if !ok || !equalJSON(old, next) {
			if err := b.changes.Logical(0, name, next); err != nil {
				return Plan{}, err
			}
		}
	}
	oldRelations := map[string]graph.RelationType{}
	for _, v := range view.Graph.RelationTypes {
		oldRelations[v.Name] = v
	}
	for name := range oldRelations {
		if _, ok := after.RelationTypes[name]; !ok {
			if err := b.changes.Logical(2, name, nil); err != nil {
				return Plan{}, err
			}
		}
	}
	for name, next := range after.RelationTypes {
		old, ok := oldRelations[name]
		if !ok || !equalJSON(old, next) {
			if err := b.changes.Logical(2, name, next); err != nil {
				return Plan{}, err
			}
		}
	}
	for id := range plan.Changes {
		if len(plan.Changes[id]) > MaxChanges {
			return Plan{}, invalid("partition mutation count exceeds its transaction budget")
		}
	}
	return plan, nil
}
