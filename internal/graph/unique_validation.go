package graph

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type uniqueFieldRef struct {
	kind  string
	field string
}

// uniqueEntityValidator keeps a commit-local index for non-scalar unique
// fields. JSON is only a candidate bucket: deep equality still decides
// uniqueness, so different Go values with the same JSON representation retain
// the existing behavior.
type uniqueEntityValidator struct {
	graph         *Graph
	fieldsByKind  map[string][]string
	effective     map[string]map[string]FieldSpec
	complexKinds  map[string]struct{}
	complexValues map[uniqueFieldRef]map[string]map[string]struct{}
}

func newUniqueEntityValidator(
	g *Graph,
	kinds map[string]struct{},
) (*uniqueEntityValidator, error) {
	validator := &uniqueEntityValidator{
		graph:         g,
		fieldsByKind:  make(map[string][]string, len(kinds)),
		effective:     make(map[string]map[string]FieldSpec),
		complexKinds:  make(map[string]struct{}),
		complexValues: make(map[uniqueFieldRef]map[string]map[string]struct{}),
	}
	for kind := range kinds {
		if err := validator.prepareKind(kind); err != nil {
			return nil, err
		}
	}
	if len(validator.complexKinds) == 0 {
		return validator, nil
	}
	for id, entity := range g.Entities {
		if _, ok := validator.complexKinds[entity.Kind]; !ok {
			continue
		}
		validator.addComplexValues(id, entity)
	}
	return validator, nil
}

func (v *uniqueEntityValidator) prepareKind(kind string) error {
	if _, prepared := v.fieldsByKind[kind]; prepared {
		return nil
	}
	specs, err := v.graph.effectiveFieldsCached(
		kind,
		v.effective,
	)
	if err != nil {
		return err
	}
	fields := make([]string, 0, len(specs))
	for field, spec := range specs {
		if !spec.Unique {
			continue
		}
		fields = append(fields, field)
		if spec.Type == "object" || spec.Type == "array" || spec.Type == "any" {
			v.complexKinds[kind] = struct{}{}
		}
	}
	sort.Strings(fields)
	v.fieldsByKind[kind] = fields
	return nil
}

func (v *uniqueEntityValidator) ensureKind(kind string) error {
	if _, prepared := v.fieldsByKind[kind]; prepared {
		return nil
	}
	if err := v.prepareKind(kind); err != nil {
		return err
	}
	if _, complex := v.complexKinds[kind]; !complex {
		return nil
	}
	for id, entity := range v.graph.Entities {
		if entity.Kind == kind {
			v.addComplexValues(id, entity)
		}
	}
	return nil
}

func (v *uniqueEntityValidator) validate(entity Entity) error {
	if err := v.ensureKind(entity.Kind); err != nil {
		return err
	}
	for _, field := range v.fieldsByKind[entity.Kind] {
		value, ok := entity.Fields[field]
		if !ok {
			continue
		}
		if key, scalar := scalarKey(value); scalar {
			for id := range v.graph.fieldIndex[entity.Kind][field][key] {
				if id != entity.ID {
					return uniqueFieldError(entity, field)
				}
			}
			continue
		}
		key, indexed := complexUniqueKey(value)
		if !indexed {
			if v.conflictsWithAnyEntity(entity, field, value) {
				return uniqueFieldError(entity, field)
			}
			continue
		}
		ref := uniqueFieldRef{kind: entity.Kind, field: field}
		for id := range v.complexValues[ref][key] {
			if id == entity.ID {
				continue
			}
			existing, exists := v.graph.Entities[id]
			if !exists {
				continue
			}
			existingValue, exists := existing.Fields[field]
			if exists && fieldValuesEqual(existingValue, value) {
				return uniqueFieldError(entity, field)
			}
		}
	}
	return nil
}

func (v *uniqueEntityValidator) add(entity Entity) {
	v.addComplexValues(entity.ID, entity)
}

func (v *uniqueEntityValidator) addComplexValues(id string, entity Entity) {
	for _, field := range v.fieldsByKind[entity.Kind] {
		value, ok := entity.Fields[field]
		if !ok {
			continue
		}
		if _, scalar := scalarKey(value); scalar {
			continue
		}
		key, ok := complexUniqueKey(value)
		if !ok {
			continue
		}
		ref := uniqueFieldRef{kind: entity.Kind, field: field}
		byValue := v.complexValues[ref]
		if byValue == nil {
			byValue = map[string]map[string]struct{}{}
			v.complexValues[ref] = byValue
		}
		ids := byValue[key]
		if ids == nil {
			ids = map[string]struct{}{}
			byValue[key] = ids
		}
		ids[id] = struct{}{}
	}
}

func (v *uniqueEntityValidator) conflictsWithAnyEntity(
	entity Entity,
	field string,
	value any,
) bool {
	for id, existing := range v.graph.Entities {
		if id == entity.ID || existing.Kind != entity.Kind {
			continue
		}
		existingValue, exists := existing.Fields[field]
		if exists && fieldValuesEqual(existingValue, value) {
			return true
		}
	}
	return false
}

func complexUniqueKey(value any) (string, bool) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return string(data), true
}

func uniqueFieldError(entity Entity, field string) error {
	return fmt.Errorf(
		"entity %q violates unique field %q for kind %q",
		entity.ID,
		field,
		entity.Kind,
	)
}

func entityMutationKinds(g *Graph, mutations Mutations) map[string]struct{} {
	kinds := make(map[string]struct{})
	for _, entity := range mutations.UpsertEntities {
		if kind := strings.TrimSpace(entity.Kind); kind != "" {
			kinds[kind] = struct{}{}
		}
	}
	for _, request := range mutations.MergeEntities {
		if entity, ok := g.Entities[request.TargetID]; ok {
			kinds[entity.Kind] = struct{}{}
		}
	}
	for _, request := range mutations.SplitEntities {
		if entity, ok := g.Entities[request.SourceID]; ok {
			kinds[entity.Kind] = struct{}{}
		}
		for _, entity := range request.Entities {
			if kind := strings.TrimSpace(entity.Kind); kind != "" {
				kinds[kind] = struct{}{}
			}
		}
	}
	return kinds
}

func (g *Graph) validateUniqueEntity(entity Entity) error {
	validator, err := newUniqueEntityValidator(
		g,
		map[string]struct{}{entity.Kind: {}},
	)
	if err != nil {
		return err
	}
	return validator.validate(entity)
}
