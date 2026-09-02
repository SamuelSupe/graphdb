package graph

import (
	"fmt"
)

func (g *Graph) applyEntitySchemaWithSpecs(
	entity *Entity,
	specs map[string]FieldSpec,
) error {
	for name, spec := range specs {
		value, exists := entity.Fields[name]
		if !exists && spec.Default != nil {
			value = copyAny(spec.Default)
			entity.Fields[name] = value
			exists = true
		}
		if spec.Required && !exists {
			return fmt.Errorf("entity %q kind %q missing required field %q", entity.ID, entity.Kind, name)
		}
		if !exists {
			continue
		}
		if !valueMatchesType(value, spec.Type) {
			return fmt.Errorf("entity %q field %q does not match type %q", entity.ID, name, spec.Type)
		}
		if len(spec.Enum) > 0 && !valueInEnum(value, spec.Enum) {
			return fmt.Errorf("entity %q field %q value is not in enum", entity.ID, name)
		}
	}
	for name, value := range entity.Identity {
		if value == nil {
			delete(entity.Identity, name)
		}
	}
	g.populateIdentity(entity)
	return nil
}

func (g *Graph) effectiveFieldsCached(
	kind string,
	cache map[string]map[string]FieldSpec,
) (map[string]FieldSpec, error) {
	if fields, ok := cache[kind]; ok {
		return fields, nil
	}
	return g.effectiveFields(
		kind,
		map[string]struct{}{},
		cache,
	)
}

func (g *Graph) effectiveFields(
	kind string,
	seen map[string]struct{},
	cache map[string]map[string]FieldSpec,
) (map[string]FieldSpec, error) {
	if fields, ok := cache[kind]; ok {
		return fields, nil
	}
	ciType, ok := g.CITypes[kind]
	if !ok {
		fields := map[string]FieldSpec{}
		cache[kind] = fields
		return fields, nil
	}
	if _, loop := seen[kind]; loop {
		return nil, fmt.Errorf("ci type %q has inheritance cycle", kind)
	}
	seen[kind] = struct{}{}
	defer delete(seen, kind)
	fields := map[string]FieldSpec{}
	for _, parent := range ciType.Extends {
		if _, ok := g.CITypes[parent]; !ok {
			return nil, fmt.Errorf("ci type %q extends missing parent %q", kind, parent)
		}
		parentFields, err := g.effectiveFields(parent, seen, cache)
		if err != nil {
			return nil, err
		}
		for name, spec := range parentFields {
			fields[name] = spec
		}
	}
	for name, spec := range ciType.Fields {
		fields[name] = spec
	}
	cache[kind] = fields
	return fields, nil
}

func (g *Graph) EffectiveFields(kind string) (map[string]FieldSpec, error) {
	return g.effectiveFields(
		kind,
		map[string]struct{}{},
		map[string]map[string]FieldSpec{},
	)
}

func (g *Graph) validateCITypes() error {
	children := make(map[string][]string, len(g.CITypes))
	parentCounts := make(map[string]int, len(g.CITypes))
	queue := make([]string, 0, len(g.CITypes))
	for kind, ciType := range g.CITypes {
		parentCounts[kind] = len(ciType.Extends)
		if len(ciType.Extends) == 0 {
			queue = append(queue, kind)
		}
		for _, parent := range ciType.Extends {
			if _, ok := g.CITypes[parent]; !ok {
				return fmt.Errorf(
					"ci type %q extends missing parent %q",
					kind,
					parent,
				)
			}
			children[parent] = append(children[parent], kind)
		}
	}
	visited := 0
	for index := 0; index < len(queue); index++ {
		parent := queue[index]
		visited++
		for _, child := range children[parent] {
			parentCounts[child]--
			if parentCounts[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if visited != len(g.CITypes) {
		for kind, remaining := range parentCounts {
			if remaining > 0 {
				return fmt.Errorf(
					"ci type %q has inheritance cycle",
					kind,
				)
			}
		}
	}
	return nil
}

func (g *Graph) validateEntitiesAgainstCITypesForKinds(
	kinds map[string]struct{},
) error {
	if kinds == nil {
		kinds = entityKindSet(g.Entities)
	}
	fieldsByKind := make(map[string]map[string]FieldSpec, len(kinds))
	for kind := range kinds {
		fields, err := g.effectiveFieldsCached(kind, fieldsByKind)
		if err != nil {
			return err
		}
		fieldsByKind[kind] = fields
	}
	uniqueValidator, err := newUniqueEntityValidator(g, kinds)
	if err != nil {
		return err
	}
	identityOwners := map[string]map[string]string{}
	for _, entity := range g.Entities {
		if _, affected := kinds[entity.Kind]; !affected {
			continue
		}
		if err := validateEntityFieldsWithSpecs(
			entity,
			fieldsByKind[entity.Kind],
		); err != nil {
			return err
		}
		if err := uniqueValidator.validate(entity); err != nil {
			return err
		}
		if err := g.validateCIIdentityOwners(entity, identityOwners); err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) validateCIIdentityOwners(
	entity Entity,
	ownersByKind map[string]map[string]string,
) error {
	signatures := g.ciIdentitySignatures(entity, entityConfidence(entity))
	if len(signatures) == 0 {
		return nil
	}
	owners := ownersByKind[entity.Kind]
	if owners == nil {
		owners = map[string]string{}
		ownersByKind[entity.Kind] = owners
	}
	for _, signature := range signatures {
		if owner := owners[signature.Value]; owner != "" && owner != entity.ID {
			return fmt.Errorf(
				"ci type %q identity %q is shared by entities %q and %q",
				entity.Kind,
				signature.Value,
				owner,
				entity.ID,
			)
		}
		owners[signature.Value] = entity.ID
	}
	return nil
}

func validateEntityFieldsWithSpecs(
	entity Entity,
	specs map[string]FieldSpec,
) error {
	for name, spec := range specs {
		value, exists := entity.Fields[name]
		if spec.Required && !exists {
			return fmt.Errorf("entity %q kind %q missing required field %q", entity.ID, entity.Kind, name)
		}
		if !exists {
			continue
		}
		if !valueMatchesType(value, spec.Type) {
			return fmt.Errorf("entity %q field %q does not match type %q", entity.ID, name, spec.Type)
		}
		if len(spec.Enum) > 0 && !valueInEnum(value, spec.Enum) {
			return fmt.Errorf("entity %q field %q value is not in enum", entity.ID, name)
		}
	}
	return nil
}

func (g *Graph) populateIdentity(entity *Entity) {
	if entity.Identity == nil {
		entity.Identity = map[string]any{}
	}
	ciType, ok := g.CITypes[entity.Kind]
	if ok {
		for _, key := range ciType.IdentityKeys {
			for _, field := range key.Fields {
				if _, exists := entity.Identity[field]; exists {
					continue
				}
				if value, ok := entity.Fields[field]; ok {
					entity.Identity[field] = copyAny(value)
				}
			}
		}
	}
	for _, source := range entity.Sources {
		entity.Identity["source:"+source.Source] = source.ExternalID
	}
}
