package graph

import (
	"fmt"
)

func (g *Graph) applyEntitySchema(entity *Entity) error {
	specs, err := g.EffectiveFields(entity.Kind)
	if err != nil {
		return err
	}
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

func (g *Graph) effectiveFields(kind string, seen map[string]struct{}) (map[string]FieldSpec, error) {
	ciType, ok := g.CITypes[kind]
	if !ok {
		return map[string]FieldSpec{}, nil
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
		parentFields, err := g.effectiveFields(parent, seen)
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
	return fields, nil
}

func (g *Graph) EffectiveFields(kind string) (map[string]FieldSpec, error) {
	return g.effectiveFields(kind, map[string]struct{}{})
}

func (g *Graph) validateCITypes() error {
	for kind := range g.CITypes {
		if _, err := g.EffectiveFields(kind); err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) validateEntitiesAgainstCITypes() error {
	for _, entity := range g.Entities {
		if err := g.validateEntityFields(entity); err != nil {
			return err
		}
		if err := g.validateUniqueEntity(entity); err != nil {
			return err
		}
	}
	return nil
}

func (g *Graph) validateEntityFields(entity Entity) error {
	specs, err := g.EffectiveFields(entity.Kind)
	if err != nil {
		return err
	}
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

func (g *Graph) validateUniqueEntity(entity Entity) error {
	specs, err := g.EffectiveFields(entity.Kind)
	if err != nil {
		return err
	}
	for field, spec := range specs {
		if !spec.Unique {
			continue
		}
		value, ok := entity.Fields[field]
		if !ok {
			continue
		}
		for id, existing := range g.Entities {
			if id == entity.ID || existing.Kind != entity.Kind {
				continue
			}
			existingValue, exists := existing.Fields[field]
			if !exists {
				continue
			}
			if fieldValuesEqual(existingValue, value) {
				return fmt.Errorf("entity %q violates unique field %q for kind %q", entity.ID, field, entity.Kind)
			}
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
