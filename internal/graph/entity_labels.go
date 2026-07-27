package graph

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// ReservedLabelsField keeps 1.1 labels inside the 1.0-compatible entity fields map.
const ReservedLabelsField = "__graphdb_labels"

// EntityLabels returns the canonical labels stored on an entity.
func EntityLabels(entity Entity) []string {
	labels, ok := labelsFromValue(entity.Fields[ReservedLabelsField])
	if !ok {
		return nil
	}
	return labels
}

// SetEntityLabels stores labels without changing the persisted Entity layout.
func SetEntityLabels(entity *Entity, labels []string) error {
	if entity == nil {
		return fmt.Errorf("entity is required")
	}
	normalized, err := normalizeLabels(labels)
	if err != nil {
		return err
	}
	if entity.Fields == nil {
		entity.Fields = Fields{}
	}
	entity.Fields[ReservedLabelsField] = labelsValue(normalized)
	return nil
}

func (entity Entity) MarshalJSON() ([]byte, error) {
	type entityAlias Entity
	return json.Marshal(struct {
		entityAlias
		Labels []string `json:"labels,omitempty"`
	}{
		entityAlias: entityAlias(entity),
		Labels:      EntityLabels(entity),
	})
}

func (entity *Entity) UnmarshalJSON(data []byte) error {
	type entityAlias Entity
	wire := struct {
		*entityAlias
		Labels json.RawMessage `json:"labels"`
	}{entityAlias: (*entityAlias)(entity)}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	stored, storedOK := labelsFromValue(entity.Fields[ReservedLabelsField])
	if len(wire.Labels) == 0 {
		if storedOK {
			entity.Fields[ReservedLabelsField] = labelsValue(stored)
		}
		return nil
	}

	var labels []string
	if err := json.Unmarshal(wire.Labels, &labels); err != nil {
		return fmt.Errorf("labels must be an array of strings: %w", err)
	}
	normalized, err := normalizeLabels(labels)
	if err != nil {
		return err
	}
	if _, exists := entity.Fields[ReservedLabelsField]; exists {
		if !storedOK || !reflect.DeepEqual(stored, normalized) {
			return fmt.Errorf("labels conflict with fields.%s", ReservedLabelsField)
		}
	}
	return SetEntityLabels(entity, normalized)
}

func labelsFromValue(value any) ([]string, bool) {
	if value == nil {
		return nil, false
	}
	var labels []string
	switch typed := value.(type) {
	case []string:
		labels = append(labels, typed...)
	case []any:
		labels = make([]string, 0, len(typed))
		for _, item := range typed {
			label, ok := item.(string)
			if !ok {
				return nil, false
			}
			labels = append(labels, label)
		}
	default:
		return nil, false
	}
	normalized, err := normalizeLabels(labels)
	return normalized, err == nil
}

func normalizeLabels(labels []string) ([]string, error) {
	seen := make(map[string]struct{}, len(labels))
	normalized := make([]string, 0, len(labels))
	for _, label := range labels {
		label = strings.TrimSpace(label)
		if label == "" {
			return nil, fmt.Errorf("labels must not contain empty values")
		}
		if _, exists := seen[label]; exists {
			continue
		}
		seen[label] = struct{}{}
		normalized = append(normalized, label)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func labelsValue(labels []string) []any {
	value := make([]any, len(labels))
	for i, label := range labels {
		value[i] = label
	}
	return value
}
