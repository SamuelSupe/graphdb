package graph

import (
	"encoding/json"
	"sort"
)

type logicalHashCategoryUpdate struct {
	key     string
	encoded []byte
	exists  bool
}

func updateLogicalHashCategoryBatch(
	category *logicalHashCategory,
	touched map[string]trackedFingerprint,
	lookup func(string) (any, bool),
) error {
	if len(touched) == 0 {
		return nil
	}
	updates := make([]logicalHashCategoryUpdate, 0, len(touched))
	for key := range touched {
		value, exists := lookup(key)
		var encoded []byte
		if exists {
			data, err := json.Marshal(value)
			if err != nil {
				return err
			}
			encoded = data
		}
		updates = append(updates, logicalHashCategoryUpdate{
			key: key, encoded: encoded, exists: exists,
		})
	}
	sort.Slice(updates, func(i, j int) bool {
		return updates[i].key < updates[j].key
	})

	keys := make([]string, 0, len(category.keys)+len(updates))
	encoded := make([][]byte, 0, cap(keys))
	oldIndex, updateIndex := 0, 0
	for oldIndex < len(category.keys) || updateIndex < len(updates) {
		if updateIndex >= len(updates) ||
			oldIndex < len(category.keys) &&
				category.keys[oldIndex] < updates[updateIndex].key {
			keys = append(keys, category.keys[oldIndex])
			encoded = append(encoded, category.encoded[oldIndex])
			oldIndex++
			continue
		}
		update := updates[updateIndex]
		if oldIndex < len(category.keys) &&
			category.keys[oldIndex] == update.key {
			oldIndex++
		}
		if update.exists {
			keys = append(keys, update.key)
			encoded = append(encoded, update.encoded)
		}
		updateIndex++
	}
	category.keys = keys
	category.encoded = encoded
	return nil
}
