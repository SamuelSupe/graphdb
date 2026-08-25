package graph

import (
	"encoding/json"
	"sort"
)

type logicalHashCategoryUpdate struct {
	key         string
	encoded     []byte
	fingerprint [16]byte
	exists      bool
}

func updateLogicalHashCategoryBatch(
	category *logicalHashCategory,
	kind string,
	touched map[string]trackedFingerprint,
	lookup func(string) (any, bool),
) error {
	if len(touched) == 0 {
		return nil
	}
	updates := make([]logicalHashCategoryUpdate, 0, len(touched))
	for key, tracked := range touched {
		exists := tracked.exists
		encoded := tracked.encoded
		fingerprint := tracked.value
		var value any
		if !tracked.resolved || exists && encoded == nil {
			var currentExists bool
			value, currentExists = lookup(key)
			exists = currentExists
		}
		if exists && encoded == nil {
			data, err := json.Marshal(value)
			if err != nil {
				return err
			}
			encoded = data
			fingerprint = contentFingerprintEntryJSON(kind, key, encoded)
		}
		updates = append(updates, logicalHashCategoryUpdate{
			key: key, encoded: encoded, fingerprint: fingerprint, exists: exists,
		})
	}
	sort.Slice(updates, func(i, j int) bool {
		return updates[i].key < updates[j].key
	})

	keys := make([]string, 0, len(category.keys)+len(updates))
	encoded := make([][]byte, 0, cap(keys))
	fingerprints := make([][16]byte, 0, cap(keys))
	completeFingerprints := len(category.fingerprints) == len(category.keys)
	oldIndex, updateIndex := 0, 0
	for oldIndex < len(category.keys) || updateIndex < len(updates) {
		if updateIndex >= len(updates) ||
			oldIndex < len(category.keys) &&
				category.keys[oldIndex] < updates[updateIndex].key {
			keys = append(keys, category.keys[oldIndex])
			encoded = append(encoded, category.encoded[oldIndex])
			if completeFingerprints {
				fingerprints = append(fingerprints, category.fingerprints[oldIndex])
			} else {
				fingerprints = append(fingerprints, contentFingerprintEntryJSON(
					kind, category.keys[oldIndex], category.encoded[oldIndex],
				))
			}
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
			fingerprints = append(fingerprints, update.fingerprint)
		}
		updateIndex++
	}
	category.keys = keys
	category.encoded = encoded
	category.fingerprints = fingerprints
	return nil
}
