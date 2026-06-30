package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"unicode"
)

const (
	secondaryIndexShardRolePrefix        = "postings_shard:"
	secondaryIndexStringShardPrefixBytes = 7
	secondaryIndexStringShardMaxBytes    = 24
	secondaryIndexShardPackTargetRows    = 2048
	secondaryIndexShardPackMaxShards     = 64
)

type secondaryIndexShard struct {
	ID    string
	Index SecondaryIndex
}

type secondaryIndexObjectGroup struct {
	ID     string
	Shards []string
	Index  SecondaryIndex
}

func secondaryIndexObjects(index SecondaryIndex) []IndexObject {
	groups := secondaryIndexObjectGroups(index)
	if len(groups) == 0 {
		return []IndexObject{{
			Role:        "postings",
			RowCount:    secondaryIndexEntryCount(index),
			ContentHash: secondaryIndexContentHash(index),
		}}
	}
	objects := make([]IndexObject, 0, len(groups))
	for _, group := range groups {
		rowCount := secondaryIndexEntryCount(group.Index)
		contentHash := secondaryIndexContentHash(group.Index)
		for _, shardID := range group.Shards {
			objects = append(objects, IndexObject{
				Role:        secondaryIndexShardRole(shardID),
				Key:         group.ID,
				RowCount:    rowCount,
				ContentHash: contentHash,
			})
		}
	}
	return objects
}

func secondaryIndexObjectGroups(index SecondaryIndex) []secondaryIndexObjectGroup {
	logical := splitSecondaryIndex(index)
	if len(logical) == 0 {
		return nil
	}
	groups := make([]secondaryIndexObjectGroup, 0, len(logical))
	current := make([]secondaryIndexShard, 0)
	currentRows := 0
	flush := func() {
		if len(current) == 0 {
			return
		}
		groups = append(groups, secondaryIndexPackGroup(current))
		current = nil
		currentRows = 0
	}
	for _, shard := range logical {
		rows := secondaryIndexEntryCount(shard.Index)
		if rows >= secondaryIndexShardPackTargetRows {
			flush()
			groups = append(groups, secondaryIndexPackGroup([]secondaryIndexShard{shard}))
			continue
		}
		if len(current) > 0 && (currentRows+rows > secondaryIndexShardPackTargetRows || len(current) >= secondaryIndexShardPackMaxShards) {
			flush()
		}
		current = append(current, shard)
		currentRows += rows
	}
	flush()
	return groups
}

func secondaryIndexPackGroup(shards []secondaryIndexShard) secondaryIndexObjectGroup {
	ids := make([]string, 0, len(shards))
	indexes := make([]SecondaryIndex, 0, len(shards))
	for _, shard := range shards {
		ids = append(ids, shard.ID)
		indexes = append(indexes, shard.Index)
	}
	sort.Strings(ids)
	return secondaryIndexObjectGroup{
		ID:     secondaryIndexPackID(ids),
		Shards: ids,
		Index:  mergeSecondaryIndexShards(indexes),
	}
}

func mergeSecondaryIndexShards(indexes []SecondaryIndex) SecondaryIndex {
	var merged SecondaryIndex
	merged.Values = map[string][]string{}
	for _, index := range indexes {
		if merged.Kind == "" {
			merged.LayoutVersion = index.LayoutVersion
			merged.TenantID = index.TenantID
			merged.Kind = index.Kind
			merged.Field = index.Field
			merged.Unique = index.Unique
			merged.Version = index.Version
			merged.UpdatedAt = index.UpdatedAt
		}
		for value, ids := range index.Values {
			merged.Values[value] = append(merged.Values[value], ids...)
		}
	}
	for value := range merged.Values {
		sort.Strings(merged.Values[value])
	}
	return merged
}

func secondaryIndexPackID(shardIDs []string) string {
	if len(shardIDs) == 0 {
		return "empty"
	}
	if len(shardIDs) == 1 {
		return shardIDs[0]
	}
	sum := sha256.Sum256([]byte(strings.Join(shardIDs, "\x00")))
	return cleanSecondaryIndexShardID("pack_" + shardIDs[0] + "_" + hex.EncodeToString(sum[:6]))
}

func splitSecondaryIndex(index SecondaryIndex) []secondaryIndexShard {
	byID := map[string]SecondaryIndex{}
	for value, ids := range index.Values {
		shardID := secondaryIndexShardID(value)
		shard, ok := byID[shardID]
		if !ok {
			shard = SecondaryIndex{
				LayoutVersion: index.LayoutVersion,
				TenantID:      index.TenantID,
				Kind:          index.Kind,
				Field:         index.Field,
				Unique:        index.Unique,
				Values:        map[string][]string{},
				Version:       index.Version,
				UpdatedAt:     index.UpdatedAt,
			}
		}
		shard.Values[value] = append([]string(nil), ids...)
		sort.Strings(shard.Values[value])
		byID[shardID] = shard
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]secondaryIndexShard, 0, len(ids))
	for _, id := range ids {
		out = append(out, splitSecondaryIndexShard(secondaryIndexShard{ID: id, Index: byID[id]})...)
	}
	return out
}

func splitSecondaryIndexShard(shard secondaryIndexShard) []secondaryIndexShard {
	if secondaryIndexEntryCount(shard.Index) <= secondaryIndexShardPackTargetRows || !strings.HasPrefix(shard.ID, "s_") {
		return []secondaryIndexShard{shard}
	}
	prefix := strings.TrimPrefix(shard.ID, "s_")
	nextPrefixBytes := len(prefix) + 1
	return splitStringSecondaryIndexShard(shard, nextPrefixBytes)
}

func splitStringSecondaryIndexShard(shard secondaryIndexShard, prefixBytes int) []secondaryIndexShard {
	if secondaryIndexEntryCount(shard.Index) <= secondaryIndexShardPackTargetRows || prefixBytes > secondaryIndexStringShardMaxBytes || len(shard.Index.Values) < 2 {
		return []secondaryIndexShard{shard}
	}
	byID := map[string]SecondaryIndex{}
	for value, ids := range shard.Index.Values {
		shardID := secondaryIndexStringShardID(value, prefixBytes)
		next := byID[shardID]
		if next.Values == nil {
			next = SecondaryIndex{
				LayoutVersion: shard.Index.LayoutVersion,
				TenantID:      shard.Index.TenantID,
				Kind:          shard.Index.Kind,
				Field:         shard.Index.Field,
				Unique:        shard.Index.Unique,
				Values:        map[string][]string{},
				Version:       shard.Index.Version,
				UpdatedAt:     shard.Index.UpdatedAt,
			}
		}
		next.Values[value] = append([]string(nil), ids...)
		sort.Strings(next.Values[value])
		byID[shardID] = next
	}
	if len(byID) < 2 {
		return []secondaryIndexShard{shard}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]secondaryIndexShard, 0, len(ids))
	for _, id := range ids {
		out = append(out, splitStringSecondaryIndexShard(secondaryIndexShard{ID: id, Index: byID[id]}, prefixBytes+1)...)
	}
	return out
}

func secondaryIndexShardRole(shardID string) string {
	return secondaryIndexShardRolePrefix + shardID
}

func secondaryIndexShardIDFromRole(role string) (string, bool) {
	id, ok := strings.CutPrefix(role, secondaryIndexShardRolePrefix)
	return id, ok && id != ""
}

func secondaryIndexShardID(valueKey string) string {
	switch {
	case valueKey == "null":
		return "null"
	case strings.HasPrefix(valueKey, "b:"):
		return cleanSecondaryIndexShardID(strings.ReplaceAll(valueKey, ":", "_"))
	case strings.HasPrefix(valueKey, "n:"):
		return cleanSecondaryIndexShardID("n_" + numericShardPrefix(strings.TrimPrefix(valueKey, "n:")))
	case strings.HasPrefix(valueKey, "s:"):
		return secondaryIndexStringShardID(valueKey, secondaryIndexStringShardPrefixBytes)
	default:
		return "other"
	}
}

func secondaryIndexStringShardID(valueKey string, prefixBytes int) string {
	value := strings.ToLower(strings.TrimPrefix(valueKey, "s:"))
	if value == "" {
		return "s_empty"
	}
	return cleanSecondaryIndexShardID("s_" + stringShardPrefixBytes(value, prefixBytes))
}

func stringShardPrefix(value string) string {
	return stringShardPrefixBytes(value, secondaryIndexStringShardPrefixBytes)
}

func stringShardPrefixBytes(value string, prefixBytes int) string {
	if prefixBytes <= 0 || len(value) <= prefixBytes {
		return value
	}
	return value[:prefixBytes]
}

func numericShardPrefix(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "empty"
	}
	if len(value) > 8 {
		return value[:8]
	}
	return value
}

func cleanSecondaryIndexShardID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "empty"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			b.WriteRune(unicode.ToLower(r))
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "_")
	if out == "" {
		return "empty"
	}
	return out
}

func secondaryIndexEntryCount(index SecondaryIndex) int {
	count, _ := secondaryIndexCounts(index)
	return count
}
