package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) buildIncrementalEntityPages(ctx context.Context, tenantID string, previousVersion int64, previous []EntityPageSpec, before *graph.Graph, after *graph.Graph, entityIDs []string, version int64, now time.Time) ([]EntityPageData, []EntityPageSpec, error) {
	if before.Version != previousVersion || after.Version != version {
		return nil, nil, fmt.Errorf("incremental entity pages require graph versions %d and %d", previousVersion, version)
	}
	changedByShard := map[string][]string{}
	for _, entityID := range entityIDs {
		_, oldOK := before.Entities[entityID]
		_, newOK := after.Entities[entityID]
		if !oldOK && !newOK {
			continue
		}
		shard := entityShardID(entityID)
		changedByShard[shard] = append(changedByShard[shard], entityID)
	}
	shards := sortedStringKeys(changedByShard)
	if len(shards) == 0 {
		return nil, previous, ctx.Err()
	}
	// The commit already owns the authoritative graph. Rebuilding only the
	// affected shards avoids reading and decoding their previous packed pages.
	entitiesByShard := make(map[string][]graph.Entity, len(shards))
	checked := 0
	for id, entity := range after.Entities {
		checked++
		if checked&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, nil, err
			}
		}
		shard := entityShardID(id)
		if _, changed := changedByShard[shard]; changed {
			// The published graph is immutable; only the page slice is reordered.
			entitiesByShard[shard] = append(entitiesByShard[shard], entity)
		}
	}
	pages := make([]EntityPageData, 0, len(shards))
	rawSpecs := make([]EntityPageSpec, 0, len(shards))
	removed := map[string]struct{}{}
	for _, shard := range shards {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		entities := entitiesByShard[shard]
		if len(entities) == 0 {
			removed[shard] = struct{}{}
			continue
		}
		page := EntityPageData{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			Shard:         shard,
			Entities:      entities,
			Version:       version,
			UpdatedAt:     now,
			hashCanonical: true,
		}
		for _, entity := range entities {
			page.hashCanonical = page.hashCanonical && graphEntityHashCanonical(entity)
		}
		sort.Slice(page.Entities, func(i, j int) bool { return page.Entities[i].ID < page.Entities[j].ID })
		page.logicalContentHash = entityPageContentHash(page)
		pages = append(pages, page)
		rawSpecs = append(rawSpecs, EntityPageSpec{
			Shard:          shard,
			EntityCount:    len(page.Entities),
			ContentHash:    page.logicalContentHash,
			UpdatedAt:      now,
			estimatedBytes: entityPagePackBytes(page),
		})
	}

	mini := IndexCatalog{Version: version, EntityPages: rawSpecs}
	s.decorateIndexCatalog(&mini, tenantID)
	decorated := entityPageSpecMap(mini)
	next := make([]EntityPageSpec, 0, len(previous)+len(decorated))
	for _, spec := range previous {
		if _, deleted := removed[spec.Shard]; deleted {
			continue
		}
		if replacement, ok := decorated[spec.Shard]; ok {
			if replacement.ContentHash == spec.ContentHash && replacement.SchemaHash == spec.SchemaHash {
				replacement.Objects = append([]IndexObject(nil), spec.Objects...)
			}
			next = append(next, replacement)
			delete(decorated, spec.Shard)
			continue
		}
		next = append(next, spec)
	}
	for _, shard := range sortedEntityPageSpecKeys(decorated) {
		next = append(next, decorated[shard])
	}
	return pages, next, nil
}

func sortedEntityPageSpecKeys(values map[string]EntityPageSpec) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
