package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) buildIncrementalEntityPages(ctx context.Context, tenantID string, previousVersion int64, previous []EntityPageSpec, before *graph.Graph, after *graph.Graph, entityIDs []string, version int64, now time.Time) ([]EntityPageData, []EntityPageSpec, error) {
	previousByShard := entityPageSpecMap(IndexCatalog{EntityPages: previous})
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
	pages := make([]EntityPageData, 0, len(shards))
	rawSpecs := make([]EntityPageSpec, 0, len(shards))
	removed := map[string]struct{}{}
	for _, shard := range shards {
		spec, existed := previousByShard[shard]
		page := EntityPageData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, Shard: shard}
		if existed {
			loaded, _, valid, err := s.loadValidatedParquetEntityPageObject(ctx, tenantID, previousVersion, spec)
			if err != nil {
				return nil, nil, err
			}
			if !valid {
				return nil, nil, fmt.Errorf("incremental entity page %s is not readable at version %d", shard, previousVersion)
			}
			page = loaded
		}
		entities := make(map[string]graph.Entity, len(page.Entities)+len(changedByShard[shard]))
		for _, entity := range page.Entities {
			entities[entity.ID] = entity
		}
		for _, entityID := range changedByShard[shard] {
			delete(entities, entityID)
			if entity, ok := after.Entities[entityID]; ok {
				entities[entityID] = graph.CopyEntity(entity)
			} else if !existed {
				return nil, nil, fmt.Errorf("incremental entity page %s is missing from the previous catalog", shard)
			}
		}
		if len(entities) == 0 {
			removed[shard] = struct{}{}
			continue
		}
		page = EntityPageData{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			Shard:         shard,
			Entities:      make([]graph.Entity, 0, len(entities)),
			Version:       version,
			UpdatedAt:     now,
		}
		for _, entity := range entities {
			page.Entities = append(page.Entities, entity)
		}
		sort.Slice(page.Entities, func(i, j int) bool { return page.Entities[i].ID < page.Entities[j].ID })
		page.logicalContentHash = entityPageContentHash(page)
		pages = append(pages, page)
		rawSpecs = append(rawSpecs, EntityPageSpec{
			Shard:       shard,
			EntityCount: len(page.Entities),
			ContentHash: page.logicalContentHash,
			UpdatedAt:   now,
		})
	}

	mini := IndexCatalog{Version: version, EntityPages: rawSpecs}
	s.decorateIndexCatalog(&mini, tenantID, IndexFormatParquet)
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
