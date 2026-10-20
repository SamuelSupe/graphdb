package storage

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func (s *TenantStore) buildIncrementalEntityPages(ctx context.Context, tenantID string, previousVersion int64, previous []EntityPageSpec, before *graph.Graph, after *graph.Graph, entityIDs []string, version int64, now time.Time) ([]EntityPageData, []EntityPageSpec, error) {
	if before.Version != previousVersion || after.Version != version {
		return nil, nil, fmt.Errorf("incremental entity pages require graph versions %d and %d", previousVersion, version)
	}
	for _, spec := range previous {
		if (hasIndexRanges(spec.Objects) && !s.WriteIndexRangePages) || (!hasIndexRanges(spec.Objects) && spec.Codec != parquetEntitySingleCodec) {
			return nil, nil, fmt.Errorf("legacy entity pages require a controlled rebuild")
		}
	}
	previousByShard := entityPageSpecMap(IndexCatalog{EntityPages: previous})
	changed := map[string][]string{}
	for _, id := range entityIDs {
		_, oldOK := before.Entities.Get(id)
		_, newOK := after.Entities.Get(id)
		if oldOK || newOK {
			shard := entityShardID(id)
			changed[shard] = append(changed[shard], id)
		}
	}
	if len(changed) == 0 {
		return nil, previous, ctx.Err()
	}
	replacements := map[string]EntityPageSpec{}
	removed := map[string]bool{}
	var pages []EntityPageData
	for _, shard := range sortedStringKeys(changed) {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		old, exists := previousByShard[shard]
		ranged := hasIndexRanges(old.Objects)
		objects := old.Objects
		if ranged {
			if err := validateRangeObjects(objects, old.EntityCount, old.ContentHash); err != nil {
				return nil, nil, err
			}
		} else {
			objects = []IndexObject{{}}
		}
		byRange := map[int][]string{}
		for _, id := range changed[shard] {
			n := 0
			if ranged {
				n = rangeObjectIndex(objects, id)
			}
			byRange[n] = append(byRange[n], id)
		}
		var nextObjects []IndexObject
		var nextLegacy EntityPageSpec
		for n, object := range objects {
			ids, touched := byRange[n]
			if !touched {
				nextObjects = append(nextObjects, object)
				continue
			}
			rows := map[string]graph.Entity{}
			if exists {
				partSpec := old
				if ranged {
					partSpec = entityRangeSpec(old, object)
				}
				part, _, ok, err := s.borrowValidatedParquetEntityPageObject(ctx, tenantID, previousVersion, partSpec)
				if err != nil {
					return nil, nil, err
				}
				if !ok {
					return nil, nil, fmt.Errorf("previous entity page is unavailable")
				}
				routing, _ := decodeIndexRange(object.Role)
				for _, entity := range part.Entities {
					if ranged && !routing.contains(entity.ID) {
						return nil, nil, fmt.Errorf("record is outside its previous index range")
					}
					rows[entity.ID] = entity
				}
			}
			for _, id := range ids {
				if entity, ok := after.Entities.Get(id); ok {
					rows[id] = entity
				} else {
					delete(rows, id)
				}
			}
			if len(rows) == 0 {
				continue
			}
			page := EntityPageData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, Shard: shard, Version: version, UpdatedAt: now, hashCanonical: true}
			if ranged {
				page.pageRole = object.Role
			}
			for _, entity := range rows {
				page.Entities = append(page.Entities, entity)
				page.hashCanonical = page.hashCanonical && graphEntityHashCanonical(entity)
			}
			sort.Slice(page.Entities, func(i, j int) bool { return page.Entities[i].ID < page.Entities[j].ID })
			parts := []EntityPageData{page}
			var err error
			if s.WriteIndexRangePages {
				parts, err = prepareBoundedEntityPages(s.rangeEncodeContext(ctx), tenantID, parts)
			}
			if err != nil {
				return nil, nil, err
			}
			for _, part := range parts {
				part.logicalContentHash = entityPageContentHash(part)
				if part.pageRole == "" {
					nextLegacy = entityPageSpecs([]EntityPageData{part})[0]
					if old.ContentHash == nextLegacy.ContentHash {
						nextLegacy.Objects = append([]IndexObject(nil), old.Objects...)
					}
					pages = append(pages, part)
				} else {
					next := IndexObject{Role: part.pageRole, Key: s.parquetEntityPageVersionKey(tenantID, version, boundedPageObjectID(shard, part.logicalContentHash)), RowCount: len(part.Entities), ContentHash: part.logicalContentHash, SchemaHash: parquetEntityPageSchemaHash()}
					if ranged && object.ContentHash == next.ContentHash && object.SchemaHash == next.SchemaHash {
						next = object
					} else {
						pages = append(pages, part)
					}
					nextObjects = append(nextObjects, next)
				}
			}
		}
		if len(nextObjects) > 0 {
			routing, _ := decodeIndexRange(nextObjects[0].Role)
			routing.Lower = ""
			nextObjects[0].Role = encodeRangeRole(routing)
			spec := EntityPageSpec{Shard: shard, Format: IndexFormatParquet, Objects: nextObjects, UpdatedAt: now}
			for _, object := range nextObjects {
				spec.EntityCount += object.RowCount
			}
			replacements[shard] = spec
		} else if nextLegacy.EntityCount > 0 {
			replacements[shard] = nextLegacy
		} else {
			removed[shard] = true
		}
	}
	mini := IndexCatalog{Version: version}
	for _, shard := range sortedEntityPageSpecKeys(replacements) {
		mini.EntityPages = append(mini.EntityPages, replacements[shard])
	}
	s.decorateIndexCatalog(&mini, tenantID)
	replacements = entityPageSpecMap(mini)
	next := make([]EntityPageSpec, 0, len(previous)+len(replacements))
	for _, spec := range previous {
		if removed[spec.Shard] {
			continue
		}
		if replacement, ok := replacements[spec.Shard]; ok {
			if !hasIndexRanges(replacement.Objects) && replacement.ContentHash == spec.ContentHash && replacement.SchemaHash == spec.SchemaHash {
				replacement.Objects = append([]IndexObject(nil), spec.Objects...)
			}
			next = append(next, replacement)
			delete(replacements, spec.Shard)
		} else {
			next = append(next, spec)
		}
	}
	for _, shard := range sortedEntityPageSpecKeys(replacements) {
		next = append(next, replacements[shard])
	}
	return pages, next, ctx.Err()
}

func sortedEntityPageSpecKeys(values map[string]EntityPageSpec) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
