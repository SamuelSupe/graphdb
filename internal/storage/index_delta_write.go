package storage

import (
	"context"
	"errors"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func (s *TenantStore) reuseUnchangedIndexCatalogObjects(tenantID string, catalog *IndexCatalog, previous IndexCatalog) {
	previousIndexes := map[string]IndexSpec{}
	for _, spec := range previous.Indexes {
		previousIndexes[spec.Kind+"\x00"+spec.Field] = spec
	}
	for i := range catalog.Indexes {
		spec := &catalog.Indexes[i]
		previousSpec, ok := previousIndexes[spec.Kind+"\x00"+spec.Field]
		if !ok {
			continue
		}
		reuseIndexObjects(spec.Objects, previousSpec.Objects)
	}

	previousEdges := map[string]EdgeShard{}
	for _, spec := range previous.EdgeShards {
		previousEdges[spec.RelationType+"\x00"+spec.Shard] = spec
	}
	edgePackIDs := edgeShardPackIDs(catalog.EdgeShards)
	changedEdgeGroups := map[string]bool{}
	for i := range catalog.EdgeShards {
		spec := &catalog.EdgeShards[i]
		previousSpec, ok := previousEdges[spec.RelationType+"\x00"+spec.Shard]
		group := edgeCatalogGroup(edgePackIDs, *spec)
		if !ok || spec.ContentHash != previousSpec.ContentHash || spec.SchemaHash != previousSpec.SchemaHash || !indexObjectsReusable(s, tenantID, spec.Objects, previousSpec.Objects) {
			changedEdgeGroups[group] = true
		}
	}
	for i := range catalog.EdgeShards {
		spec := &catalog.EdgeShards[i]
		if changedEdgeGroups[edgeCatalogGroup(edgePackIDs, *spec)] {
			continue
		}
		previousSpec, ok := previousEdges[spec.RelationType+"\x00"+spec.Shard]
		if ok {
			spec.Objects = append([]IndexObject(nil), previousSpec.Objects...)
		}
	}

	previousPages := map[string]EntityPageSpec{}
	for _, spec := range previous.EntityPages {
		previousPages[spec.Shard] = spec
	}
	entityPackIDs := entityPagePackIDs(catalog.EntityPages, !s.WriteEntityRecords, s.EntityPagePackMaxBytes)
	changedEntityGroups := map[string]bool{}
	for i := range catalog.EntityPages {
		spec := &catalog.EntityPages[i]
		previousSpec, ok := previousPages[spec.Shard]
		group := entityCatalogGroup(entityPackIDs, *spec)
		if !ok || spec.ContentHash != previousSpec.ContentHash || spec.SchemaHash != previousSpec.SchemaHash || !indexObjectsReusable(s, tenantID, spec.Objects, previousSpec.Objects) {
			changedEntityGroups[group] = true
		}
	}
	for i := range catalog.EntityPages {
		spec := &catalog.EntityPages[i]
		if changedEntityGroups[entityCatalogGroup(entityPackIDs, *spec)] {
			continue
		}
		previousSpec, ok := previousPages[spec.Shard]
		if ok {
			spec.Objects = append([]IndexObject(nil), previousSpec.Objects...)
		}
	}
}

func edgeCatalogGroup(packIDs map[string]string, spec EdgeShard) string {
	key := spec.RelationType + "\x00" + spec.Shard
	if packID := packIDs[key]; packID != "" {
		return spec.RelationType + "\x00" + packID
	}
	return key
}

func entityCatalogGroup(packIDs map[string]string, spec EntityPageSpec) string {
	key := "entities\x00" + spec.Shard
	if packID := packIDs[key]; packID != "" {
		return "entities\x00" + packID
	}
	return key
}

func reuseIndexObjects(objects []IndexObject, previous []IndexObject) {
	previousByRole := map[string]IndexObject{}
	for _, object := range previous {
		previousByRole[object.Role] = object
	}
	for i := range objects {
		previousObject, ok := previousByRole[objects[i].Role]
		if ok && previousObject.ContentHash == objects[i].ContentHash && previousObject.SchemaHash == objects[i].SchemaHash {
			objects[i] = previousObject
		}
	}
}

func indexObjectsReusable(s *TenantStore, tenantID string, objects []IndexObject, previous []IndexObject) bool {
	if len(objects) == 0 || len(previous) == 0 {
		return false
	}
	previousByRole := map[string]IndexObject{}
	for _, object := range previous {
		previousByRole[object.Role] = object
	}
	for _, object := range objects {
		previousObject, ok := previousByRole[object.Role]
		if !ok ||
			object.ContentHash != previousObject.ContentHash ||
			object.SchemaHash != previousObject.SchemaHash ||
			s.parquetObjectSuffix(tenantID, object.Key) != s.parquetObjectSuffix(tenantID, previousObject.Key) {
			return false
		}
	}
	return true
}

func (s *TenantStore) parquetObjectSuffix(tenantID string, key string) string {
	prefix := s.parquetVersionRootPrefix(tenantID) + "v"
	if !strings.HasPrefix(key, prefix) {
		return key
	}
	rest := strings.TrimPrefix(key, prefix)
	slash := strings.IndexByte(rest, '/')
	if slash < 0 {
		return key
	}
	return rest[slash+1:]
}

func (s *TenantStore) writeChangedParquetSecondaryIndexesFast(ctx context.Context, tenantID string, indexes []SecondaryIndex, catalog IndexCatalog, version int64) error {
	specs := indexCatalogSpecMap(catalog)
	for _, index := range indexes {
		spec, ok := specs[index.Kind+"\x00"+index.Field]
		if !ok {
			continue
		}
		groups := secondaryIndexObjectGroups(index)
		if len(groups) == 0 {
			object, ok := indexObjectByRole(spec.Objects, "postings")
			if !ok || !s.indexObjectUsesVersion(tenantID, object, version) {
				continue
			}
			if err := s.putParquetSecondaryIndex(ctx, tenantID, index, false); err != nil {
				return err
			}
			continue
		}
		for _, group := range groups {
			if !s.secondaryIndexGroupUsesVersion(tenantID, spec, group, version) {
				continue
			}
			if err := s.putParquetSecondaryIndexShard(ctx, tenantID, group.ID, group.Index, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *TenantStore) writeIncrementalSecondaryIndexObjects(ctx context.Context, tenantID string, writes []incrementalSecondaryIndexWrite) error {
	for _, write := range writes {
		if err := s.putParquetSecondaryIndexObject(ctx, write.Key, tenantID, write.Index, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) secondaryIndexGroupUsesVersion(tenantID string, spec IndexSpec, group secondaryIndexObjectGroup, version int64) bool {
	for _, shardID := range group.Shards {
		object, ok := indexObjectByRole(spec.Objects, secondaryIndexShardRole(shardID))
		if ok && s.indexObjectUsesVersion(tenantID, object, version) {
			return true
		}
	}
	return false
}

func (s *TenantStore) writeChangedParquetEdgeShardsFast(ctx context.Context, tenantID string, shards []EdgeShardData, catalog IndexCatalog, version int64) error {
	specs := edgeShardSpecMap(catalog)
	for _, group := range edgeShardDataPackGroups(shards) {
		for i := range group.Shards {
			group.Shards[i].TenantID = tenantID
		}
		if !s.edgeShardGroupUsesVersion(tenantID, specs, group, version) {
			continue
		}
		pack := mergeEdgeShardPack(group)
		pack.TenantID = tenantID
		key := s.parquetEdgeShardVersionKey(tenantID, pack.Version, pack.RelationType, pack.Shard)
		if len(group.Shards) > 1 {
			key = s.parquetEdgeShardPackVersionKey(tenantID, pack.Version, pack.RelationType, group.ID)
		}
		if err := s.putParquetEdgeShardObject(ctx, key, tenantID, pack, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) edgeShardGroupUsesVersion(tenantID string, specs map[string]EdgeShard, group edgeShardDataPackGroup, version int64) bool {
	for _, shard := range group.Shards {
		spec, ok := specs[edgeShardTargetKey(shard.RelationType, shard.Shard)]
		if !ok {
			continue
		}
		for _, object := range spec.Objects {
			if s.indexObjectUsesVersion(tenantID, object, version) {
				return true
			}
		}
	}
	return false
}

func (s *TenantStore) writeChangedParquetEntityPagesFast(ctx context.Context, tenantID string, pages []EntityPageData, catalog IndexCatalog, before *graph.Graph, after *graph.Graph, changedEntityIDs []string, version int64) error {
	recordJobs := []entityRecordWriteJob{}
	writeRecords := s.WriteEntityRecords
	changed := make(map[string]struct{}, len(changedEntityIDs))
	for _, entityID := range changedEntityIDs {
		changed[entityID] = struct{}{}
	}
	specs := entityPageSpecMap(catalog)
	for _, group := range entityPageDataPackGroups(pages, !s.WriteEntityRecords, s.EntityPagePackMaxBytes) {
		for i := range group.Pages {
			group.Pages[i].TenantID = tenantID
		}
		if !s.entityPageGroupUsesVersion(tenantID, specs, group, version) {
			continue
		}
		pack := mergeEntityPagePack(group)
		pack.TenantID = tenantID
		key := s.parquetEntityPageVersionKey(tenantID, pack.Version, pack.Shard)
		if len(group.Pages) > 1 {
			key = s.parquetEntityPagePackVersionKey(tenantID, pack.Version, group.ID)
		}
		pageMeta, err := s.putParquetEntityPageObject(ctx, key, tenantID, pack, false)
		if err != nil {
			return err
		}
		if writeRecords {
			for _, page := range group.Pages {
				pageHash := entityPageContentHash(page)
				for _, entity := range page.Entities {
					if _, ok := changed[entity.ID]; !ok {
						continue
					}
					record := newEntityRecord(tenantID, entity, page.Shard, pageHash, pageMeta.ETag, page.Version, page.UpdatedAt)
					recordJobs = append(recordJobs, entityRecordWriteJob{Key: s.entityRecordKey(tenantID, entity.ID), Record: record})
				}
			}
		}
	}
	if !writeRecords {
		return nil
	}
	if err := s.putEntityRecordBatch(ctx, recordJobs); err != nil {
		return err
	}
	return s.tombstoneDeletedEntityRecords(ctx, tenantID, before, after, changedEntityIDs, version)
}

func (s *TenantStore) entityPageGroupUsesVersion(tenantID string, specs map[string]EntityPageSpec, group entityPageDataPackGroup, version int64) bool {
	for _, page := range group.Pages {
		spec, ok := specs[page.Shard]
		if !ok {
			continue
		}
		for _, object := range spec.Objects {
			if s.indexObjectUsesVersion(tenantID, object, version) {
				return true
			}
		}
	}
	return false
}

func (s *TenantStore) tombstoneDeletedEntityRecords(ctx context.Context, tenantID string, before *graph.Graph, after *graph.Graph, changedEntityIDs []string, version int64) error {
	if before == nil || after == nil {
		return nil
	}
	for _, id := range changedEntityIDs {
		if _, existed := before.Entities[id]; !existed {
			continue
		}
		if _, exists := after.Entities[id]; exists {
			continue
		}
		key := s.entityRecordKey(tenantID, id)
		s.clearCoordinatedWriterObjectKey(key)
		meta, err := objectMeta(ctx, s.Objects, key)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return err
		}
		record := EntityRecord{
			LayoutVersion: CurrentObjectLayoutVersion,
			TenantID:      tenantID,
			ID:            id,
			Page:          entityShardID(id),
			Deleted:       true,
			Version:       version,
		}
		stampEntityRecordHash(&record)
		if err := s.putEntityRecordWithMeta(ctx, key, record, meta); err != nil {
			return err
		}
	}
	return nil
}

func (s *TenantStore) indexObjectUsesVersion(tenantID string, object IndexObject, version int64) bool {
	objectVersion, ok := s.parquetVersionFromKey(tenantID, object.Key)
	return ok && objectVersion == version
}

func indexCatalogSpecMap(catalog IndexCatalog) map[string]IndexSpec {
	specs := make(map[string]IndexSpec, len(catalog.Indexes))
	for _, spec := range catalog.Indexes {
		specs[spec.Kind+"\x00"+spec.Field] = spec
	}
	return specs
}
