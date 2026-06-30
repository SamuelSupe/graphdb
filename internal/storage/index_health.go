package storage

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type IndexHealth struct {
	TenantID               string    `json:"tenant_id"`
	Status                 string    `json:"status"`
	ManifestVersion        int64     `json:"manifest_version"`
	SnapshotVersion        int64     `json:"snapshot_version,omitempty"`
	SnapshotCatalogVersion int64     `json:"snapshot_catalog_version,omitempty"`
	CatalogVersion         int64     `json:"catalog_version,omitempty"`
	CheckedAt              time.Time `json:"checked_at"`
	Issues                 []string  `json:"issues,omitempty"`
}

type IndexHealthOptions struct {
	Deep bool
}

func (s *TenantStore) IndexHealth(ctx context.Context, tenantID string) (IndexHealth, error) {
	return s.IndexHealthWithOptions(ctx, tenantID, IndexHealthOptions{Deep: true})
}

func (s *TenantStore) IndexHealthWithOptions(ctx context.Context, tenantID string, options IndexHealthOptions) (IndexHealth, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexHealth{}, err
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return IndexHealth{}, err
	}
	health := IndexHealth{TenantID: tenantID, ManifestVersion: manifest.Version, SnapshotVersion: manifest.SnapshotVersion, CheckedAt: time.Now().UTC(), Status: "ready"}
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if errors.Is(err, ErrNotFound) {
		health.Status = "missing"
		health.Issues = append(health.Issues, "index catalog is missing")
		return health, nil
	}
	if err != nil {
		return IndexHealth{}, err
	}
	health.CatalogVersion = catalog.Version
	if catalog.Version != manifest.Version {
		health.Status = "stale"
		health.Issues = append(health.Issues, "index catalog version does not match manifest")
		return health, nil
	}
	if !options.Deep {
		return health, nil
	}
	s.checkSnapshotCatalogObjects(ctx, tenantID, manifest, &health)
	if hasBlockingIndexHealthIssue(health.Issues) && health.Status == "ready" {
		health.Status = "error"
		return health, nil
	}
	loaded, err := s.loadWithMeta(ctx, tenantID)
	if err != nil {
		return IndexHealth{}, err
	}
	if loaded.Manifest.Version != catalog.Version {
		health.Status = "stale"
		health.Issues = append(health.Issues, "loaded graph version does not match index catalog")
		return health, nil
	}
	definitions, err := s.getIndexDefinitions(ctx, tenantID)
	if err != nil {
		return IndexHealth{}, err
	}
	s.checkFieldIndexObjects(ctx, tenantID, catalog, loaded.Graph, definitions, &health)
	s.checkEdgeShardObjects(ctx, tenantID, catalog, loaded.Graph, &health)
	s.checkEntityPageObjects(ctx, tenantID, catalog, loaded.Graph, &health)
	s.checkOrphanIndexObjects(ctx, tenantID, catalog, &health)
	if hasBlockingIndexHealthIssue(health.Issues) && health.Status == "ready" {
		health.Status = "error"
	}
	return health, nil
}

func (s *TenantStore) checkSnapshotCatalogObjects(ctx context.Context, tenantID string, manifest Manifest, health *IndexHealth) {
	if manifest.SnapshotCatalogKey == "" {
		if manifest.SnapshotKey != "" || manifest.SnapshotVersion > 0 {
			health.Issues = append(health.Issues, "snapshot catalog is missing from manifest")
		}
		return
	}
	if err := s.validateTenantObjectKey(tenantID, manifest.SnapshotCatalogKey); err != nil {
		health.Issues = append(health.Issues, err.Error())
		return
	}
	catalog, err := s.getShardedSnapshotCatalog(ctx, tenantID, manifest.SnapshotCatalogKey)
	if errors.Is(err, ErrNotFound) {
		health.Issues = append(health.Issues, "snapshot catalog is missing")
		return
	}
	if err != nil {
		health.Issues = append(health.Issues, "snapshot catalog decode failed: "+err.Error())
		return
	}
	health.SnapshotCatalogVersion = catalog.Version
	if catalog.Format != snapshotFormatParquetSharded {
		health.Issues = append(health.Issues, "snapshot catalog format is not parquet-sharded")
	}
	if catalog.Key != "" && catalog.Key != manifest.SnapshotCatalogKey {
		health.Issues = append(health.Issues, "snapshot catalog key mismatch")
	}
	if catalog.Version != manifest.SnapshotVersion {
		health.Issues = append(health.Issues, "snapshot catalog version does not match manifest snapshot")
	}
	if catalog.Version > manifest.Version {
		health.Issues = append(health.Issues, "snapshot catalog version is ahead of manifest")
	}
	s.checkSnapshotSchemaObject(ctx, tenantID, catalog, health)
	for _, spec := range catalog.EntityPages {
		s.checkSnapshotEntityPageObject(ctx, tenantID, catalog, spec, health)
	}
	for _, spec := range catalog.EdgeShards {
		s.checkSnapshotEdgeShardObject(ctx, tenantID, catalog, spec, health)
	}
}

func (s *TenantStore) checkSnapshotSchemaObject(ctx context.Context, tenantID string, catalog ShardedSnapshotCatalog, health *IndexHealth) {
	if catalog.Schema.Key == "" {
		health.Issues = append(health.Issues, "snapshot schema key is missing")
		return
	}
	if catalog.Schema.Format != snapshotSchemaFormatParquet {
		health.Issues = append(health.Issues, "snapshot schema is not parquet")
	}
	if catalog.Schema.ContentHash == "" {
		health.Issues = append(health.Issues, "snapshot schema content hash missing")
	}
	if err := s.validateTenantObjectKey(tenantID, catalog.Schema.Key); err != nil {
		health.Issues = append(health.Issues, err.Error())
		return
	}
	data, err := s.Objects.Get(ctx, catalog.Schema.Key)
	if errors.Is(err, ErrNotFound) {
		health.Issues = append(health.Issues, "snapshot schema is missing")
		return
	}
	if err != nil {
		health.Issues = append(health.Issues, err.Error())
		return
	}
	schemaData, err := decodeSnapshotSchemaObject(ctx, data)
	if err != nil {
		health.Issues = append(health.Issues, "snapshot schema decode failed: "+err.Error())
		return
	}
	if schemaData.TenantID != "" && schemaData.TenantID != tenantID {
		health.Issues = append(health.Issues, "snapshot schema tenant mismatch")
	}
	if schemaData.Version != catalog.Version {
		health.Issues = append(health.Issues, "snapshot schema version mismatch")
	}
	if catalog.Schema.ContentHash != "" && snapshotSchemaContentHash(schemaData) != catalog.Schema.ContentHash {
		health.Issues = append(health.Issues, "snapshot schema content hash mismatch")
	}
}

func (s *TenantStore) checkSnapshotEntityPageObject(ctx context.Context, tenantID string, catalog ShardedSnapshotCatalog, spec SnapshotEntityPageSpec, health *IndexHealth) {
	label := "snapshot entity page " + spec.Shard
	if spec.Format != IndexFormatParquet {
		health.Issues = append(health.Issues, label+" is not parquet")
		return
	}
	if spec.ContentHash == "" {
		health.Issues = append(health.Issues, label+" content hash missing")
	}
	if err := s.validateTenantObjectKey(tenantID, spec.Key); err != nil {
		health.Issues = append(health.Issues, err.Error())
		return
	}
	page, err := s.loadSnapshotEntityPage(ctx, tenantID, catalog.Version, spec)
	if errors.Is(err, ErrNotFound) {
		health.Issues = append(health.Issues, label+" is missing")
		return
	}
	if err != nil {
		health.Issues = append(health.Issues, label+" decode failed: "+err.Error())
		return
	}
	if !indexTenantMatches(page.TenantID, tenantID) {
		health.Issues = append(health.Issues, label+" tenant mismatch")
	}
	if page.Version != catalog.Version {
		health.Issues = append(health.Issues, label+" version mismatch")
	}
	if page.Shard != spec.Shard {
		health.Issues = append(health.Issues, label+" shard mismatch")
	}
	if len(page.Entities) != spec.EntityCount {
		health.Issues = append(health.Issues, label+" count mismatch")
	}
	if spec.ContentHash != "" && entityPageContentHash(page) != spec.ContentHash {
		health.Issues = append(health.Issues, label+" content hash mismatch")
	}
}

func (s *TenantStore) checkSnapshotEdgeShardObject(ctx context.Context, tenantID string, catalog ShardedSnapshotCatalog, spec SnapshotEdgeShardSpec, health *IndexHealth) {
	label := "snapshot edge shard " + spec.RelationType + "/" + spec.Shard
	if spec.Format != IndexFormatParquet {
		health.Issues = append(health.Issues, label+" is not parquet")
		return
	}
	if spec.ContentHash == "" {
		health.Issues = append(health.Issues, label+" content hash missing")
	}
	if err := s.validateTenantObjectKey(tenantID, spec.Key); err != nil {
		health.Issues = append(health.Issues, err.Error())
		return
	}
	shard, err := s.loadSnapshotEdgeShard(ctx, tenantID, catalog.Version, spec)
	if errors.Is(err, ErrNotFound) {
		health.Issues = append(health.Issues, label+" is missing")
		return
	}
	if err != nil {
		health.Issues = append(health.Issues, label+" decode failed: "+err.Error())
		return
	}
	if !indexTenantMatches(shard.TenantID, tenantID) {
		health.Issues = append(health.Issues, label+" tenant mismatch")
	}
	if shard.Version != catalog.Version {
		health.Issues = append(health.Issues, label+" version mismatch")
	}
	if shard.RelationType != spec.RelationType || shard.Shard != spec.Shard {
		health.Issues = append(health.Issues, label+" metadata mismatch")
	}
	if len(shard.Edges) != spec.EdgeCount {
		health.Issues = append(health.Issues, label+" count mismatch")
	}
	if spec.ContentHash != "" && edgeShardContentHash(shard) != spec.ContentHash {
		health.Issues = append(health.Issues, label+" content hash mismatch")
	}
}

func hasBlockingIndexHealthIssue(issues []string) bool {
	for _, issue := range issues {
		if !strings.HasPrefix(issue, "orphan index object ") {
			return true
		}
	}
	return false
}

func (s *TenantStore) checkFieldIndexObjects(ctx context.Context, tenantID string, catalog IndexCatalog, g *graph.Graph, definitions []IndexDefinition, health *IndexHealth) {
	for _, indexSpec := range catalog.Indexes {
		index, ok := s.loadFieldIndexForHealth(ctx, tenantID, indexSpec, health)
		if !ok {
			continue
		}
		if !indexTenantMatches(index.TenantID, tenantID) {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" tenant mismatch")
			continue
		}
		if index.Version > catalog.Version {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" version is ahead of catalog")
		}
		if indexSpec.ContentHash == "" {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" content hash missing")
		} else if secondaryIndexContentHash(index) != indexSpec.ContentHash {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" content hash mismatch")
		}
		if index.Kind != indexSpec.Kind || index.Field != indexSpec.Field {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" metadata mismatch")
		}
		entryCount, distinctValues := secondaryIndexCounts(index)
		if entryCount != indexSpec.EntryCount {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" entry count mismatch")
		}
		if distinctValues != indexSpec.DistinctValues {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" distinct value count mismatch")
		}
		if !indexTopValuesMatchCatalog(index, indexSpec.TopValues) {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" top values mismatch")
		}
		expected := SecondaryIndex{Kind: indexSpec.Kind, Field: indexSpec.Field, Values: map[string][]string{}}
		for _, definition := range definitions {
			if definition.Kind == indexSpec.Kind && definition.Field == indexSpec.Field {
				expected.Unique = definition.Unique
			}
		}
		addEntitiesToIndex(g, &expected)
		if !reflect.DeepEqual(normalizeSecondaryIndexValues(index.Values), normalizeSecondaryIndexValues(expected.Values)) {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" content mismatch")
		}
	}
}

func (s *TenantStore) loadFieldIndexForHealth(ctx context.Context, tenantID string, indexSpec IndexSpec, health *IndexHealth) (SecondaryIndex, bool) {
	if specFormat(indexSpec.Format) == IndexFormatParquet {
		index, ok, err := s.loadParquetSecondaryIndexObject(ctx, tenantID, 0, indexSpec)
		if err != nil {
			health.Issues = append(health.Issues, "field index "+indexSpec.Name+" parquet decode failed: "+err.Error())
			return SecondaryIndex{}, false
		}
		if !ok {
			health.Issues = append(health.Issues, "missing field index "+indexSpec.Name)
			return SecondaryIndex{}, false
		}
		return index, true
	}
	health.Issues = append(health.Issues, "field index "+indexSpec.Name+" is not parquet")
	return SecondaryIndex{}, false
}

func normalizeSecondaryIndexValues(values map[string][]string) map[string][]string {
	out := make(map[string][]string, len(values))
	for value, ids := range values {
		if len(ids) == 0 {
			continue
		}
		copied := append([]string(nil), ids...)
		sort.Strings(copied)
		out[value] = copied
	}
	return out
}

func (s *TenantStore) checkEdgeShardObjects(ctx context.Context, tenantID string, catalog IndexCatalog, g *graph.Graph, health *IndexHealth) {
	for _, shardSpec := range catalog.EdgeShards {
		shard, ok := s.loadEdgeShardForHealth(ctx, tenantID, shardSpec, health)
		if !ok {
			continue
		}
		if !indexTenantMatches(shard.TenantID, tenantID) {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" tenant mismatch")
			continue
		}
		if shard.Version > catalog.Version {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" version is ahead of catalog")
		}
		if shardSpec.ContentHash == "" {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" content hash missing")
		} else if edgeShardContentHash(shard) != shardSpec.ContentHash {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" content hash mismatch")
		}
		if shard.RelationType != shardSpec.RelationType || shard.Shard != shardSpec.Shard {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" metadata mismatch")
		}
		if len(shard.Edges) != shardSpec.EdgeCount {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" count mismatch")
		}
		if !reflect.DeepEqual(normalizeGraphEdges(shard.Edges), normalizeGraphEdges(expectedShardEdges(g, shardSpec.RelationType, shardSpec.Shard))) {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" content mismatch")
		}
	}
}

func (s *TenantStore) loadEdgeShardForHealth(ctx context.Context, tenantID string, shardSpec EdgeShard, health *IndexHealth) (EdgeShardData, bool) {
	if specFormat(shardSpec.Format) == IndexFormatParquet {
		shard, ok, err := s.loadParquetEdgeShardObject(ctx, tenantID, 0, shardSpec)
		if err != nil {
			health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" parquet decode failed: "+err.Error())
			return EdgeShardData{}, false
		}
		if !ok {
			health.Issues = append(health.Issues, "missing edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard)
			return EdgeShardData{}, false
		}
		return shard, true
	}
	health.Issues = append(health.Issues, "edge shard "+shardSpec.RelationType+"/"+shardSpec.Shard+" is not parquet")
	return EdgeShardData{}, false
}

func expectedShardEdges(g *graph.Graph, relationType string, shardID string) []graph.Edge {
	edges := make([]graph.Edge, 0)
	for _, edge := range g.Edges {
		if edge.Type == relationType && indexShardIDMatches(edge.From, shardID) {
			edges = append(edges, edge)
		}
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	return edges
}

func normalizeGraphEdges(edges []graph.Edge) []graph.Edge {
	data, err := json.Marshal(edges)
	if err != nil {
		return edges
	}
	var normalized []graph.Edge
	if err := json.Unmarshal(data, &normalized); err != nil {
		return edges
	}
	if normalized == nil {
		return []graph.Edge{}
	}
	return normalized
}

func normalizeGraphEntities(entities []graph.Entity) []graph.Entity {
	data, err := json.Marshal(entities)
	if err != nil {
		return entities
	}
	var normalized []graph.Entity
	if err := json.Unmarshal(data, &normalized); err != nil {
		return entities
	}
	if normalized == nil {
		return []graph.Entity{}
	}
	return normalized
}

func (s *TenantStore) checkEntityPageObjects(ctx context.Context, tenantID string, catalog IndexCatalog, g *graph.Graph, health *IndexHealth) {
	expectedRecords := map[string]entityRecordExpectation{}
	checkRecords := false
	for _, pageSpec := range catalog.EntityPages {
		page, pageETag, ok := s.loadEntityPageForHealth(ctx, tenantID, pageSpec, catalog.Version, health)
		if !ok {
			continue
		}
		if !indexTenantMatches(page.TenantID, tenantID) {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" tenant mismatch")
			continue
		}
		if page.Version > catalog.Version {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" version is ahead of catalog")
		}
		if pageSpec.ContentHash == "" {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" content hash missing")
		} else if entityPageContentHash(page) != pageSpec.ContentHash {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" content hash mismatch")
		}
		if page.Shard != pageSpec.Shard {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" metadata mismatch")
		}
		if len(page.Entities) != pageSpec.EntityCount {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" count mismatch")
		}
		if !reflect.DeepEqual(normalizeGraphEntities(page.Entities), normalizeGraphEntities(expectedPageEntities(g, pageSpec.Shard))) {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" content mismatch")
		}
		checkRecords = true
		pageHash := entityPageContentHash(page)
		for _, entity := range page.Entities {
			expectedRecords[entity.ID] = entityRecordExpectation{Entity: entity, Page: page.Shard, PageHash: pageHash, PageETag: pageETag}
		}
	}
	if checkRecords {
		s.checkEntityRecords(ctx, tenantID, catalog.Version, expectedRecords, health)
	}
}

type entityRecordExpectation struct {
	Entity   graph.Entity
	Page     string
	PageHash string
	PageETag string
}

func (s *TenantStore) loadEntityPageForHealth(ctx context.Context, tenantID string, pageSpec EntityPageSpec, version int64, health *IndexHealth) (EntityPageData, string, bool) {
	if specFormat(pageSpec.Format) == IndexFormatParquet {
		page, pageETag, ok, err := s.loadParquetEntityPageObject(ctx, tenantID, version, pageSpec)
		if err != nil {
			health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" parquet decode failed: "+err.Error())
			return EntityPageData{}, "", false
		}
		if !ok {
			health.Issues = append(health.Issues, "missing entity page "+pageSpec.Shard)
			return EntityPageData{}, "", false
		}
		return page, pageETag, true
	}
	health.Issues = append(health.Issues, "entity page "+pageSpec.Shard+" is not parquet")
	return EntityPageData{}, "", false
}

func expectedPageEntities(g *graph.Graph, shardID string) []graph.Entity {
	entities := make([]graph.Entity, 0)
	for _, entity := range g.Entities {
		if indexShardIDMatches(entity.ID, shardID) {
			entities = append(entities, entity)
		}
	}
	sort.Slice(entities, func(i, j int) bool { return entities[i].ID < entities[j].ID })
	return entities
}

func secondaryIndexCounts(index SecondaryIndex) (entryCount int, distinctValues int) {
	for _, ids := range index.Values {
		if len(ids) == 0 {
			continue
		}
		distinctValues++
		entryCount += len(ids)
	}
	return entryCount, distinctValues
}

func (s *TenantStore) checkEntityRecords(ctx context.Context, tenantID string, catalogVersion int64, expected map[string]entityRecordExpectation, health *IndexHealth) {
	objects, err := s.Objects.List(ctx, s.entityRecordPrefix(tenantID))
	if err != nil {
		health.Issues = append(health.Issues, err.Error())
		return
	}
	for _, object := range objects {
		entityID, ok, err := s.entityIDFromRecordKey(tenantID, object.Key)
		if err != nil {
			health.Issues = append(health.Issues, err.Error())
			continue
		}
		if !ok {
			continue
		}
		record, err := s.loadEntityRecordKey(ctx, tenantID, object.Key)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			if strings.Contains(err.Error(), "tenant mismatch") {
				health.Issues = append(health.Issues, "entity record "+entityID+" tenant mismatch")
				continue
			}
			health.Issues = append(health.Issues, "invalid entity record "+object.Key)
			continue
		}
		if !indexTenantMatches(record.TenantID, tenantID) {
			health.Issues = append(health.Issues, "entity record "+object.Key+" tenant mismatch")
			continue
		}
		expectedRecord, live := expected[record.ID]
		if record.Deleted {
			if live {
				health.Issues = append(health.Issues, "entity record "+record.ID+" is deleted")
			}
			continue
		}
		if !live {
			health.Issues = append(health.Issues, "stale entity record "+record.ID)
			continue
		}
		checkEntityRecordContent(record, expectedRecord, catalogVersion, health)
	}
}

func checkEntityRecordContent(record EntityRecord, expected entityRecordExpectation, catalogVersion int64, health *IndexHealth) {
	entity := expected.Entity
	if record.Page != expected.Page {
		health.Issues = append(health.Issues, "entity record "+entity.ID+" page mismatch")
	}
	if record.PageHash != "" && record.PageHash != expected.PageHash {
		health.Issues = append(health.Issues, "entity record "+entity.ID+" page hash mismatch")
	}
	if record.PageETag != "" && expected.PageETag != "" && record.PageETag != expected.PageETag {
		health.Issues = append(health.Issues, "entity record "+entity.ID+" page etag mismatch")
	}
	if record.ContentHash != "" && entityRecordContentHash(record) != record.ContentHash {
		health.Issues = append(health.Issues, "entity record "+entity.ID+" content hash mismatch")
	}
	if record.ID != entity.ID || record.Entity.ID != entity.ID {
		health.Issues = append(health.Issues, "entity record "+entity.ID+" metadata mismatch")
	}
	if !reflect.DeepEqual(record.Entity, entity) {
		health.Issues = append(health.Issues, "entity record "+entity.ID+" content mismatch")
	}
	if record.Version > catalogVersion {
		health.Issues = append(health.Issues, "entity record "+entity.ID+" version is ahead of catalog")
	}
}
