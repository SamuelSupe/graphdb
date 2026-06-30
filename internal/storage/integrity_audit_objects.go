package storage

import (
	"context"
	"errors"
	"reflect"
	"strings"
)

func (s *TenantStore) auditSnapshotCatalog(ctx context.Context, tenantID string, manifest Manifest, report *IntegrityAuditReport) {
	if manifest.SnapshotCatalogKey == "" {
		report.addIssue("snapshot_catalog_missing", "error", "snapshot_catalog", "", "", "manifest does not point to a snapshot catalog")
		return
	}
	data, _, ok := s.auditReadObject(ctx, manifest.SnapshotCatalogKey, "snapshot_catalog", true, report)
	if !ok {
		return
	}
	catalog, err := decodeParquetShardedSnapshotCatalog(ctx, data)
	if err != nil {
		report.addIssue("snapshot_catalog_decode_failed", "error", "snapshot_catalog", "", manifest.SnapshotCatalogKey, err.Error())
		return
	}
	report.updateLastCheck(1, shardedSnapshotCatalogContentHash(catalog), parquetSnapshotCatalogSchemaHash())
	if catalog.TenantID != "" && catalog.TenantID != tenantID {
		report.addIssue("snapshot_catalog_tenant_mismatch", "error", "snapshot_catalog", "", manifest.SnapshotCatalogKey, "snapshot catalog tenant does not match")
	}
	if catalog.Version != manifest.SnapshotVersion {
		report.addIssue("snapshot_catalog_version_mismatch", "error", "snapshot_catalog", "", manifest.SnapshotCatalogKey, "snapshot catalog version does not match manifest")
	}
	s.auditSnapshotSchema(ctx, tenantID, catalog, report)
	for _, spec := range catalog.EntityPages {
		s.auditSnapshotEntityPage(ctx, tenantID, catalog.Version, spec, report)
	}
	for _, spec := range catalog.EdgeShards {
		s.auditSnapshotEdgeShard(ctx, tenantID, catalog.Version, spec, report)
	}
}

func (s *TenantStore) auditSnapshotSchema(ctx context.Context, tenantID string, catalog ShardedSnapshotCatalog, report *IntegrityAuditReport) {
	data, _, ok := s.auditReadObject(ctx, catalog.Schema.Key, "snapshot_schema", true, report)
	if !ok {
		return
	}
	schema, err := decodeParquetSnapshotSchema(ctx, data)
	if err != nil {
		report.addIssue("snapshot_schema_decode_failed", "error", "snapshot_schema", "", catalog.Schema.Key, err.Error())
		return
	}
	contentHash := snapshotSchemaContentHash(schema)
	report.updateLastCheck(1, contentHash, "")
	if schema.TenantID != "" && schema.TenantID != tenantID {
		report.addIssue("snapshot_schema_tenant_mismatch", "error", "snapshot_schema", "", catalog.Schema.Key, "snapshot schema tenant does not match")
	}
	if schema.Version != catalog.Version {
		report.addIssue("snapshot_schema_version_mismatch", "error", "snapshot_schema", "", catalog.Schema.Key, "snapshot schema version does not match catalog")
	}
	if catalog.Schema.ContentHash != "" && contentHash != catalog.Schema.ContentHash {
		report.addIssue("snapshot_schema_content_hash_mismatch", "error", "snapshot_schema", "", catalog.Schema.Key, "snapshot schema content hash does not match catalog")
	}
}

func (s *TenantStore) auditSnapshotEntityPage(ctx context.Context, tenantID string, version int64, spec SnapshotEntityPageSpec, report *IntegrityAuditReport) {
	resourceID := spec.Shard
	data, _, ok := s.auditReadObject(ctx, spec.Key, "snapshot_entity_page", true, report)
	if !ok {
		return
	}
	page, err := decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, version)
	if err != nil {
		report.addIssue("snapshot_entity_page_decode_failed", "error", "snapshot_entity_page", resourceID, spec.Key, err.Error())
		return
	}
	contentHash := entityPageContentHash(page)
	report.updateLastCheck(len(page.Entities), contentHash, parquetEntityPageSchemaHash())
	if page.Version != version || page.Shard != spec.Shard {
		report.addIssue("snapshot_entity_page_metadata_mismatch", "error", "snapshot_entity_page", resourceID, spec.Key, "snapshot entity page metadata does not match catalog")
	}
	if len(page.Entities) != spec.EntityCount {
		report.addIssue("snapshot_entity_page_row_count_mismatch", "error", "snapshot_entity_page", resourceID, spec.Key, "snapshot entity page row count does not match catalog")
	}
	if spec.ContentHash != "" && contentHash != spec.ContentHash {
		report.addIssue("snapshot_entity_page_content_hash_mismatch", "error", "snapshot_entity_page", resourceID, spec.Key, "snapshot entity page content hash does not match catalog")
	}
}

func (s *TenantStore) auditSnapshotEdgeShard(ctx context.Context, tenantID string, version int64, spec SnapshotEdgeShardSpec, report *IntegrityAuditReport) {
	resourceID := spec.RelationType + "/" + spec.Shard
	data, _, ok := s.auditReadObject(ctx, spec.Key, "snapshot_edge_shard", true, report)
	if !ok {
		return
	}
	shard, err := decodeParquetEdgeShard(ctx, data, tenantID, spec.RelationType, spec.Shard, version)
	if err != nil {
		report.addIssue("snapshot_edge_shard_decode_failed", "error", "snapshot_edge_shard", resourceID, spec.Key, err.Error())
		return
	}
	contentHash := edgeShardContentHash(shard)
	report.updateLastCheck(len(shard.Edges), contentHash, parquetEdgeShardSchemaHash())
	if shard.Version != version || shard.RelationType != spec.RelationType || shard.Shard != spec.Shard {
		report.addIssue("snapshot_edge_shard_metadata_mismatch", "error", "snapshot_edge_shard", resourceID, spec.Key, "snapshot edge shard metadata does not match catalog")
	}
	if len(shard.Edges) != spec.EdgeCount {
		report.addIssue("snapshot_edge_shard_row_count_mismatch", "error", "snapshot_edge_shard", resourceID, spec.Key, "snapshot edge shard row count does not match catalog")
	}
	if spec.ContentHash != "" && contentHash != spec.ContentHash {
		report.addIssue("snapshot_edge_shard_content_hash_mismatch", "error", "snapshot_edge_shard", resourceID, spec.Key, "snapshot edge shard content hash does not match catalog")
	}
}

func (s *TenantStore) auditIndexCatalog(ctx context.Context, tenantID string, manifest Manifest, options IntegrityAuditOptions, report *IntegrityAuditReport) {
	key := s.indexCatalogKey(tenantID)
	data, _, ok := s.auditReadObject(ctx, key, "index_catalog", true, report)
	if !ok {
		return
	}
	catalog, err := decodeIndexCatalogObject(ctx, data)
	if err != nil {
		report.addIssue("index_catalog_decode_failed", "error", "index_catalog", "", key, err.Error())
		return
	}
	report.IndexCatalogVersion = catalog.Version
	contentHash, err := indexCatalogContentHash(catalog)
	if err != nil {
		report.addIssue("index_catalog_content_hash_failed", "error", "index_catalog", "", key, err.Error())
	} else {
		report.updateLastCheck(1, contentHash, parquetIndexCatalogSchemaHash())
	}
	if catalog.TenantID != "" && catalog.TenantID != tenantID {
		report.addIssue("index_catalog_tenant_mismatch", "error", "index_catalog", "", key, "index catalog tenant does not match")
	}
	if catalog.Version != manifest.Version {
		report.addIssue("index_stale", "error", "index_catalog", "", key, "index catalog version does not match manifest")
	}
	if !options.Deep {
		return
	}
	expectedRecords := map[string]entityRecordExpectation{}
	for _, index := range catalog.Indexes {
		s.auditSecondaryIndexObjects(ctx, tenantID, catalog.Version, index, report)
	}
	for _, shard := range catalog.EdgeShards {
		s.auditIndexEdgeShardObjects(ctx, tenantID, catalog.Version, shard, report)
	}
	for _, page := range catalog.EntityPages {
		s.auditIndexEntityPageObjects(ctx, tenantID, catalog.Version, page, expectedRecords, report)
	}
	s.auditEntityRecords(ctx, tenantID, catalog.Version, expectedRecords, report)
}

func (s *TenantStore) auditSecondaryIndexObjects(ctx context.Context, tenantID string, version int64, spec IndexSpec, report *IntegrityAuditReport) {
	for _, object := range spec.Objects {
		data, _, ok := s.auditReadObject(ctx, object.Key, "secondary_index", true, report)
		if !ok {
			continue
		}
		index, err := decodeParquetSecondaryIndex(ctx, data, tenantID, spec.Kind, spec.Field, version, spec.Type == "unique")
		if err != nil {
			report.addIssue("secondary_index_decode_failed", "error", "secondary_index", spec.Name, object.Key, err.Error())
			continue
		}
		contentHash := secondaryIndexContentHash(index)
		report.updateLastCheck(secondaryIndexEntryCount(index), contentHash, parquetSecondaryIndexSchemaHash())
		if object.RowCount > 0 && secondaryIndexEntryCount(index) != object.RowCount {
			report.addIssue("secondary_index_row_count_mismatch", "error", "secondary_index", spec.Name, object.Key, "secondary index row count does not match catalog object")
		}
		if object.ContentHash != "" && contentHash != object.ContentHash {
			report.addIssue("secondary_index_content_hash_mismatch", "error", "secondary_index", spec.Name, object.Key, "secondary index content hash does not match catalog object")
		}
		if object.SchemaHash != "" && object.SchemaHash != parquetSecondaryIndexSchemaHash() {
			report.addIssue("secondary_index_schema_hash_mismatch", "error", "secondary_index", spec.Name, object.Key, "secondary index schema hash does not match reader schema")
		}
	}
}

func (s *TenantStore) auditIndexEdgeShardObjects(ctx context.Context, tenantID string, version int64, spec EdgeShard, report *IntegrityAuditReport) {
	resourceID := spec.RelationType + "/" + spec.Shard
	for _, object := range spec.Objects {
		data, _, ok := s.auditReadObject(ctx, object.Key, "edge_shard", true, report)
		if !ok {
			continue
		}
		shard, err := decodeParquetEdgeShard(ctx, data, tenantID, spec.RelationType, spec.Shard, version)
		if err != nil {
			report.addIssue("edge_shard_decode_failed", "error", "edge_shard", resourceID, object.Key, err.Error())
			continue
		}
		contentHash := edgeShardContentHash(shard)
		report.updateLastCheck(len(shard.Edges), contentHash, parquetEdgeShardSchemaHash())
		if len(shard.Edges) != spec.EdgeCount {
			report.addIssue("edge_shard_row_count_mismatch", "error", "edge_shard", resourceID, object.Key, "edge shard row count does not match catalog")
		}
		if object.ContentHash != "" && contentHash != object.ContentHash {
			report.addIssue("edge_shard_content_hash_mismatch", "error", "edge_shard", resourceID, object.Key, "edge shard content hash does not match catalog object")
		}
	}
}

func (s *TenantStore) auditIndexEntityPageObjects(ctx context.Context, tenantID string, version int64, spec EntityPageSpec, expected map[string]entityRecordExpectation, report *IntegrityAuditReport) {
	for _, object := range spec.Objects {
		data, meta, ok := s.auditReadObject(ctx, object.Key, "entity_page", true, report)
		if !ok {
			continue
		}
		page, err := decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, version)
		if err != nil {
			report.addIssue("entity_page_decode_failed", "error", "entity_page", spec.Shard, object.Key, err.Error())
			continue
		}
		contentHash := entityPageContentHash(page)
		report.updateLastCheck(len(page.Entities), contentHash, parquetEntityPageSchemaHash())
		if len(page.Entities) != spec.EntityCount {
			report.addIssue("entity_page_row_count_mismatch", "error", "entity_page", spec.Shard, object.Key, "entity page row count does not match catalog")
		}
		if object.ContentHash != "" && contentHash != object.ContentHash {
			report.addIssue("entity_page_content_hash_mismatch", "error", "entity_page", spec.Shard, object.Key, "entity page content hash does not match catalog object")
		}
		for _, entity := range page.Entities {
			expected[entity.ID] = entityRecordExpectation{Entity: entity, Page: page.Shard, PageHash: contentHash, PageETag: meta.ETag}
		}
	}
}

func (s *TenantStore) auditEntityRecords(ctx context.Context, tenantID string, catalogVersion int64, expected map[string]entityRecordExpectation, report *IntegrityAuditReport) {
	objects, err := s.Objects.List(ctx, s.entityRecordPrefix(tenantID))
	if err != nil {
		report.addIssue("entity_record_list_failed", "error", "entity_record", "", s.entityRecordPrefix(tenantID), err.Error())
		return
	}
	seen := map[string]bool{}
	for _, object := range objects {
		entityID, ok, err := s.entityIDFromRecordKey(tenantID, object.Key)
		if err != nil {
			report.addIssue("entity_record_key_invalid", "error", "entity_record", "", object.Key, err.Error())
			continue
		}
		if !ok {
			continue
		}
		record, err := s.loadEntityRecordKey(ctx, tenantID, object.Key)
		if errors.Is(err, ErrNotFound) {
			report.addIssue("entity_record_missing", "error", "entity_record", entityID, object.Key, err.Error())
			continue
		}
		if err != nil {
			report.addIssue("entity_record_decode_failed", "error", "entity_record", entityID, object.Key, err.Error())
			continue
		}
		report.Objects++
		report.Bytes += object.Size
		report.Checks = append(report.Checks, IntegrityObjectCheck{
			Role:        "entity_record",
			Key:         object.Key,
			Status:      "ok",
			Bytes:       object.Size,
			RowCount:    1,
			ContentHash: entityRecordContentHash(record),
			SchemaHash:  parquetEntityRecordSchemaHash(),
		})
		seen[record.ID] = true
		s.auditEntityRecordContent(record, object.Key, catalogVersion, expected, report)
	}
	for entityID := range expected {
		if !seen[entityID] {
			report.addIssue("entity_record_missing", "error", "entity_record", entityID, s.entityRecordKey(tenantID, entityID), "entity page has no by-id record")
		}
	}
}

func (s *TenantStore) auditEntityRecordContent(record EntityRecord, key string, catalogVersion int64, expected map[string]entityRecordExpectation, report *IntegrityAuditReport) {
	if record.ContentHash != "" && entityRecordContentHash(record) != record.ContentHash {
		report.addIssue("entity_record_content_hash_mismatch", "error", "entity_record", record.ID, key, "entity record content hash does not match record")
	}
	if !indexTenantMatches(record.TenantID, report.TenantID) {
		report.addIssue("entity_record_tenant_mismatch", "error", "entity_record", record.ID, key, "entity record tenant does not match")
	}
	if record.Version > catalogVersion {
		report.addIssue("entity_record_version_ahead", "error", "entity_record", record.ID, key, "entity record version is ahead of index catalog")
	}
	expectedRecord, ok := expected[record.ID]
	if record.Deleted {
		if ok {
			report.addIssue("entity_record_deleted_but_indexed", "error", "entity_record", record.ID, key, "entity record is deleted but entity page contains it")
		}
		return
	}
	if !ok {
		report.addIssue("entity_record_stale", "error", "entity_record", record.ID, key, "entity record is not present in current entity pages")
		return
	}
	if record.Page != expectedRecord.Page {
		report.addIssue("entity_record_page_mismatch", "error", "entity_record", record.ID, key, "entity record page does not match entity page")
	}
	if record.PageHash != "" && record.PageHash != expectedRecord.PageHash {
		report.addIssue("entity_record_page_hash_mismatch", "error", "entity_record", record.ID, key, "entity record page hash does not match entity page")
	}
	if record.PageETag != "" && expectedRecord.PageETag != "" && record.PageETag != expectedRecord.PageETag {
		report.addIssue("entity_record_page_etag_mismatch", "error", "entity_record", record.ID, key, "entity record page etag does not match entity page")
	}
	if !reflect.DeepEqual(record.Entity, expectedRecord.Entity) || strings.TrimSpace(record.Entity.ID) != record.ID {
		report.addIssue("entity_record_content_mismatch", "error", "entity_record", record.ID, key, "entity record content does not match entity page")
	}
}
