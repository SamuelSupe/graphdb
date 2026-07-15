package storage

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
)

const (
	snapshotFormatParquetSharded = "parquet-sharded"
	snapshotSchemaFormatParquet  = "parquet"
)

type ShardedSnapshotCatalog struct {
	LayoutVersion int                      `json:"layout_version,omitempty"`
	TenantID      string                   `json:"tenant_id,omitempty"`
	Key           string                   `json:"key,omitempty"`
	Version       int64                    `json:"version"`
	Format        string                   `json:"format"`
	Schema        SnapshotSchemaSpec       `json:"schema"`
	EntityPages   []SnapshotEntityPageSpec `json:"entity_pages,omitempty"`
	EdgeShards    []SnapshotEdgeShardSpec  `json:"edge_shards,omitempty"`
	UpdatedAt     time.Time                `json:"updated_at"`
}

type SnapshotSchemaSpec struct {
	Key         string `json:"key"`
	Format      string `json:"format"`
	ContentHash string `json:"content_hash,omitempty"`
}

type SnapshotEntityPageSpec struct {
	Shard       string `json:"shard"`
	Key         string `json:"key"`
	Format      string `json:"format"`
	EntityCount int    `json:"entity_count"`
	ContentHash string `json:"content_hash,omitempty"`
}

type SnapshotEdgeShardSpec struct {
	RelationType string `json:"relation_type"`
	Shard        string `json:"shard"`
	Key          string `json:"key"`
	Format       string `json:"format"`
	EdgeCount    int    `json:"edge_count"`
	ContentHash  string `json:"content_hash,omitempty"`
}

type snapshotSchemaData struct {
	LayoutVersion int                  `json:"layout_version,omitempty"`
	TenantID      string               `json:"tenant_id,omitempty"`
	Version       int64                `json:"version"`
	CITypes       []graph.CIType       `json:"ci_types,omitempty"`
	RelationTypes []graph.RelationType `json:"relation_types,omitempty"`
	UpdatedAt     time.Time            `json:"updated_at"`
}

func (s *TenantStore) putShardedSnapshot(ctx context.Context, tenantID string, snapshot graph.Snapshot) (ShardedSnapshotCatalog, error) {
	updatedAt := stableSnapshotUpdatedAt(snapshot)
	schema := snapshotSchemaData{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      tenantID,
		Version:       snapshot.Version,
		CITypes:       append([]graph.CIType(nil), snapshot.CITypes...),
		RelationTypes: append([]graph.RelationType(nil), snapshot.RelationTypes...),
		UpdatedAt:     updatedAt,
	}
	schemaKey := s.snapshotSchemaKey(tenantID, snapshot.Version)
	if err := s.putSnapshotSchemaIfAbsentOrSame(ctx, schemaKey, schema); err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	catalog := ShardedSnapshotCatalog{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      tenantID,
		Key:           s.snapshotCatalogKey(tenantID, snapshot.Version),
		Version:       snapshot.Version,
		Format:        snapshotFormatParquetSharded,
		Schema:        SnapshotSchemaSpec{Key: schemaKey, Format: snapshotSchemaFormatParquet, ContentHash: snapshotSchemaContentHash(schema)},
		UpdatedAt:     updatedAt,
	}
	for _, page := range buildEntityPagesFromEntities(snapshot.Entities, snapshot.Version) {
		page.TenantID = tenantID
		page.UpdatedAt = updatedAt
		key := s.snapshotEntityPageKey(tenantID, snapshot.Version, page.Shard)
		if err := s.putSnapshotParquetEntityPageIfAbsentOrSame(ctx, key, tenantID, page); err != nil {
			return ShardedSnapshotCatalog{}, err
		}
		catalog.EntityPages = append(catalog.EntityPages, SnapshotEntityPageSpec{
			Shard:       page.Shard,
			Key:         key,
			Format:      IndexFormatParquet,
			EntityCount: len(page.Entities),
			ContentHash: entityPageContentHash(page),
		})
	}
	for _, shard := range buildEdgeShardsFromEdges(snapshot.Edges, snapshot.Version) {
		shard.TenantID = tenantID
		shard.UpdatedAt = updatedAt
		key := s.snapshotEdgeShardKey(tenantID, snapshot.Version, shard.RelationType, shard.Shard)
		if err := s.putSnapshotParquetEdgeShardIfAbsentOrSame(ctx, key, tenantID, shard); err != nil {
			return ShardedSnapshotCatalog{}, err
		}
		catalog.EdgeShards = append(catalog.EdgeShards, SnapshotEdgeShardSpec{
			RelationType: shard.RelationType,
			Shard:        shard.Shard,
			Key:          key,
			Format:       IndexFormatParquet,
			EdgeCount:    len(shard.Edges),
			ContentHash:  edgeShardContentHash(shard),
		})
	}
	if err := s.putShardedSnapshotCatalogIfAbsentOrSame(ctx, catalog.Key, catalog); err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	return catalog, nil
}

func stableSnapshotUpdatedAt(snapshot graph.Snapshot) time.Time {
	var out time.Time
	for _, entity := range snapshot.Entities {
		if entity.UpdatedAt.After(out) {
			out = entity.UpdatedAt
		}
	}
	for _, edge := range snapshot.Edges {
		if edge.UpdatedAt.After(out) {
			out = edge.UpdatedAt
		}
	}
	return out.UTC()
}

func (s *TenantStore) getShardedSnapshotCatalog(ctx context.Context, tenantID string, key string) (ShardedSnapshotCatalog, error) {
	if err := s.validateTenantObjectKey(tenantID, key); err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	data, err := s.Objects.Get(ctx, key)
	if err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	catalog, err := decodeShardedSnapshotCatalogObject(ctx, data)
	if err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	if catalog.TenantID != "" && catalog.TenantID != tenantID {
		return ShardedSnapshotCatalog{}, fmt.Errorf("snapshot catalog tenant mismatch: key tenant %q object %q contains tenant %q", tenantID, key, catalog.TenantID)
	}
	if catalog.TenantID == "" {
		catalog.TenantID = tenantID
	}
	if catalog.Key == "" {
		catalog.Key = key
	}
	if catalog.Version <= 0 {
		return ShardedSnapshotCatalog{}, ErrNotFound
	}
	return catalog, nil
}

func (s *TenantStore) CurrentShardedSnapshotCatalog(ctx context.Context, tenantID string) (catalog ShardedSnapshotCatalog, manifest Manifest, err error) {
	ctx, span := startStorageSpan(ctx, "graphdb.storage.snapshot.current_catalog", tenantTraceAttr(tenantID))
	defer func() {
		span.SetAttributes(
			attribute.Bool("graphdb.snapshot.catalog_available", err == nil),
			attribute.Int64("graphdb.snapshot.catalog_version", catalog.Version),
			attribute.Int64("graphdb.snapshot.manifest_version", manifest.Version),
			attribute.Int("graphdb.snapshot.entity_pages", len(catalog.EntityPages)),
			attribute.Int("graphdb.snapshot.edge_shards", len(catalog.EdgeShards)),
		)
		spanErr := err
		if errors.Is(err, ErrNotFound) {
			spanErr = nil
		}
		endStorageSpan(span, spanErr)
	}()
	if err := ValidateTenantID(tenantID); err != nil {
		return ShardedSnapshotCatalog{}, Manifest{}, err
	}
	manifest, _, err = s.getManifest(ctx, tenantID)
	if err != nil {
		return ShardedSnapshotCatalog{}, Manifest{}, err
	}
	if manifest.SnapshotCatalogKey == "" {
		return ShardedSnapshotCatalog{}, manifest, ErrNotFound
	}
	catalog, err = s.getShardedSnapshotCatalog(ctx, tenantID, manifest.SnapshotCatalogKey)
	if err != nil {
		return ShardedSnapshotCatalog{}, manifest, err
	}
	if catalog.Version != manifest.SnapshotVersion {
		return ShardedSnapshotCatalog{}, manifest, fmt.Errorf("snapshot catalog version mismatch: manifest snapshot version %d catalog version %d", manifest.SnapshotVersion, catalog.Version)
	}
	if catalog.Version > manifest.Version {
		return ShardedSnapshotCatalog{}, manifest, fmt.Errorf("snapshot catalog version %d is ahead of manifest version %d", catalog.Version, manifest.Version)
	}
	return catalog, manifest, nil
}

func (s *TenantStore) loadSnapshotFromCatalog(ctx context.Context, tenantID string, catalog ShardedSnapshotCatalog) (graph.Snapshot, error) {
	if err := s.validateTenantObjectKey(tenantID, catalog.Schema.Key); err != nil {
		return graph.Snapshot{}, err
	}
	data, err := s.Objects.Get(ctx, catalog.Schema.Key)
	if err != nil {
		return graph.Snapshot{}, err
	}
	schema, err := decodeSnapshotSchemaObject(ctx, data)
	if err != nil {
		return graph.Snapshot{}, err
	}
	if schema.TenantID != "" && schema.TenantID != tenantID {
		return graph.Snapshot{}, fmt.Errorf("snapshot schema tenant mismatch: key tenant %q object %q contains tenant %q", tenantID, catalog.Schema.Key, schema.TenantID)
	}
	snapshot := graph.Snapshot{
		Version:       catalog.Version,
		CITypes:       append([]graph.CIType(nil), schema.CITypes...),
		RelationTypes: append([]graph.RelationType(nil), schema.RelationTypes...),
	}
	for _, spec := range catalog.EntityPages {
		if err := s.validateTenantObjectKey(tenantID, spec.Key); err != nil {
			return graph.Snapshot{}, err
		}
		page, err := s.loadSnapshotEntityPage(ctx, tenantID, catalog.Version, spec)
		if err != nil {
			return graph.Snapshot{}, err
		}
		if !shardedEntityPageReadable(page, tenantID, catalog.Version, spec) {
			return graph.Snapshot{}, fmt.Errorf("snapshot entity page %q failed validation", spec.Key)
		}
		snapshot.Entities = append(snapshot.Entities, page.Entities...)
	}
	for _, spec := range catalog.EdgeShards {
		if err := s.validateTenantObjectKey(tenantID, spec.Key); err != nil {
			return graph.Snapshot{}, err
		}
		shard, err := s.loadSnapshotEdgeShard(ctx, tenantID, catalog.Version, spec)
		if err != nil {
			return graph.Snapshot{}, err
		}
		if !shardedEdgeShardReadable(shard, tenantID, catalog.Version, spec) {
			return graph.Snapshot{}, fmt.Errorf("snapshot edge shard %q failed validation", spec.Key)
		}
		snapshot.Edges = append(snapshot.Edges, shard.Edges...)
	}
	sort.Slice(snapshot.Entities, func(i, j int) bool { return snapshot.Entities[i].ID < snapshot.Entities[j].ID })
	sort.Slice(snapshot.Edges, func(i, j int) bool { return snapshot.Edges[i].ID < snapshot.Edges[j].ID })
	return snapshot, nil
}

func (s *TenantStore) loadSnapshotEntityPage(ctx context.Context, tenantID string, version int64, spec SnapshotEntityPageSpec) (EntityPageData, error) {
	if spec.Format != IndexFormatParquet {
		return EntityPageData{}, fmt.Errorf("unsupported snapshot entity page format %q: only parquet pages are readable", spec.Format)
	}
	data, err := s.Objects.Get(ctx, spec.Key)
	if err != nil {
		return EntityPageData{}, err
	}
	return decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, version)
}

func (s *TenantStore) loadSnapshotEdgeShard(ctx context.Context, tenantID string, version int64, spec SnapshotEdgeShardSpec) (EdgeShardData, error) {
	if spec.Format != IndexFormatParquet {
		return EdgeShardData{}, fmt.Errorf("unsupported snapshot edge shard format %q: only parquet shards are readable", spec.Format)
	}
	data, err := s.Objects.Get(ctx, spec.Key)
	if err != nil {
		return EdgeShardData{}, err
	}
	return decodeParquetEdgeShard(ctx, data, tenantID, spec.RelationType, spec.Shard, version)
}

func (s *TenantStore) putSnapshotParquetEntityPageIfAbsentOrSame(ctx context.Context, key string, tenantID string, page EntityPageData) error {
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		return err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	decoded, err := decodeParquetEntityPage(ctx, existing, tenantID, page.Shard, page.Version)
	if err != nil || entityPageContentHash(decoded) != entityPageContentHash(page) {
		return fmt.Errorf("%w: object %q already exists with different content", ErrConflict, key)
	}
	return nil
}

func (s *TenantStore) putSnapshotParquetEdgeShardIfAbsentOrSame(ctx context.Context, key string, tenantID string, shard EdgeShardData) error {
	data, err := marshalParquetEdgeShard(ctx, shard)
	if err != nil {
		return err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	decoded, err := decodeParquetEdgeShard(ctx, existing, tenantID, shard.RelationType, shard.Shard, shard.Version)
	if err != nil || edgeShardContentHash(decoded) != edgeShardContentHash(shard) {
		return fmt.Errorf("%w: object %q already exists with different content", ErrConflict, key)
	}
	return nil
}

func shardedEntityPageReadable(page EntityPageData, tenantID string, version int64, spec SnapshotEntityPageSpec) bool {
	return indexTenantMatches(page.TenantID, tenantID) &&
		page.Shard == spec.Shard &&
		page.Version == version &&
		spec.ContentHash != "" &&
		entityPageContentHash(page) == spec.ContentHash
}

func shardedEdgeShardReadable(shard EdgeShardData, tenantID string, version int64, spec SnapshotEdgeShardSpec) bool {
	return indexTenantMatches(shard.TenantID, tenantID) &&
		shard.RelationType == spec.RelationType &&
		shard.Shard == spec.Shard &&
		shard.Version == version &&
		spec.ContentHash != "" &&
		edgeShardContentHash(shard) == spec.ContentHash
}
