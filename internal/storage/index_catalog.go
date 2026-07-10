package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type IndexSpec struct {
	Name           string           `json:"name"`
	Kind           string           `json:"kind,omitempty"`
	Field          string           `json:"field,omitempty"`
	Type           string           `json:"type"`
	Status         string           `json:"status"`
	Format         string           `json:"format,omitempty"`
	Codec          string           `json:"codec,omitempty"`
	Objects        []IndexObject    `json:"objects,omitempty"`
	RowCount       int              `json:"row_count,omitempty"`
	EntryCount     int              `json:"entry_count,omitempty"`
	DistinctValues int              `json:"distinct_values,omitempty"`
	TopValues      []IndexValueStat `json:"top_values,omitempty"`
	ContentHash    string           `json:"content_hash,omitempty"`
	SchemaHash     string           `json:"schema_hash,omitempty"`
	UpdatedAt      time.Time        `json:"updated_at"`
}

type IndexObject struct {
	Role        string `json:"role"`
	Key         string `json:"key"`
	Format      string `json:"format,omitempty"`
	Codec       string `json:"codec,omitempty"`
	RowCount    int    `json:"row_count,omitempty"`
	ContentHash string `json:"content_hash,omitempty"`
	SchemaHash  string `json:"schema_hash,omitempty"`

	inspectRelationType string
	inspectShard        string
}

type IndexValueStat struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type EdgeShard struct {
	RelationType string        `json:"relation_type"`
	Shard        string        `json:"shard"`
	Format       string        `json:"format,omitempty"`
	Codec        string        `json:"codec,omitempty"`
	Objects      []IndexObject `json:"objects,omitempty"`
	RowCount     int           `json:"row_count,omitempty"`
	EdgeCount    int           `json:"edge_count"`
	ContentHash  string        `json:"content_hash,omitempty"`
	SchemaHash   string        `json:"schema_hash,omitempty"`
	UpdatedAt    time.Time     `json:"updated_at"`
}

type EntityPageSpec struct {
	Shard       string        `json:"shard"`
	Format      string        `json:"format,omitempty"`
	Codec       string        `json:"codec,omitempty"`
	Objects     []IndexObject `json:"objects,omitempty"`
	RowCount    int           `json:"row_count,omitempty"`
	EntityCount int           `json:"entity_count"`
	ContentHash string        `json:"content_hash,omitempty"`
	SchemaHash  string        `json:"schema_hash,omitempty"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

type SecondaryIndex struct {
	LayoutVersion      int                 `json:"layout_version,omitempty"`
	TenantID           string              `json:"tenant_id,omitempty"`
	Kind               string              `json:"kind"`
	Field              string              `json:"field"`
	Unique             bool                `json:"unique"`
	Values             map[string][]string `json:"values"`
	Version            int64               `json:"version"`
	UpdatedAt          time.Time           `json:"updated_at"`
	cacheVerified      bool
	hashCanonical      bool
	logicalContentHash string
	cachedObjectGroups []secondaryIndexObjectGroup
}

type EdgeShardData struct {
	LayoutVersion      int          `json:"layout_version,omitempty"`
	TenantID           string       `json:"tenant_id,omitempty"`
	RelationType       string       `json:"relation_type"`
	Shard              string       `json:"shard"`
	Edges              []graph.Edge `json:"edges"`
	Version            int64        `json:"version"`
	UpdatedAt          time.Time    `json:"updated_at"`
	cacheVerified      bool
	hashCanonical      bool
	logicalContentHash string
}

type EntityPageData struct {
	LayoutVersion      int            `json:"layout_version,omitempty"`
	TenantID           string         `json:"tenant_id,omitempty"`
	Shard              string         `json:"shard"`
	Entities           []graph.Entity `json:"entities"`
	Version            int64          `json:"version"`
	UpdatedAt          time.Time      `json:"updated_at"`
	cacheVerified      bool
	hashCanonical      bool
	logicalContentHash string
}

type EntityRecord struct {
	LayoutVersion int          `json:"layout_version,omitempty"`
	TenantID      string       `json:"tenant_id,omitempty"`
	ID            string       `json:"id"`
	Page          string       `json:"page"`
	Ordinal       int          `json:"ordinal,omitempty"`
	PageHash      string       `json:"page_hash,omitempty"`
	PageETag      string       `json:"page_etag,omitempty"`
	ContentHash   string       `json:"content_hash,omitempty"`
	Entity        graph.Entity `json:"entity"`
	Deleted       bool         `json:"deleted,omitempty"`
	Version       int64        `json:"version"`
	UpdatedAt     time.Time    `json:"updated_at"`
}

type IndexCatalog struct {
	LayoutVersion int              `json:"layout_version,omitempty"`
	TenantID      string           `json:"tenant_id,omitempty"`
	Version       int64            `json:"version"`
	Indexes       []IndexSpec      `json:"indexes"`
	EdgeShards    []EdgeShard      `json:"edge_shards"`
	EntityPages   []EntityPageSpec `json:"entity_pages,omitempty"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

func (s *TenantStore) RebuildIndexes(ctx context.Context, tenantID string) (IndexCatalog, error) {
	return s.RebuildIndexesWithOptions(ctx, tenantID, IndexRebuildOptions{})
}

func (s *TenantStore) RebuildIndexesWithOptions(ctx context.Context, tenantID string, opts IndexRebuildOptions) (IndexCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexCatalog{}, err
	}
	format, err := s.effectiveIndexFormat(opts.Format)
	if err != nil {
		return IndexCatalog{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return IndexCatalog{}, err
	}
	loaded, err := s.loadForWriteLocked(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, err
	}
	g := loaded.Graph
	manifest := loaded.Manifest
	_, previousMeta, _ := s.previousIndexCatalog(ctx, tenantID)
	definitions, err := s.getIndexDefinitions(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, err
	}
	artifacts, err := buildIndexArtifactsWithDefinitions(g, manifest.Version, definitions)
	if err != nil {
		return IndexCatalog{}, err
	}
	catalog := artifacts.Catalog
	catalog.TenantID = tenantID
	s.decorateIndexCatalog(&catalog, tenantID, format)
	if err := s.ensureIndexRebuildCurrent(ctx, tenantID, manifest); err != nil {
		return IndexCatalog{}, err
	}
	if err := s.writeSecondaryIndexesWithFormat(ctx, tenantID, artifacts.Indexes, format); err != nil {
		return IndexCatalog{}, err
	}
	if err := s.writeEdgeShardsWithFormat(ctx, tenantID, artifacts.EdgeShards, format); err != nil {
		return IndexCatalog{}, err
	}
	if err := s.writeEntityPagesWithFormat(ctx, tenantID, g, manifest.Version, artifacts.EntityPages, format); err != nil {
		return IndexCatalog{}, err
	}
	if err := s.ensureIndexRebuildCurrent(ctx, tenantID, manifest); err != nil {
		return IndexCatalog{}, err
	}
	catalogMeta, err := s.putIndexCatalogWithMeta(ctx, tenantID, catalog, previousMeta)
	if err != nil {
		s.deleteCachedIndexCatalog(tenantID)
		return IndexCatalog{}, err
	}
	s.setCachedIndexCatalog(tenantID, catalog, catalogMeta)
	if err := s.ensureIndexRebuildCurrent(ctx, tenantID, manifest); err != nil {
		return IndexCatalog{}, err
	}
	return catalog, nil
}

func (s *TenantStore) ensureIndexRebuildCurrent(ctx context.Context, tenantID string, loaded Manifest) error {
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return err
	}
	current, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return err
	}
	if current.Version != loaded.Version ||
		current.HeadCommitID != loaded.HeadCommitID ||
		current.SnapshotKey != loaded.SnapshotKey ||
		current.SnapshotCatalogKey != loaded.SnapshotCatalogKey ||
		current.SnapshotVersion != loaded.SnapshotVersion ||
		!commitSegmentsEqual(current.CommitSegments, loaded.CommitSegments) ||
		!slices.Equal(current.CommitKeys, loaded.CommitKeys) {
		return fmt.Errorf("%w: manifest for tenant %q changed while rebuilding indexes", ErrConflict, tenantID)
	}
	return nil
}

func (s *TenantStore) previousIndexCatalog(ctx context.Context, tenantID string) (IndexCatalog, ObjectMeta, bool) {
	catalog, meta, err := s.getIndexCatalogWithMeta(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, ObjectMeta{Key: s.indexCatalogKey(tenantID)}, false
	}
	return catalog, meta, true
}

func (s *TenantStore) GetIndexCatalog(ctx context.Context, tenantID string) (IndexCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexCatalog{}, err
	}
	catalog, _, err := s.getIndexCatalogWithMeta(ctx, tenantID)
	return catalog, err
}

// GetIndexCatalogAtVersion reuses a decoded catalog only when the caller has
// already established the matching manifest version. A same-version catalog
// rebuild can change index availability, but cannot expose different graph
// data, so this cache does not weaken the manifest visibility boundary.
func (s *TenantStore) GetIndexCatalogAtVersion(ctx context.Context, tenantID string, version int64) (IndexCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexCatalog{}, err
	}
	if cached, _, ok := s.getCachedIndexCatalog(tenantID); ok {
		if version <= 0 || cached.Version == version {
			return cached, nil
		}
	}
	catalog, meta, err := s.getIndexCatalogWithMeta(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, err
	}
	if version <= 0 || catalog.Version == version {
		s.setCachedIndexCatalog(tenantID, catalog, meta)
	}
	return catalog, nil
}

func (s *TenantStore) GetIndexCatalogVersion(ctx context.Context, tenantID string, version int64) (IndexCatalog, error) {
	return s.GetIndexCatalogSnapshot(ctx, tenantID, version, "")
}

func (s *TenantStore) GetIndexCatalogSnapshot(ctx context.Context, tenantID string, version int64, contentHash string) (IndexCatalog, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexCatalog{}, err
	}
	if version <= 0 {
		return IndexCatalog{}, ErrNotFound
	}
	current, err := s.GetIndexCatalog(ctx, tenantID)
	if err == nil && current.Version == version {
		if contentHash == "" {
			return current, nil
		}
		hash, hashErr := indexCatalogContentHash(current)
		if hashErr == nil && hash == contentHash {
			return current, nil
		}
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return IndexCatalog{}, err
	}
	key := s.indexCatalogVersionHashKey(tenantID, version, contentHash)
	data, _, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return IndexCatalog{}, err
	}
	catalog, err := decodeIndexCatalogObject(ctx, data)
	if err != nil {
		return IndexCatalog{}, err
	}
	if catalog.TenantID != "" && catalog.TenantID != tenantID {
		return IndexCatalog{}, fmt.Errorf("index catalog tenant mismatch: path tenant %q contains tenant %q", tenantID, catalog.TenantID)
	}
	if catalog.Version != version {
		return IndexCatalog{}, fmt.Errorf("index catalog version mismatch: key version %d object version %d", version, catalog.Version)
	}
	if contentHash != "" {
		hash, err := indexCatalogContentHash(catalog)
		if err != nil {
			return IndexCatalog{}, err
		}
		if hash != contentHash {
			return IndexCatalog{}, fmt.Errorf("index catalog content hash mismatch")
		}
	}
	if catalog.TenantID == "" {
		catalog.TenantID = tenantID
	}
	return catalog, nil
}

func (s *TenantStore) getIndexCatalogWithMeta(ctx context.Context, tenantID string) (IndexCatalog, ObjectMeta, error) {
	key := s.indexCatalogKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return IndexCatalog{}, meta, err
	}
	catalog, err := decodeIndexCatalogObject(ctx, data)
	if err != nil {
		return IndexCatalog{}, meta, err
	}
	if catalog.TenantID != "" && catalog.TenantID != tenantID {
		return IndexCatalog{}, meta, fmt.Errorf("index catalog tenant mismatch: path tenant %q contains tenant %q", tenantID, catalog.TenantID)
	}
	if catalog.TenantID == "" {
		catalog.TenantID = tenantID
	}
	return catalog, meta, nil
}

func (s *TenantStore) getIndexCatalogForWriteWithMeta(ctx context.Context, tenantID string) (IndexCatalog, ObjectMeta, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexCatalog{}, ObjectMeta{}, err
	}
	if catalog, meta, ok := s.getCachedIndexCatalog(tenantID); ok {
		return catalog, meta, nil
	}
	catalog, meta, err := s.getIndexCatalogWithMeta(ctx, tenantID)
	if err != nil {
		return IndexCatalog{}, meta, err
	}
	s.setCachedIndexCatalog(tenantID, catalog, meta)
	return catalog, meta, nil
}

func (s *TenantStore) putIndexCatalogWithMeta(ctx context.Context, tenantID string, catalog IndexCatalog, meta ObjectMeta) (ObjectMeta, error) {
	return s.putIndexCatalogWithMetaMode(ctx, tenantID, catalog, meta, false)
}

func (s *TenantStore) putIndexCatalogWithMetaFast(ctx context.Context, tenantID string, catalog IndexCatalog, meta ObjectMeta) (ObjectMeta, error) {
	return s.putIndexCatalogWithMetaMode(ctx, tenantID, catalog, meta, true)
}

func (s *TenantStore) putIndexCatalogWithMetaMode(ctx context.Context, tenantID string, catalog IndexCatalog, meta ObjectMeta, fast bool) (ObjectMeta, error) {
	catalog.TenantID = tenantID
	key := s.indexCatalogKey(tenantID)
	data, err := marshalParquetIndexCatalog(ctx, catalog)
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := s.putIndexCatalogVersion(ctx, tenantID, catalog, data, fast); err != nil {
		return ObjectMeta{}, err
	}
	writeMeta := meta
	if writeMeta.Key != key {
		writeMeta = ObjectMeta{Key: key}
	}
	nextMeta, err := s.putBytesWithMetaResult(ctx, key, data, writeMeta)
	if err != nil {
		return ObjectMeta{}, err
	}
	s.setCachedIndexCatalog(tenantID, catalog, nextMeta)
	return nextMeta, nil
}

func (s *TenantStore) putIndexCatalogVersion(ctx context.Context, tenantID string, catalog IndexCatalog, data []byte, fast bool) error {
	hash, err := indexCatalogContentHash(catalog)
	if err != nil {
		return err
	}
	if err := s.putImmutableIndexCatalogVersion(ctx, s.indexCatalogVersionHashKey(tenantID, catalog.Version, hash), catalog.Version, data, fast); err != nil {
		return err
	}
	return s.putLegacyIndexCatalogVersion(ctx, tenantID, catalog.Version, data, fast)
}

func (s *TenantStore) putImmutableIndexCatalogVersion(ctx context.Context, key string, version int64, data []byte, fast bool) error {
	if !fast {
		existing, err := s.Objects.Get(ctx, key)
		if err == nil {
			if sameIndexCatalogObjectContent(ctx, existing, data) {
				return nil
			}
			return fmt.Errorf("%w: index catalog version %d already exists with different content", ErrConflict, version)
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, getErr := s.Objects.Get(ctx, key)
	if getErr != nil {
		return getErr
	}
	if sameIndexCatalogObjectContent(ctx, existing, data) {
		return nil
	}
	return fmt.Errorf("%w: index catalog version %d already exists with different content", ErrConflict, version)
}

func (s *TenantStore) putLegacyIndexCatalogVersion(ctx context.Context, tenantID string, version int64, data []byte, fast bool) error {
	key := s.indexCatalogVersionKey(tenantID, version)
	if !fast {
		existing, err := s.Objects.Get(ctx, key)
		if err == nil {
			if sameIndexCatalogObjectContent(ctx, existing, data) {
				return nil
			}
			return nil
		}
		if !errors.Is(err, ErrNotFound) {
			return err
		}
	}
	_, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
	if errors.Is(err, ErrConflict) {
		return nil
	}
	return err
}

func sameIndexCatalogObjectContent(ctx context.Context, left []byte, right []byte) bool {
	if bytes.Equal(left, right) {
		return true
	}
	leftCatalog, leftErr := decodeIndexCatalogObject(ctx, left)
	rightCatalog, rightErr := decodeIndexCatalogObject(ctx, right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftHash, leftErr := indexCatalogContentHash(leftCatalog)
	rightHash, rightErr := indexCatalogContentHash(rightCatalog)
	return leftErr == nil && rightErr == nil && leftHash == rightHash
}

func decodeIndexCatalogObject(ctx context.Context, data []byte) (IndexCatalog, error) {
	if !isParquetBytes(data) {
		return IndexCatalog{}, fmt.Errorf("unsupported index catalog: only parquet catalogs are readable")
	}
	return decodeParquetIndexCatalog(ctx, data)
}
