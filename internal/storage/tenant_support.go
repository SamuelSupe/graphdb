package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
)

var tenantIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func ValidateTenantID(tenantID string) error {
	if !tenantIDPattern.MatchString(tenantID) || strings.Contains(tenantID, "..") {
		return fmt.Errorf("invalid tenant id %q", tenantID)
	}
	return nil
}

func (s *TenantStore) getManifest(ctx context.Context, tenantID string) (Manifest, ObjectMeta, error) {
	var manifest Manifest
	key := s.manifestKey(tenantID)
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return Manifest{TenantID: tenantID}, ObjectMeta{Key: key}, nil
	}
	if err != nil {
		return Manifest{}, ObjectMeta{}, err
	}
	if !isParquetBytes(data) {
		return Manifest{}, ObjectMeta{}, fmt.Errorf("unsupported manifest: only parquet manifests are readable")
	}
	manifest, err = decodeParquetManifest(ctx, data)
	if err != nil {
		return Manifest{}, ObjectMeta{}, err
	}
	if manifest.TenantID != tenantID {
		return Manifest{}, ObjectMeta{}, fmt.Errorf("manifest tenant mismatch: key tenant %q contains tenant %q", tenantID, manifest.TenantID)
	}
	return manifest, meta, nil
}

func (s *TenantStore) putManifest(ctx context.Context, tenantID string, manifest Manifest, meta ObjectMeta) error {
	_, err := s.putManifestMeta(ctx, tenantID, manifest, meta)
	return err
}

func (s *TenantStore) putManifestMeta(ctx context.Context, tenantID string, manifest Manifest, meta ObjectMeta) (ObjectMeta, error) {
	manifest.TenantID = tenantID
	data, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		return ObjectMeta{}, err
	}
	condition := PutCondition{}
	if meta.Exists {
		condition.IfMatch = meta.ETag
	} else {
		condition.IfNoneMatch = true
	}
	next, err := s.Objects.PutConditional(ctx, s.manifestKey(tenantID), data, condition)
	if errors.Is(err, ErrConflict) {
		s.recordManifestCASConflict(tenantID)
		return ObjectMeta{}, fmt.Errorf("%w: manifest for tenant %q changed while publishing", ErrConflict, tenantID)
	}
	return next, err
}

func (s *TenantStore) manifestKey(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "manifest.parquet")
}

func (s *TenantStore) tenantObjectPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID) + "/"
}

func (s *TenantStore) tenantRegistryKey() string {
	return path.Join(s.Prefix, "tenants", "_registry.parquet")
}

func (s *TenantStore) validateTenantObjectKey(tenantID string, key string) error {
	if key == "" {
		return nil
	}
	prefix := s.tenantObjectPrefix(tenantID)
	if !strings.HasPrefix(key, prefix) {
		return fmt.Errorf("object key %q is outside tenant prefix %q", key, prefix)
	}
	return nil
}

func errTenantCommitMismatch(tenantID string, key string, commitTenantID string) error {
	return fmt.Errorf("commit tenant mismatch: key tenant %q object %q contains tenant %q", tenantID, key, commitTenantID)
}

func (s *TenantStore) commitKey(tenantID string, version int64, commitID string) string {
	name := fmt.Sprintf("%020d-%s.parquet", version, commitID)
	return path.Join(s.Prefix, "tenants", tenantID, "commits", name)
}

func (s *TenantStore) commitPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "commits") + "/"
}

func (s *TenantStore) commitSegmentKey(tenantID string, firstVersion int64, lastVersion int64, contentHash string) string {
	hash := contentHash
	if len(hash) > 16 {
		hash = hash[:16]
	}
	name := fmt.Sprintf("%020d-%020d-%s.parquet", firstVersion, lastVersion, hash)
	return path.Join(s.commitSegmentPrefix(tenantID), name)
}

func (s *TenantStore) commitSegmentPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "commits", "segments") + "/"
}

func (s *TenantStore) commitIdempotencyKey(tenantID string, idempotencyKey string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "idempotency", "commits", objectSegment(idempotencyKey)+".parquet")
}

func (s *TenantStore) snapshotKey(tenantID string, version int64) string {
	name := fmt.Sprintf("%020d.parquet", version)
	return path.Join(s.Prefix, "tenants", tenantID, "snapshots", name)
}

func (s *TenantStore) snapshotPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "snapshots") + "/"
}

func (s *TenantStore) snapshotCatalogKey(tenantID string, version int64) string {
	return path.Join(s.snapshotVersionPrefix(tenantID, version), "catalog.parquet")
}

func (s *TenantStore) snapshotSchemaKey(tenantID string, version int64) string {
	return path.Join(s.snapshotVersionPrefix(tenantID, version), "schema.parquet")
}

func (s *TenantStore) snapshotEntityPageKey(tenantID string, version int64, shard string) string {
	return path.Join(s.snapshotVersionPrefix(tenantID, version), "entities", "pages", objectSegment(shard)+".parquet")
}

func (s *TenantStore) snapshotEdgeShardKey(tenantID string, version int64, relationType string, shard string) string {
	return path.Join(s.snapshotVersionPrefix(tenantID, version), "edges", objectSegment(relationType), objectSegment(shard)+".parquet")
}

func (s *TenantStore) snapshotVersionPrefix(tenantID string, version int64) string {
	return path.Join(s.Prefix, "tenants", tenantID, "snapshots", "sharded", "v"+strconv.FormatInt(version, 10))
}

func (s *TenantStore) writerLeaseKey(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "control", "writer-lease.parquet")
}

func (s *TenantStore) tenantMetadataKey(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "metadata.parquet")
}

func (s *TenantStore) sourcePolicyKey(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "config", "source-policy.parquet")
}

func (s *TenantStore) tenantConfigKey(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "config", "tenant-config.parquet")
}

func (s *TenantStore) ingestBatchKey(tenantID string, source string, collectorID string, batchID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "ingest", objectSegment(source), "batches", objectSegment(collectorID), objectSegment(batchID)+".parquet")
}

func (s *TenantStore) legacyIngestBatchKey(tenantID string, source string, batchID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "ingest", objectSegment(source), "batches", objectSegment(batchID)+".parquet")
}

func (s *TenantStore) ingestIdempotencyKey(tenantID string, source string, collectorID string, idempotencyKey string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "ingest", objectSegment(source), "idempotency", objectSegment(collectorID), objectSegment(idempotencyKey)+".parquet")
}

func (s *TenantStore) legacyIngestIdempotencyKey(tenantID string, source string, idempotencyKey string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "ingest", objectSegment(source), "idempotency", objectSegment(idempotencyKey)+".parquet")
}

func (s *TenantStore) collectorStatusKey(tenantID string, source string, collectorID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "ingest", objectSegment(source), "collectors", objectSegment(collectorID)+".parquet")
}

func (s *TenantStore) deadLetterPrefix(tenantID string, source string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "ingest", objectSegment(source), "deadletters") + "/"
}

func (s *TenantStore) deadLetterKey(tenantID string, source string, id string) string {
	return path.Join(s.deadLetterPrefix(tenantID, source), objectSegment(id)+".parquet")
}

func (s *TenantStore) savedQueryPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "queries") + "/"
}

func (s *TenantStore) savedQueryKey(tenantID string, name string) string {
	return path.Join(s.savedQueryPrefix(tenantID), objectSegment(name)+".parquet")
}

func (s *TenantStore) indexCatalogKey(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "catalog.parquet")
}

func (s *TenantStore) indexCatalogVersionKey(tenantID string, version int64) string {
	return path.Join(s.parquetVersionPrefix(tenantID, version), "catalog.parquet")
}

func (s *TenantStore) indexCatalogVersionHashKey(tenantID string, version int64, hash string) string {
	if hash == "" {
		return s.indexCatalogVersionKey(tenantID, version)
	}
	return path.Join(s.parquetVersionPrefix(tenantID, version), "catalogs", objectSegment(hash)+".parquet")
}

func (s *TenantStore) indexDefinitionsKey(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "definitions.parquet")
}

func (s *TenantStore) indexTaskKey(tenantID string, taskID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "tasks", objectSegment(taskID)+".parquet")
}

func (s *TenantStore) indexTaskPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "tasks") + "/"
}

func (s *TenantStore) taskKey(tenantID string, taskID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "tasks", objectSegment(taskID)+".parquet")
}

func (s *TenantStore) taskPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "tasks") + "/"
}

func (s *TenantStore) taskResultKey(tenantID string, taskID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "tasks", "results", objectSegment(taskID)+".parquet")
}

func (s *TenantStore) backupManifestKey(tenantID string, backupID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "backups", objectSegment(backupID), "manifest.parquet")
}

func (s *TenantStore) secondaryIndexPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "fields") + "/"
}

func (s *TenantStore) edgeShardPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "edges") + "/"
}

func (s *TenantStore) parquetEntityPageVersionKey(tenantID string, version int64, shard string) string {
	return path.Join(s.parquetVersionPrefix(tenantID, version), "entities", "pages", objectSegment(shard)+".parquet")
}

func (s *TenantStore) parquetEntityPagePackVersionKey(tenantID string, version int64, packID string) string {
	return path.Join(s.parquetVersionPrefix(tenantID, version), "entities", "pages", "packs", objectSegment(packID)+".parquet")
}

func (s *TenantStore) parquetSecondaryIndexVersionKey(tenantID string, version int64, kind string, field string) string {
	return path.Join(s.parquetVersionPrefix(tenantID, version), "fields", objectSegment(kind), objectSegment(field)+".parquet")
}

func (s *TenantStore) parquetSecondaryIndexShardVersionKey(tenantID string, version int64, kind string, field string, shard string) string {
	return path.Join(s.parquetVersionPrefix(tenantID, version), "fields", objectSegment(kind), objectSegment(field), "shards", objectSegment(shard)+".parquet")
}

func (s *TenantStore) parquetEdgeShardVersionKey(tenantID string, version int64, relationType string, shard string) string {
	return path.Join(s.parquetVersionPrefix(tenantID, version), "edges", objectSegment(relationType), objectSegment(shard)+".parquet")
}

func (s *TenantStore) parquetEdgeShardPackVersionKey(tenantID string, version int64, relationType string, packID string) string {
	return path.Join(s.parquetVersionPrefix(tenantID, version), "edges", objectSegment(relationType), "packs", objectSegment(packID)+".parquet")
}

func (s *TenantStore) parquetVersionPrefix(tenantID string, version int64) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "parquet", "versions", "v"+strconv.FormatInt(version, 10))
}

func (s *TenantStore) parquetVersionRootPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "parquet", "versions") + "/"
}

func (s *TenantStore) entityPagePrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "entities", "pages") + "/"
}

func (s *TenantStore) entityRecordKey(tenantID string, entityID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "entities", "by-id", objectSegment(entityID)+".parquet")
}

func (s *TenantStore) entityRecordPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "indexes", "entities", "by-id") + "/"
}

func cleanPrefix(prefix string) string {
	return strings.Trim(strings.TrimSpace(prefix), "/")
}

func objectSegment(value string) string {
	escaped := url.PathEscape(strings.TrimSpace(value))
	switch escaped {
	case ".":
		return "%2E"
	case "..":
		return "%2E%2E"
	default:
		return escaped
	}
}

func newCommitID() (string, error) {
	var bytes [12]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
