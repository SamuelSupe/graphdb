package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"graphdb/internal/graph"
)

type Manifest struct {
	LayoutVersion      int                `json:"layout_version,omitempty"`
	TenantID           string             `json:"tenant_id"`
	Version            int64              `json:"version"`
	HeadCommitID       string             `json:"head_commit_id,omitempty"`
	SnapshotKey        string             `json:"snapshot_key,omitempty"`
	SnapshotCatalogKey string             `json:"snapshot_catalog_key,omitempty"`
	SnapshotVersion    int64              `json:"snapshot_version"`
	CommitSegments     []CommitSegmentRef `json:"commit_segments,omitempty"`
	CommitKeys         []string           `json:"commit_keys,omitempty"`
	UpdatedAt          time.Time          `json:"updated_at"`
}

type CommitSegmentRef struct {
	Key          string `json:"key"`
	Codec        string `json:"codec"`
	FirstVersion int64  `json:"first_version"`
	LastVersion  int64  `json:"last_version"`
	Count        int    `json:"count"`
	ContentHash  string `json:"content_hash"`
}

type CommitResult struct {
	Manifest
	ReadableVersion   int64                          `json:"readable_version,omitempty"`
	ReadAfterCommitID string                         `json:"read_after_commit_id,omitempty"`
	Skipped           bool                           `json:"skipped,omitempty"`
	IdempotentReplay  bool                           `json:"idempotent_replay,omitempty"`
	DataMD5           string                         `json:"data_md5,omitempty"`
	Suppressed        []graph.FieldConflict          `json:"suppressed,omitempty"`
	CanonicalEntities []graph.EntityCanonicalization `json:"canonical_entities,omitempty"`
	CanonicalEdges    []graph.EdgeCanonicalization   `json:"canonical_edges,omitempty"`
	IndexWarnings     []string                       `json:"index_warnings,omitempty"`
}

type snapshotRecord struct {
	LayoutVersion int    `json:"layout_version,omitempty"`
	TenantID      string `json:"tenant_id,omitempty"`
	graph.Snapshot
}

type CommitOptions struct {
	ExpectedVersion *int64
	IdempotencyKey  string
}

type TenantStore struct {
	Objects                    ObjectStore
	Prefix                     string
	lockMu                     sync.Mutex
	tenantLocks                map[string]*tenantLock
	tenantRegistryMu           sync.Mutex
	writeCache                 map[string]loadedGraph
	writeCacheOrder            []string
	writeCacheBytes            int64
	writerLeaseCache           map[string]cachedWriterLease
	registeredTenantCache      map[string]struct{}
	collectorStatusCache       map[string]cachedCollectorStatus
	readerHeartbeatCache       map[string]cachedReaderHeartbeat
	objectKeyCache             map[string]struct{}
	objectPrefixCache          map[string]struct{}
	tenantMetadataCache        map[string]cachedTenantMetadata
	sourcePolicyCache          map[string]cachedSourcePolicy
	tenantConfigCache          map[string]cachedTenantConfig
	indexCatalogCache          map[string]cachedIndexCatalog
	indexCache                 *indexObjectCache
	entityPageCache            *entityPageCache
	taskMu                     sync.Mutex
	indexTasks                 map[string]IndexTask
	taskCancels                map[string]context.CancelFunc
	taskActive                 map[string]Task
	taskQueueSlots             chan struct{}
	taskExecutionSlots         chan struct{}
	taskTenantSlots            []chan struct{}
	InstanceID                 string
	ReaderID                   string
	LeaseTTL                   time.Duration
	MaxRetries                 int
	MaxWriteCacheTenants       int
	MaxWriteCacheBytes         int64
	IndexFormat                string
	WriteEntityRecords         bool
	MaterializeCollectorStatus bool
	Backpressure               *WritePressure
	BackpressureObserver       BackpressureObserver
	CacheObserver              ReaderCacheObserver
}

type loadedGraph struct {
	Graph      *graph.Graph
	Manifest   Manifest
	Meta       ObjectMeta
	DataMD5    string
	CacheBytes int64
}

func NewTenantStore(objects ObjectStore, prefix string) *TenantStore {
	instanceID, err := newCommitID()
	if err != nil {
		instanceID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return &TenantStore{
		Objects:                    objects,
		Prefix:                     cleanPrefix(prefix),
		tenantLocks:                map[string]*tenantLock{},
		writeCache:                 map[string]loadedGraph{},
		writerLeaseCache:           map[string]cachedWriterLease{},
		registeredTenantCache:      map[string]struct{}{},
		collectorStatusCache:       map[string]cachedCollectorStatus{},
		readerHeartbeatCache:       map[string]cachedReaderHeartbeat{},
		objectKeyCache:             map[string]struct{}{},
		objectPrefixCache:          map[string]struct{}{},
		tenantMetadataCache:        map[string]cachedTenantMetadata{},
		sourcePolicyCache:          map[string]cachedSourcePolicy{},
		tenantConfigCache:          map[string]cachedTenantConfig{},
		indexCatalogCache:          map[string]cachedIndexCatalog{},
		indexCache:                 newIndexObjectCache(4096),
		entityPageCache:            newEntityPageCache(2048),
		indexTasks:                 map[string]IndexTask{},
		taskCancels:                map[string]context.CancelFunc{},
		taskActive:                 map[string]Task{},
		taskQueueSlots:             make(chan struct{}, defaultTaskQueueLimit),
		taskExecutionSlots:         make(chan struct{}, defaultTaskExecutionLimit),
		taskTenantSlots:            newTaskTenantSlots(defaultTaskTenantStripes),
		InstanceID:                 instanceID,
		ReaderID:                   instanceID,
		LeaseTTL:                   30 * time.Second,
		MaxRetries:                 3,
		MaxWriteCacheTenants:       64,
		MaxWriteCacheBytes:         512 * 1024 * 1024,
		WriteEntityRecords:         true,
		MaterializeCollectorStatus: true,
	}
}

func (s *TenantStore) InitTenant(ctx context.Context, tenantID string) (Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Manifest{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, UpdatedAt: time.Now().UTC()}
	meta, err := s.putManifestMeta(ctx, tenantID, manifest, ObjectMeta{Key: s.manifestKey(tenantID)})
	if err == nil {
		_ = s.addTenantToRegistry(ctx, tenantID)
		s.setWriteCache(tenantID, loadedGraph{Graph: graph.New(), Manifest: manifest, Meta: meta})
	}
	return manifest, err
}

func (s *TenantStore) Commit(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (Manifest, error) {
	result, err := s.CommitWithReport(ctx, tenantID, mutations, opts)
	return result.Manifest, err
}

func (s *TenantStore) Compact(ctx context.Context, tenantID string) (Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Manifest{}, err
	}
	unlock := s.lockTenant(tenantID)
	defer unlock()
	if err := s.acquireWriterLease(ctx, tenantID); err != nil {
		return Manifest{}, err
	}
	return s.compactLocked(ctx, tenantID)
}

func (s *TenantStore) compactLocked(ctx context.Context, tenantID string) (Manifest, error) {
	loaded, err := s.loadForWriteLocked(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	g := loaded.Graph
	manifest := loaded.Manifest
	if manifestCommitTailLength(manifest) == 0 && manifest.Version == manifest.SnapshotVersion && manifest.SnapshotKey != "" && manifest.SnapshotCatalogKey != "" {
		return manifest, nil
	}
	snapshot := g.Snapshot()
	snapshotCatalog, err := s.putShardedSnapshot(ctx, tenantID, snapshot)
	if err != nil {
		return Manifest{}, err
	}
	snapshotKey := s.snapshotKey(tenantID, snapshot.Version)
	record := snapshotRecord{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, Snapshot: snapshot}
	if err := s.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, record); err != nil {
		return Manifest{}, err
	}
	manifest.TenantID = tenantID
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.Version = snapshot.Version
	manifest.SnapshotKey = snapshotKey
	manifest.SnapshotCatalogKey = snapshotCatalog.Key
	manifest.SnapshotVersion = snapshot.Version
	manifest.CommitSegments = nil
	manifest.CommitKeys = nil
	manifest.UpdatedAt = time.Now().UTC()
	meta, err := s.putManifestMeta(ctx, tenantID, manifest, loaded.Meta)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return Manifest{}, err
	}
	s.setWriteCache(tenantID, loadedGraph{Graph: g, Manifest: manifest, Meta: meta, DataMD5: loaded.DataMD5, CacheBytes: loaded.CacheBytes})
	return manifest, nil
}
