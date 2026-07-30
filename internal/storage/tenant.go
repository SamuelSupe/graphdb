package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
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
	WriterFence        string             `json:"-"`
	WriterFenceEpoch   int64              `json:"-"`
	DataMD5            string             `json:"-"`
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
	// WriteBackpressureChecked avoids repeating the full admission scan when
	// the HTTP layer already completed it for this request.
	WriteBackpressureChecked bool
	directCommit             *directCommitReservation
	collectorState           *CollectorStateUpdate
}

type TenantStore struct {
	Objects                    ObjectStore
	Coordinator                WriteCoordinator
	Prefix                     string
	coordinatorStatusMu        sync.RWMutex
	coordinatorStatusCache     CoordinatorStatus
	coordinatorStatusActive    *coordinatorStatusCall
	objectProbeMu              sync.Mutex
	objectProbeActive          *objectStoreProbeCall
	lockMu                     sync.Mutex
	tenantLocks                map[string]*tenantLock
	tenantRegistryMu           sync.Mutex
	writeCache                 map[string]loadedGraph
	writeCacheOrder            []string
	writeCacheBytes            int64
	writerLeaseCache           map[string]cachedWriterLease
	registeredTenantCache      map[string]registeredTenantCacheEntry
	collectorStatusCache       map[string]cachedCollectorStatus
	readerHeartbeatCache       map[string]cachedReaderHeartbeat
	objectKeyCache             map[string]struct{}
	tenantMetadataCache        map[string]cachedTenantMetadata
	purgeTombstoneCache        map[string]cachedTenantPurgeTombstone
	sourcePolicyCache          map[string]cachedSourcePolicy
	tenantConfigCache          map[string]cachedTenantConfig
	indexCatalogCache          map[string]cachedIndexCatalog
	indexCatalogLoads          map[string]*indexCatalogLoad
	reverseIndexCatalogCache   map[string]cachedReverseIndexCatalog
	reverseIndexCatalogLoads   map[string]*reverseIndexCatalogLoad
	compiledScanCatalogCache   map[string]*compiledScanCatalog
	indexCache                 *indexObjectCache
	entityPageCache            *entityPageCache
	edgeLookupCache            *edgeLookupCache
	taskMu                     sync.Mutex
	indexTasks                 map[string]IndexTask
	taskCancels                map[string]context.CancelFunc
	taskActive                 map[string]Task
	taskQueueSlots             chan struct{}
	taskExecutionSlots         chan struct{}
	taskTenantSlots            []chan struct{}
	indexTaskStartSlots        []chan struct{}
	InstanceID                 string
	ReaderID                   string
	LeaseTTL                   time.Duration
	LifecycleCacheTTL          time.Duration
	TaskMarkerTTL              time.Duration
	MaxRetries                 int
	CoordinatorRetryLimit      int
	CoordinatorPendingTTL      time.Duration
	CoordinatorCleanup         CoordinatorCleanupConfig
	MaxWriteCacheTenants       int
	MaxWriteCacheBytes         int64
	EntityPagePackMaxBytes     int64
	IndexFormat                string
	WriteEntityRecords         bool
	UseEntityRecordsForRead    bool
	MaterializeCollectorStatus bool
	Backpressure               *WritePressure
	BackpressureObserver       BackpressureObserver
	CacheObserver              ReaderCacheObserver
	CoordinatorObserver        CoordinatorObserver
	IngestBarrier              func(context.Context, string) error
}

type loadedGraph struct {
	Graph      *graph.Graph
	Manifest   Manifest
	Meta       ObjectMeta
	DataMD5    string
	CommitTail commitTailCache
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
		registeredTenantCache:      map[string]registeredTenantCacheEntry{},
		collectorStatusCache:       map[string]cachedCollectorStatus{},
		readerHeartbeatCache:       map[string]cachedReaderHeartbeat{},
		objectKeyCache:             map[string]struct{}{},
		tenantMetadataCache:        map[string]cachedTenantMetadata{},
		purgeTombstoneCache:        map[string]cachedTenantPurgeTombstone{},
		sourcePolicyCache:          map[string]cachedSourcePolicy{},
		tenantConfigCache:          map[string]cachedTenantConfig{},
		indexCatalogCache:          map[string]cachedIndexCatalog{},
		indexCatalogLoads:          map[string]*indexCatalogLoad{},
		reverseIndexCatalogCache:   map[string]cachedReverseIndexCatalog{},
		reverseIndexCatalogLoads:   map[string]*reverseIndexCatalogLoad{},
		compiledScanCatalogCache:   map[string]*compiledScanCatalog{},
		indexCache:                 newIndexObjectCache(4096),
		entityPageCache:            newEntityPageCache(2048),
		edgeLookupCache:            newEdgeLookupCache(2048, defaultEdgeLookupCacheMaxBytes),
		indexTasks:                 map[string]IndexTask{},
		taskCancels:                map[string]context.CancelFunc{},
		taskActive:                 map[string]Task{},
		taskQueueSlots:             make(chan struct{}, defaultTaskQueueLimit),
		taskExecutionSlots:         make(chan struct{}, defaultTaskExecutionLimit),
		taskTenantSlots:            newTaskTenantSlots(defaultTaskTenantStripes),
		indexTaskStartSlots:        newTaskTenantSlots(defaultTaskTenantStripes),
		InstanceID:                 instanceID,
		ReaderID:                   instanceID,
		LeaseTTL:                   30 * time.Second,
		LifecycleCacheTTL:          time.Second,
		TaskMarkerTTL:              30 * time.Second,
		MaxRetries:                 3,
		CoordinatorRetryLimit:      8,
		CoordinatorPendingTTL:      coordinatorPendingReservationTTL,
		CoordinatorCleanup:         DefaultCoordinatorCleanupConfig(),
		MaxWriteCacheTenants:       64,
		MaxWriteCacheBytes:         512 * 1024 * 1024,
		EntityPagePackMaxBytes:     defaultEntityPagePackMaxBytes,
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
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	ctx = boundCtx
	g, dataMD5, cacheBytes, err := newEmptyTenantGraph()
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, UpdatedAt: time.Now().UTC(), DataMD5: dataMD5}
	meta, err := s.putManifestMeta(ctx, tenantID, manifest, ObjectMeta{Key: s.manifestKey(tenantID)})
	if err != nil {
		return manifest, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return manifest, fmt.Errorf("register tenant %q: %w", tenantID, err)
	}
	s.setWriteCache(tenantID, loadedGraph{
		Graph: g, Manifest: manifest, Meta: meta,
		DataMD5:    dataMD5,
		CommitTail: emptyCommitTailCache(),
		CacheBytes: cacheBytes,
	})
	return manifest, nil
}

func newEmptyTenantGraph() (*graph.Graph, string, int64, error) {
	g := graph.New()
	if _, err := g.ContentFingerprint(); err != nil {
		return nil, "", 0, err
	}
	dataMD5, logicalBytes, err := g.ContentMD5WithLogicalSize()
	return g, dataMD5, writeCacheBytesForGraph(g, logicalBytes), err
}

func (s *TenantStore) Commit(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (Manifest, error) {
	result, err := s.CommitWithReport(ctx, tenantID, mutations, opts)
	return result.Manifest, err
}

func (s *TenantStore) Compact(ctx context.Context, tenantID string) (Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Manifest{}, err
	}
	if s.IngestBarrier != nil {
		if err := s.IngestBarrier(ctx, tenantID); err != nil {
			return Manifest{}, err
		}
	}
	if s.coordinated() {
		operationCtx, stop, err := s.startCoordinatorOperationLease(
			ctx, tenantID, TaskTypeCompact,
		)
		if err != nil {
			return Manifest{}, err
		}
		defer stop()
		ctx = operationCtx
	}
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return Manifest{}, err
	}
	loaded, err := s.loadForWriteLocked(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	g := loaded.Graph
	manifest := loaded.Manifest
	alreadyCompacted := manifestCommitTailLength(manifest) == 0 && manifest.Version == manifest.SnapshotVersion && manifest.SnapshotKey != "" && manifest.SnapshotCatalogKey != ""
	dataMD5 := loaded.DataMD5
	if !alreadyCompacted && dataMD5 == "" {
		dataMD5, err = g.ContentMD5()
		if err != nil {
			return Manifest{}, err
		}
	}
	var snapshotCatalog ShardedSnapshotCatalog
	var snapshotKey string
	if !alreadyCompacted {
		// Snapshot objects are versioned and immutable, so their expensive build
		// can run concurrently with foreground commits.
		snapshot := g.Snapshot()
		snapshotCatalog, err = s.putShardedSnapshot(ctx, tenantID, snapshot)
		if err != nil {
			return Manifest{}, err
		}
		snapshotKey = s.snapshotKey(tenantID, snapshot.Version)
		record := snapshotRecord{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, Snapshot: snapshot}
		if err := s.putSnapshotRecordIfAbsentOrEquivalent(ctx, snapshotKey, record); err != nil {
			return Manifest{}, err
		}
	}

	unlock, err := s.lockTenantMaintenance(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	defer unlock()
	boundCtx, err = s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	ctx = boundCtx
	if err := s.EnsureTenantWritable(ctx, tenantID); err != nil {
		return Manifest{}, err
	}
	if s.coordinated() {
		manifest, meta, err := s.publishCoordinatedCompaction(
			ctx,
			tenantID,
			loaded,
			snapshotKey,
			snapshotCatalog.Key,
			dataMD5,
		)
		if err != nil {
			return Manifest{}, err
		}
		if manifest.Version == loaded.Manifest.Version {
			s.setWriteCache(tenantID, loadedGraph{
				Graph: g, Manifest: manifest, Meta: meta,
				DataMD5:    dataMD5,
				CommitTail: emptyCommitTailCache(),
				CacheBytes: writeCacheBytesWithoutCommitTail(
					loaded,
				),
			})
		}
		return manifest, nil
	}
	current, currentMeta, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	if !cachedManifestMatches(loaded, current, currentMeta) {
		return Manifest{}, fmt.Errorf("%w: manifest changed while compacting tenant %q", ErrConflict, tenantID)
	}
	if alreadyCompacted {
		return current, nil
	}
	manifest = current
	manifest.TenantID = tenantID
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.SnapshotKey = snapshotKey
	manifest.SnapshotCatalogKey = snapshotCatalog.Key
	manifest.SnapshotVersion = manifest.Version
	manifest.CommitSegments = nil
	manifest.CommitKeys = nil
	manifest.UpdatedAt = time.Now().UTC()
	manifest.DataMD5 = dataMD5
	meta, err := s.putManifestMeta(ctx, tenantID, manifest, currentMeta)
	if err != nil {
		s.deleteWriteCache(tenantID)
		return Manifest{}, err
	}
	s.setWriteCache(tenantID, loadedGraph{
		Graph: g, Manifest: manifest, Meta: meta,
		DataMD5:    dataMD5,
		CommitTail: emptyCommitTailCache(),
		CacheBytes: writeCacheBytesWithoutCommitTail(loaded),
	})
	return manifest, nil
}
