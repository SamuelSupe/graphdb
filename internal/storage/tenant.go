package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
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
	DataHash           string             `json:"-"`
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
	DataHash          string                         `json:"data_hash,omitempty"`
	Suppressed        []graph.FieldConflict          `json:"suppressed,omitempty"`
	CanonicalEntities []graph.EntityCanonicalization `json:"canonical_entities,omitempty"`
	CanonicalEdges    []graph.EdgeCanonicalization   `json:"canonical_edges,omitempty"`
	IndexWarnings     []string                       `json:"index_warnings,omitempty"`
	indexUpdate       *commitIndexUpdate
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
	ingestPreconditions      []IngestPrecondition
	ingestAcceptedAt         time.Time
	rejectSuppressed         bool
}

type TenantStore struct {
	ReplicationMode  bool
	queryMu          sync.Mutex
	queryUnavailable map[string]queryIndexFailure
	Objects          ObjectStore
	files            *FileStore
	viewsMu          sync.RWMutex
	readViews        []*ReaderCache
	viewGenerations  sync.Map
	unsubscribe      func()
	Backups          *backupstore.Repository

	Prefix string

	objectProbeMu            sync.Mutex
	objectProbeActive        *objectStoreProbeCall
	lockMu                   sync.Mutex
	tenantLocks              map[string]*tenantLock
	tenantRegistryMu         sync.Mutex
	writeCache               map[string]loadedGraph
	writeCacheOrder          []string
	writeCacheBytes          int64
	writerLeaseCache         map[string]cachedWriterLease
	registeredTenantCache    map[string]registeredTenantCacheEntry
	collectorStatusCache     map[string]cachedCollectorStatus
	readerHeartbeatCache     map[string]cachedReaderHeartbeat
	objectKeyCache           map[string]struct{}
	tenantMetadataCache      map[string]cachedTenantMetadata
	purgeTombstoneCache      map[string]cachedTenantPurgeTombstone
	sourcePolicyCache        map[string]cachedSourcePolicy
	tenantConfigCache        map[string]cachedTenantConfig
	indexCatalogCache        map[string]cachedIndexCatalog
	indexCatalogLoads        map[string]*indexCatalogLoad
	indexUpdateMu            sync.Mutex
	indexUpdateTails         map[string]chan struct{}
	indexUnreservedTails     map[string]chan struct{}
	pendingIngestIndexes     map[string]*commitIndexUpdate
	maintenance              *maintenanceResources
	activeIngestIndexUpdates int
	reverseIndexCatalogCache map[string]cachedReverseIndexCatalog
	reverseIndexCatalogLoads map[string]*reverseIndexCatalogLoad
	compiledScanCatalogCache map[string]*compiledScanCatalog
	indexCache               *indexObjectCache
	entityPageCache          *entityPageCache
	edgeLookupCache          *edgeLookupCache
	taskMu                   sync.Mutex
	taskCancels              map[string]context.CancelFunc
	taskActive               map[string]Task
	taskWorkers              sync.WaitGroup
	backgroundCtx            context.Context
	stopBackground           context.CancelFunc
	taskClosing              bool
	taskShutdownOnce         sync.Once
	taskShutdownDone         chan struct{}
	taskQueueSlots           chan struct{}
	taskExecutionSlots       chan struct{}
	taskBackupSlots          chan struct{}
	taskResidentSlots        chan struct{}
	taskTenantSlots          []chan struct{}
	InstanceID               string
	ReaderID                 string
	LifecycleCacheTTL        time.Duration
	TaskPersistenceTimeout   time.Duration
	MaxRetries               int

	MaxWriteCacheTenants       int
	MaxMaintenanceBytes        int64
	MaxWriteCacheBytes         int64
	EntityPagePackMaxBytes     int64
	IndexPrefetchTimeout       time.Duration
	WriteEntityRecords         bool
	UseEntityRecordsForRead    bool
	MaterializeCollectorStatus bool
	Backpressure               *WritePressure
	backpressureObserver       BackpressureObserver
	cacheObserver              ReaderCacheObserver

	ingestBarrier func(context.Context, string) error
}

type loadedGraph struct {
	Graph      *graph.Graph
	Manifest   Manifest
	Meta       ObjectMeta
	DataHash   string
	CommitTail commitTailCache
	CacheBytes int64
}

func NewTenantStore(objects ObjectStore, prefix string) *TenantStore {
	instanceID, err := newCommitID()
	if err != nil {
		instanceID = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	backgroundCtx, stopBackground := context.WithCancel(context.Background())
	store := &TenantStore{
		backgroundCtx: backgroundCtx, stopBackground: stopBackground,
		Objects:                  objects,
		files:                    exclusiveFileStore(objects),
		Prefix:                   cleanPrefix(prefix),
		tenantLocks:              map[string]*tenantLock{},
		writeCache:               map[string]loadedGraph{},
		writerLeaseCache:         map[string]cachedWriterLease{},
		registeredTenantCache:    map[string]registeredTenantCacheEntry{},
		collectorStatusCache:     map[string]cachedCollectorStatus{},
		readerHeartbeatCache:     map[string]cachedReaderHeartbeat{},
		objectKeyCache:           map[string]struct{}{},
		tenantMetadataCache:      map[string]cachedTenantMetadata{},
		purgeTombstoneCache:      map[string]cachedTenantPurgeTombstone{},
		sourcePolicyCache:        map[string]cachedSourcePolicy{},
		tenantConfigCache:        map[string]cachedTenantConfig{},
		indexCatalogCache:        map[string]cachedIndexCatalog{},
		indexCatalogLoads:        map[string]*indexCatalogLoad{},
		indexUpdateTails:         map[string]chan struct{}{},
		indexUnreservedTails:     map[string]chan struct{}{},
		reverseIndexCatalogCache: map[string]cachedReverseIndexCatalog{},
		reverseIndexCatalogLoads: map[string]*reverseIndexCatalogLoad{},
		compiledScanCatalogCache: map[string]*compiledScanCatalog{},
		indexCache:               newIndexObjectCache(4096),
		entityPageCache:          newEntityPageCache(2048),
		edgeLookupCache:          newEdgeLookupCache(2048, defaultEdgeLookupCacheMaxBytes),
		taskCancels:              map[string]context.CancelFunc{},
		taskActive:               map[string]Task{},
		taskQueueSlots:           make(chan struct{}, defaultTaskQueueLimit),
		taskExecutionSlots:       make(chan struct{}, defaultTaskExecutionLimit),
		taskBackupSlots:          make(chan struct{}, 2),
		taskResidentSlots:        make(chan struct{}, defaultTaskExecutionLimit),
		taskTenantSlots:          newTaskTenantSlots(defaultTaskTenantStripes),
		InstanceID:               instanceID,
		ReaderID:                 instanceID,
		LifecycleCacheTTL:        time.Second,
		TaskPersistenceTimeout:   10 * time.Second,
		MaxRetries:               3,

		MaxWriteCacheTenants:       64,
		MaxWriteCacheBytes:         512 * 1024 * 1024,
		MaxMaintenanceBytes:        defaultMaintenanceBytes,
		maintenance:                &maintenanceResources{builds: make(chan struct{}, 2), encodes: make(chan struct{}, indexWriteConcurrency)},
		EntityPagePackMaxBytes:     defaultEntityPagePackMaxBytes,
		IndexPrefetchTimeout:       defaultIndexObjectPrefetchTimeout,
		WriteEntityRecords:         true,
		MaterializeCollectorStatus: true,
	}
	if source, ok := unwrapTenantMigrationStore(objects).(interface{ OnChange(func(string)) func() }); ok {
		store.unsubscribe = source.OnChange(store.localFileChanged)
	}
	return store
}

func (s *TenantStore) InitTenant(ctx context.Context, tenantID string) (Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Manifest{}, err
	}
	unlock, err := s.lockTenantForeground(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	defer unlock()
	boundCtx, err := s.acquireAndBindWriterFence(ctx, tenantID)
	if err != nil {
		return Manifest{}, err
	}
	ctx = boundCtx
	g, dataHash, cacheBytes, err := newEmptyTenantGraph()
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, UpdatedAt: mutationTime(ctx), DataHash: dataHash}
	meta, err := s.putManifestMeta(ctx, tenantID, manifest, ObjectMeta{Key: s.manifestKey(tenantID)})
	if err != nil {
		return manifest, err
	}
	if err := s.addTenantToRegistry(ctx, tenantID); err != nil {
		return manifest, fmt.Errorf("register tenant %q: %w", tenantID, err)
	}
	s.setWriteCache(tenantID, loadedGraph{
		Graph: g, Manifest: manifest, Meta: meta,
		DataHash:   dataHash,
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
	dataHash, logicalBytes, err := g.ContentHashWithLogicalSize()
	return g, dataHash, writeCacheBytesForGraph(g, logicalBytes), err
}

func (s *TenantStore) Commit(ctx context.Context, tenantID string, mutations graph.Mutations, opts CommitOptions) (Manifest, error) {
	result, err := s.CommitWithReport(ctx, tenantID, mutations, opts)
	return result.Manifest, err
}

func (s *TenantStore) Compact(ctx context.Context, tenantID string) (Manifest, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return Manifest{}, err
	}
	if err := s.CheckTaskDiskSpace(ctx, Task{TenantID: tenantID, Type: TaskTypeCompact}); err != nil {
		return Manifest{}, err
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
	dataHash := loaded.DataHash
	if !alreadyCompacted && dataHash == "" {
		dataHash, err = g.ContentHash()
		if err != nil {
			return Manifest{}, err
		}
	}
	ctx, releaseMemory, err := s.admitMaintenance(ctx, maintenanceGraphBytes(g))
	if err != nil {
		return Manifest{}, err
	}
	defer releaseMemory()
	var snapshotCatalog ShardedSnapshotCatalog
	var snapshotKey string
	if !alreadyCompacted {
		// Snapshot objects are versioned and immutable, so their expensive build
		// can run concurrently with foreground commits.
		snapshot := g.SnapshotForStorage()
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

	if alreadyCompacted {
		current, currentMeta, currentErr := s.getManifest(ctx, tenantID)
		if currentErr != nil {
			return Manifest{}, currentErr
		}
		if !cachedManifestMatches(loaded, current, currentMeta) {
			return Manifest{}, fmt.Errorf(
				"%w: manifest changed while compacting tenant %q",
				ErrConflict, tenantID,
			)
		}
		return current, nil
	}
	manifest, _, err = s.publishLocalCompaction(
		ctx, tenantID, loaded, snapshotKey, snapshotCatalog.Key, dataHash,
	)
	return manifest, err
}
