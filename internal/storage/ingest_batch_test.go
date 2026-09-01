package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestDurableBatchPublishesOneSegmentAndManifest(t *testing.T) {
	objects := newIngestBatchCountingStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.reset()

	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.CommitSegments) != 1 || len(manifest.CommitKeys) != 0 || manifest.CommitSegments[0].Count != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	if puts := objects.count(store.manifestKey("tenant-a")); puts != 1 {
		t.Fatalf("manifest PUTs = %d, want 1", puts)
	}
	if puts := objects.countPrefix(store.commitSegmentPrefix("tenant-a")); puts != 1 {
		t.Fatalf("segment PUTs = %d, want 1", puts)
	}
	if puts := objects.countLooseCommits(store.commitPrefix("tenant-a"), store.commitSegmentPrefix("tenant-a")); puts != 0 {
		t.Fatalf("loose commit PUTs = %d, want 0", puts)
	}
}

func TestCoordinatedIngestPublishSlotRejectsBeforeGraphLoadAndRestoresBatchContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const tenantID = "tenant-a"
	base := NewMemoryStore()
	objects := &ingestPublishSlotObjectStore{
		ObjectStore: base,
		gets:        map[string]int{},
	}
	coordinator := &ingestPublishSlotCoordinator{
		taskLeaseTestCoordinator: newTaskLeaseTestCoordinator(),
	}
	owner := NewTenantStore(objects, "test")
	owner.InstanceID = "writer-a"
	owner.LeaseTTL = time.Hour
	owner.SetCoordinator(coordinator)
	writer := NewTenantStore(objects, "test")
	writer.InstanceID = "writer-b"
	writer.LeaseTTL = time.Hour
	writer.SetCoordinator(coordinator)

	_, dataMD5, _, err := newEmptyTenantGraph()
	if err != nil {
		t.Fatalf("new empty graph: %v", err)
	}
	manifest := Manifest{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      tenantID,
		DataMD5:       dataMD5,
		UpdatedAt:     time.Unix(1, 0).UTC(),
	}
	manifestData, err := marshalParquetManifest(ctx, manifest)
	if err != nil {
		t.Fatalf("marshal initial manifest: %v", err)
	}
	manifestHash := objectContentHash(manifestData)
	manifestKey := owner.coordinatorManifestKey(tenantID, 0, 1, manifestHash)
	if err := base.Put(ctx, manifestKey, manifestData); err != nil {
		t.Fatalf("put initial manifest: %v", err)
	}
	registryData, err := marshalParquetTenantRegistry(ctx, tenantRegistry{TenantIDs: []string{tenantID}})
	if err != nil {
		t.Fatalf("marshal tenant registry: %v", err)
	}
	if err := base.Put(ctx, owner.tenantRegistryKey(), registryData); err != nil {
		t.Fatalf("put tenant registry: %v", err)
	}
	coordinator.head = CoordinationHead{
		TenantID:             tenantID,
		Generation:           1,
		Status:               TenantStatusActive,
		Revision:             1,
		GraphVersion:         0,
		ManifestKey:          manifestKey,
		ManifestHash:         manifestHash,
		WriteContextRevision: 0,
	}

	_, releaseOwner, err := owner.startCoordinatorOperationLease(ctx, tenantID, coordinatorIngestPublishTaskType)
	if err != nil {
		t.Fatalf("hold ingest publish slot: %v", err)
	}
	defer releaseOwner()
	objects.reset()
	coordinator.contended = make(chan struct{})
	request := ingestEntityRequest("batch-slot", "host:slot")
	blockedDone := make(chan error, 1)
	go func() {
		_, err := writer.IngestDurableBatch(ctx, tenantID, []IngestBatchEntry{{Request: request}})
		blockedDone <- err
	}()
	select {
	case <-coordinator.contended:
	case err := <-blockedDone:
		t.Fatalf("second writer returned before slot contention was observed: %v", err)
	case <-ctx.Done():
		t.Fatalf("waiting for second writer slot attempt: %v", ctx.Err())
	}
	select {
	case err := <-blockedDone:
		if !errors.Is(err, ErrTaskLeaseHeld) {
			t.Fatalf("second writer err = %v, want ErrTaskLeaseHeld", err)
		}
	case <-ctx.Done():
		t.Fatalf("waiting for second writer rejection: %v", ctx.Err())
	}
	if got := objects.getCount(manifestKey); got != 0 {
		t.Fatalf("manifest reads while publish slot was held = %d, want 0", got)
	}
	if got := objects.putCountPrefix(writer.commitSegmentPrefix(tenantID)); got != 0 {
		t.Fatalf("segment PUTs while publish slot was held = %d, want 0", got)
	}
	if got := objects.putCountPrefix(writer.coordinatorManifestPrefix(tenantID)); got != 0 {
		t.Fatalf("coordinator manifest PUTs while publish slot was held = %d, want 0", got)
	}
	if got := coordinator.publishCallCount(); got != 0 {
		t.Fatalf("PublishIngestBatch calls while publish slot was held = %d, want 0", got)
	}

	releaseOwner()
	objects.reset()
	type batchCall struct {
		results []IngestResult
		err     error
	}
	releaseCallsBefore := coordinator.releaseTaskLeaseCallCount()
	batchDone := make(chan batchCall, 1)
	go func() {
		results, err := writer.IngestDurableBatch(ctx, tenantID, []IngestBatchEntry{{Request: request}})
		batchDone <- batchCall{results: results, err: err}
	}()
	select {
	case batchResult := <-batchDone:
		if batchResult.err != nil {
			t.Fatalf("retry after publish slot release: %v", batchResult.err)
		}
		if len(batchResult.results) != 1 || batchResult.results[0].Version != 1 || batchResult.results[0].Applied != 1 || batchResult.results[0].Failed != 0 {
			t.Fatalf("retry result = %#v", batchResult.results)
		}
	case <-ctx.Done():
		t.Fatalf("successful publish slot retry did not return: %v", ctx.Err())
	}
	if got := objects.getCount(manifestKey); got == 0 {
		t.Fatal("retry did not load the coordinator manifest after acquiring the publish slot")
	}
	if got := objects.putCountPrefix(writer.commitSegmentPrefix(tenantID)); got != 1 {
		t.Fatalf("retry segment PUTs = %d, want 1", got)
	}
	if got := objects.putCountPrefix(writer.coordinatorManifestPrefix(tenantID)); got != 1 {
		t.Fatalf("retry coordinator manifest PUTs = %d, want 1", got)
	}
	if got := coordinator.publishCallCount(); got != 1 {
		t.Fatalf("retry PublishIngestBatch calls = %d, want 1", got)
	}
	if got := coordinator.publishLeaseReleaseCount(); got != 1 {
		t.Fatalf("transactional publish lease releases = %d, want 1", got)
	}
	if got := coordinator.releaseTaskLeaseCallCount(); got != releaseCallsBefore {
		t.Fatalf("successful publish called ReleaseTaskLease %d additional times", got-releaseCallsBefore)
	}
	if got := objects.canceledPutCount(); got != 0 {
		t.Fatalf("metadata PUTs observed canceled context = %d, want 0", got)
	}
	if _, active, err := coordinator.TaskLease(ctx, tenantID, coordinatorIngestPublishTaskType); err != nil {
		t.Fatalf("read publish slot after retry: %v", err)
	} else if active {
		t.Fatal("publish slot remained held after successful batch finalization")
	}
	for _, taskType := range coordinator.acquireTaskTypes() {
		if taskType != coordinatorIngestPublishTaskType {
			t.Fatalf("acquired task type = %q, want %q", taskType, coordinatorIngestPublishTaskType)
		}
	}
	if got := len(coordinator.acquireTaskTypes()); got != 1 {
		t.Fatalf("legacy task-lease acquisition attempts = %d, want only owner hold", got)
	}
	if got := coordinator.fastAcquireCallCount(); got != 2 {
		t.Fatalf("fast publish-slot acquisition attempts = %d, want contention and retry", got)
	}
	lastAcquireHeadCalls := coordinator.fastAcquireHeadCallCount()
	if got := coordinator.headCallCount(); got != lastAcquireHeadCalls {
		t.Fatalf("Coordinator.Head calls after the last fast acquire = %d (snapshots=%v total=%d)", got-lastAcquireHeadCalls, coordinator.fastAcquireHeadCallCounts(), got)
	}
	for _, head := range coordinator.fastAcquireHeads() {
		if head != coordinator.head {
			t.Fatalf("fast acquire head = %#v, want %#v", head, coordinator.head)
		}
	}
	record, err := writer.GetIngestBatch(ctx, tenantID, request.Source, request.CollectorID, request.BatchID)
	if err != nil {
		t.Fatalf("load finalized ingest metadata: %v", err)
	}
	if record.Result.Version != 1 || record.Result.Applied != 1 {
		t.Fatalf("finalized ingest record = %#v", record)
	}

	writer.Objects = &failPutStore{ObjectStore: objects, contains: writer.commitSegmentPrefix(tenantID)}
	errorReleaseStarted, errorReleaseGate, errorReleaseDone := coordinator.blockNextRelease()
	errorDone := make(chan error, 1)
	errorRequest := ingestEntityRequest("batch-slot-error", "host:slot-error")
	go func() {
		_, err := writer.IngestDurableBatch(ctx, tenantID, []IngestBatchEntry{{Request: errorRequest}})
		errorDone <- err
	}()
	select {
	case <-errorReleaseStarted:
	case <-ctx.Done():
		t.Fatalf("waiting for failed publish slot release: %v", ctx.Err())
	}
	select {
	case err := <-errorDone:
		t.Fatalf("error path returned before synchronous release completed: %v", err)
	default:
	}
	close(errorReleaseGate)
	select {
	case <-errorReleaseDone:
	case <-ctx.Done():
		t.Fatalf("waiting for failed publish slot release completion: %v", ctx.Err())
	}
	if err := <-errorDone; err == nil || !strings.Contains(err.Error(), "injected put failure") {
		t.Fatalf("failed publish result error = %v, want injected publish failure", err)
	}
	if _, active, err := coordinator.TaskLease(ctx, tenantID, coordinatorIngestPublishTaskType); err != nil {
		t.Fatalf("read publish slot after failed batch: %v", err)
	} else if active {
		t.Fatal("publish slot remained held after failed batch release")
	}
}

type ingestPublishSlotCoordinator struct {
	*taskLeaseTestCoordinator
	mu                    sync.Mutex
	publishCalls          int
	acquireTypes          []string
	fastAcquireCalls      int
	fastHeads             []CoordinationHead
	fastAcquireHeadCalls  []int
	headCalls             int
	publishLeaseReleases  int
	releaseTaskLeaseCalls int
	releaseGate           chan struct{}
	releaseStarted        chan struct{}
	releaseFinished       chan struct{}
}

func (c *ingestPublishSlotCoordinator) AcquireIngestPublishSlot(
	ctx context.Context,
	tenantID string,
	owner string,
	ttl time.Duration,
) (CoordinatorTaskLease, CoordinationHead, bool, bool, error) {
	c.mu.Lock()
	c.fastAcquireCalls++
	c.fastAcquireHeadCalls = append(c.fastAcquireHeadCalls, c.headCalls)
	head := c.taskLeaseTestCoordinator.head
	c.fastHeads = append(c.fastHeads, head)
	c.mu.Unlock()
	lease, acquired, err := c.taskLeaseTestCoordinator.AcquireTaskLease(
		ctx, tenantID, coordinatorIngestPublishTaskType, owner, ttl,
	)
	if err != nil || !acquired {
		return CoordinatorTaskLease{}, CoordinationHead{}, false, acquired, err
	}
	return lease, head, true, true, nil
}

func (c *ingestPublishSlotCoordinator) Head(
	_ context.Context,
	_ string,
) (CoordinationHead, bool, error) {
	c.mu.Lock()
	c.headCalls++
	head := c.taskLeaseTestCoordinator.head
	c.mu.Unlock()
	return head, true, nil
}

func (c *ingestPublishSlotCoordinator) AcquireTaskLease(
	ctx context.Context,
	tenantID string,
	taskType string,
	owner string,
	ttl time.Duration,
) (CoordinatorTaskLease, bool, error) {
	c.mu.Lock()
	c.acquireTypes = append(c.acquireTypes, taskType)
	c.mu.Unlock()
	return c.taskLeaseTestCoordinator.AcquireTaskLease(ctx, tenantID, taskType, owner, ttl)
}

func (c *ingestPublishSlotCoordinator) ReserveCommit(
	_ context.Context,
	_ string,
	key string,
	requestHash string,
	ownerToken string,
	_ time.Duration,
) (CommitReservation, error) {
	return CommitReservation{Key: key, RequestHash: requestHash, OwnerToken: ownerToken}, nil
}

func (c *ingestPublishSlotCoordinator) PublishIngestBatch(
	_ context.Context,
	request IngestBatchPublishRequest,
) (CoordinationHead, bool, error) {
	c.mu.Lock()
	c.publishCalls++
	c.mu.Unlock()
	head := CoordinationHead{
		TenantID:             request.Head.TenantID,
		Generation:           request.Head.ExpectedGeneration,
		Status:               TenantStatusActive,
		Revision:             request.Head.ExpectedRevision + 1,
		GraphVersion:         request.Head.GraphVersion,
		ManifestKey:          request.Head.ManifestKey,
		ManifestHash:         request.Head.ManifestHash,
		CommitID:             request.Head.CommitID,
		WriteContextRevision: request.Head.ExpectedWriteContextRevision,
	}
	if request.PublishLease != nil {
		if err := c.releasePublishedLease(*request.PublishLease); err != nil {
			return CoordinationHead{}, false, err
		}
	}
	return head, true, nil
}

func (c *ingestPublishSlotCoordinator) releasePublishedLease(lease CoordinatorTaskLease) error {
	c.taskLeaseTestCoordinator.mu.Lock()
	key := lease.TenantID + "\x00" + lease.TaskType
	current := c.taskLeaseTestCoordinator.leases[key]
	if current.OwnerToken != lease.OwnerToken || current.FenceEpoch != lease.FenceEpoch {
		c.taskLeaseTestCoordinator.mu.Unlock()
		return ErrConflict
	}
	current.OwnerToken = ""
	current.ExpiresAt = time.Now().UTC().Add(-time.Second)
	c.taskLeaseTestCoordinator.leases[key] = current
	c.taskLeaseTestCoordinator.mu.Unlock()
	c.mu.Lock()
	c.publishLeaseReleases++
	c.mu.Unlock()
	return nil
}

func (c *ingestPublishSlotCoordinator) blockNextRelease() (chan struct{}, chan struct{}, chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.releaseGate != nil {
		panic("publish slot release barrier already armed")
	}
	started := make(chan struct{})
	gate := make(chan struct{})
	finished := make(chan struct{})
	c.releaseStarted = started
	c.releaseGate = gate
	c.releaseFinished = finished
	return started, gate, finished
}

func (c *ingestPublishSlotCoordinator) ReleaseTaskLease(
	ctx context.Context,
	lease CoordinatorTaskLease,
) error {
	c.mu.Lock()
	c.releaseTaskLeaseCalls++
	gate := c.releaseGate
	started := c.releaseStarted
	finished := c.releaseFinished
	if gate != nil {
		c.releaseGate = nil
		c.releaseStarted = nil
		c.releaseFinished = nil
	}
	c.mu.Unlock()
	if gate != nil {
		close(started)
		defer close(finished)
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return c.taskLeaseTestCoordinator.ReleaseTaskLease(ctx, lease)
}

func (c *ingestPublishSlotCoordinator) publishCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.publishCalls
}

func (c *ingestPublishSlotCoordinator) publishLeaseReleaseCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.publishLeaseReleases
}

func (c *ingestPublishSlotCoordinator) releaseTaskLeaseCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.releaseTaskLeaseCalls
}

func (c *ingestPublishSlotCoordinator) fastAcquireCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fastAcquireCalls
}

func (c *ingestPublishSlotCoordinator) fastAcquireHeads() []CoordinationHead {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]CoordinationHead(nil), c.fastHeads...)
}

func (c *ingestPublishSlotCoordinator) fastAcquireHeadCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.fastAcquireHeadCalls) == 0 {
		return 0
	}
	return c.fastAcquireHeadCalls[len(c.fastAcquireHeadCalls)-1]
}

func (c *ingestPublishSlotCoordinator) fastAcquireHeadCallCounts() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.fastAcquireHeadCalls...)
}

func (c *ingestPublishSlotCoordinator) headCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.headCalls
}

func (c *ingestPublishSlotCoordinator) acquireTaskTypes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.acquireTypes...)
}

type ingestPublishSlotObjectStore struct {
	ObjectStore
	mu           sync.Mutex
	gets         map[string]int
	puts         []string
	canceledPuts int
}

func (s *ingestPublishSlotObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.recordGet(key)
	return s.ObjectStore.Get(ctx, key)
}

func (s *ingestPublishSlotObjectStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	s.recordGet(key)
	return s.ObjectStore.GetWithMeta(ctx, key)
}

func (s *ingestPublishSlotObjectStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	s.mu.Lock()
	s.puts = append(s.puts, key)
	if ctx != nil && ctx.Err() != nil {
		s.canceledPuts++
	}
	s.mu.Unlock()
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *ingestPublishSlotObjectStore) recordGet(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets[key]++
}

func (s *ingestPublishSlotObjectStore) reset() {
	s.mu.Lock()
	s.gets = map[string]int{}
	s.puts = nil
	s.canceledPuts = 0
	s.mu.Unlock()
}

func (s *ingestPublishSlotObjectStore) getCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[key]
}

func (s *ingestPublishSlotObjectStore) putCountPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.puts {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func (s *ingestPublishSlotObjectStore) canceledPutCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.canceledPuts
}

func TestIngestDurableBatchPublishedHookRunsAfterManifestPublish(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	var hookCalls int
	var hookManifest Manifest
	var hookErr error
	results, err := store.IngestDurableBatchWithHooks(ctx, "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
	}, IngestBatchHooks{
		Published: func() {
			hookCalls++
			hookManifest, hookErr = store.CurrentManifest(ctx, "tenant-a")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Version != 1 {
		t.Fatalf("results = %#v", results)
	}
	if hookCalls != 1 || hookErr != nil || hookManifest.Version != 1 {
		t.Fatalf("published hook calls=%d manifest=%#v err=%v, want one post-publish callback", hookCalls, hookManifest, hookErr)
	}
}

func TestIngestDurableBatchMergesExistingLooseTail(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	before, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(before.CommitKeys) != 1 {
		t.Fatalf("seed manifest = %#v, want one loose commit", before)
	}
	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 2 || results[1].Version != 3 {
		t.Fatalf("results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(manifest.CommitKeys) != 0 || len(manifest.CommitSegments) != 1 {
		t.Fatalf("manifest tail = %#v", manifest)
	}
	ref := manifest.CommitSegments[0]
	if ref.FirstVersion != 1 || ref.LastVersion != 3 || ref.Count != 3 {
		t.Fatalf("segment ref = %#v", ref)
	}
	reloaded := NewTenantStore(store.Objects, "test")
	loaded, loadedManifest, err := reloaded.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if loadedManifest.Version != 3 || len(loaded.Entities) != 3 {
		t.Fatalf("reloaded graph/manifest = %d entities, version %d", len(loaded.Entities), loadedManifest.Version)
	}
}

func TestIngestDurableBatchIsolatesBadRequestAndContinues(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	bad := IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     "bad",
		Items: []IngestItem{{
			Edge: &graph.Edge{Type: "runs_on", From: "service:missing", To: "host:missing"},
		}},
	}
	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: bad},
		{Request: ingestEntityRequest("good", "host:1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Applied != 0 || results[0].Failed != 1 || results[1].Version != 1 {
		t.Fatalf("results = %#v", results)
	}
	loaded, manifest, err := store.Load(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 1 {
		t.Fatalf("manifest version = %d, want 1", manifest.Version)
	}
	if _, ok := loaded.GetEntity("host:1"); !ok {
		t.Fatal("valid request after bad request was not applied")
	}
}

func TestIngestDurableBatchDuplicateContentDoesNotConsumeVersion(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	first := ingestEntityRequest("batch-1", "host:1")
	second := ingestEntityRequest("batch-2", "host:1")
	results, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: first, AcceptedAt: time.Unix(1, 0).UTC()},
		{Request: second, AcceptedAt: time.Unix(2, 0).UTC()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 1 || results[1].Version != 1 || !results[1].Skipped || results[1].SkipReason != IngestSkipReasonLogicalNoop {
		t.Fatalf("results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 1 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestIngestDurableBatchRejectsProjectedCommitTailOverflow(t *testing.T) {
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	store.Backpressure = NewWritePressure(BackpressureConfig{MaxCommitTail: 1})
	_, err := store.IngestDurableBatch(context.Background(), "tenant-a", []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	})
	assertBackpressureReason(t, err, "commit_tail_too_long")
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 0 || len(manifest.CommitSegments) != 0 {
		t.Fatalf("manifest after rejected flush = %#v", manifest)
	}
}

func TestIngestDurableBatchMetadataRetryDoesNotRepublishManifest(t *testing.T) {
	objects := &failIngestRecordOnceStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	first := ingestEntityRequest("batch-1", "host:1")
	second := ingestEntityRequest("batch-2", "host:2")
	objects.failKey = store.ingestBatchKey("tenant-a", first.Source, first.CollectorID, first.BatchID)
	entries := []IngestBatchEntry{{Request: first}, {Request: second}}
	var plans []*IngestPreparedRequest
	results, err := store.IngestDurableBatchWithHooks(
		context.Background(),
		"tenant-a",
		entries,
		IngestBatchHooks{
			Prepared: func(_ context.Context, prepared []*IngestPreparedRequest) error {
				plans = prepared
				return nil
			},
		},
	)
	if err == nil || results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("first flush results/err = %#v / %v", results, err)
	}
	if len(plans) != len(entries) || plans[0] == nil || plans[1] == nil {
		t.Fatalf("prepared plans = %#v", plans)
	}
	for index := range entries {
		entries[index].Prepared = plans[index]
	}
	results, err = store.IngestDurableBatch(context.Background(), "tenant-a", entries)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("retry results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.CommitSegments) != 1 {
		t.Fatalf("manifest = %#v", manifest)
	}
}

func TestIngestDurableBatchReplaysPreparedPlanAfterManifestFailure(t *testing.T) {
	base := NewMemoryStore()
	objects := &failIngestRecordOnceStore{ObjectStore: base}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(context.Background(), "tenant-a"); err != nil {
		t.Fatal(err)
	}
	objects.failKey = store.manifestKey("tenant-a")
	entries := []IngestBatchEntry{
		{Request: ingestEntityRequest("batch-1", "host:1")},
		{Request: ingestEntityRequest("batch-2", "host:2")},
	}
	var plans []*IngestPreparedRequest
	results, err := store.IngestDurableBatchWithHooks(
		context.Background(),
		"tenant-a",
		entries,
		IngestBatchHooks{
			Prepared: func(_ context.Context, prepared []*IngestPreparedRequest) error {
				plans = prepared
				return nil
			},
		},
	)
	if err == nil {
		t.Fatalf("first flush unexpectedly succeeded: %#v", results)
	}
	for index := range entries {
		if plans[index] == nil {
			t.Fatalf("prepared plan %d is nil", index)
		}
		entries[index].Prepared = plans[index]
	}
	if plans[0].Result.Version != 1 || plans[1].Result.Version != 2 {
		t.Fatalf("prepared results = %#v", plans)
	}
	results, err = store.IngestDurableBatch(context.Background(), "tenant-a", entries)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Version != 1 || results[1].Version != 2 {
		t.Fatalf("retry results = %#v", results)
	}
	manifest, err := store.CurrentManifest(context.Background(), "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != 2 || len(manifest.CommitSegments) != 1 || manifest.CommitSegments[0].Count != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	segments, err := base.List(context.Background(), store.commitSegmentPrefix("tenant-a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 {
		t.Fatalf("commit segment objects = %#v", segments)
	}
}

func ingestEntityRequest(batchID string, entityID string) IngestRequest {
	return IngestRequest{
		Source:      "agent",
		CollectorID: "collector-a",
		BatchID:     batchID,
		Items: []IngestItem{{
			ExternalID: entityID,
			Entity: &graph.Entity{
				ID: entityID, Kind: "host", Fields: graph.Fields{"name": entityID},
			},
		}},
	}
}

type ingestBatchCountingStore struct {
	ObjectStore
	mu   sync.Mutex
	puts []string
}

func newIngestBatchCountingStore(inner ObjectStore) *ingestBatchCountingStore {
	return &ingestBatchCountingStore{ObjectStore: inner}
}

func (s *ingestBatchCountingStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	s.mu.Lock()
	s.puts = append(s.puts, key)
	s.mu.Unlock()
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *ingestBatchCountingStore) reset() {
	s.mu.Lock()
	s.puts = nil
	s.mu.Unlock()
}

func (s *ingestBatchCountingStore) count(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, current := range s.puts {
		if current == key {
			count++
		}
	}
	return count
}

func (s *ingestBatchCountingStore) countPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.puts {
		if strings.HasPrefix(key, prefix) {
			count++
		}
	}
	return count
}

func (s *ingestBatchCountingStore) countLooseCommits(commitPrefix string, segmentPrefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.puts {
		if strings.HasPrefix(key, commitPrefix) && !strings.HasPrefix(key, segmentPrefix) {
			count++
		}
	}
	return count
}
