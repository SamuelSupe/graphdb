package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

type putCountingStore struct {
	ObjectStore
	puts             int
	gets             int
	deletes          int
	heartbeatGets    int
	heartbeatDeletes int
	pageCalls        int
	lastPrefix       string
	lastPageLen      int
}

func (s *putCountingStore) ListPage(ctx context.Context, prefix string, after string, limit int) ([]ObjectInfo, string, error) {
	items, next, err := listObjectPage(ctx, s.ObjectStore, prefix, after, limit)
	s.pageCalls++
	s.lastPrefix = prefix
	s.lastPageLen = len(items)
	return items, next, err
}

func (s *putCountingStore) Put(ctx context.Context, key string, data []byte) error {
	s.puts++
	return s.ObjectStore.Put(ctx, key, data)
}

func (s *putCountingStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.gets++
	if strings.Contains(key, "/control/readers/") {
		s.heartbeatGets++
	}
	return s.ObjectStore.Get(ctx, key)
}

func (s *putCountingStore) Delete(ctx context.Context, key string) error {
	s.deletes++
	if strings.Contains(key, "/control/readers/") {
		s.heartbeatDeletes++
	}
	return s.ObjectStore.Delete(ctx, key)
}

func TestReaderHeartbeatWritesAreRateLimitedAndStateSensitive(t *testing.T) {
	ctx := context.Background()
	objects := &putCountingStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	first := ReaderHeartbeat{ReaderID: "reader-a", Status: "fresh", VisibleVersion: 1, ManifestVersion: 1, LastSeenAt: time.Now().UTC()}
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", first); err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	second := first
	second.LastSeenAt = second.LastSeenAt.Add(time.Second)
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", second); err != nil {
		t.Fatalf("rate-limited heartbeat: %v", err)
	}
	if objects.puts != 1 {
		t.Fatalf("heartbeat puts = %d, want 1", objects.puts)
	}
	second.VisibleVersion = 2
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", second); err != nil {
		t.Fatalf("changed heartbeat: %v", err)
	}
	if objects.puts != 2 {
		t.Fatalf("heartbeat puts after state change = %d, want 2", objects.puts)
	}
}

func TestListReaderHeartbeatsDeletesExpiredRecords(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	heartbeat := ReaderHeartbeat{ReaderID: "reader-old", Status: "fresh", VisibleVersion: 1, LastSeenAt: time.Now().UTC().Add(-time.Hour)}
	if _, err := store.PutReaderHeartbeat(ctx, "tenant-a", heartbeat); err != nil {
		t.Fatalf("put heartbeat: %v", err)
	}
	items, err := store.ListReaderHeartbeatsWithOptions(ctx, "tenant-a", ReaderHeartbeatListOptions{
		MaxAge:        time.Minute,
		Limit:         10,
		DeleteExpired: true,
	})
	if err != nil {
		t.Fatalf("list heartbeats: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("heartbeats = %#v", items)
	}
	if _, err := store.Objects.Get(ctx, store.readerHeartbeatKey("tenant-a", "reader-old")); err != ErrNotFound {
		t.Fatalf("expired heartbeat get err = %v, want ErrNotFound", err)
	}
}

func TestReaderHeartbeatLegacyCleanupHasTotalWorkBudget(t *testing.T) {
	ctx := context.Background()
	objects := &putCountingStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	for i := 0; i < 130; i++ {
		_, err := store.PutReaderHeartbeat(ctx, "tenant-a", ReaderHeartbeat{
			ReaderID:       fmt.Sprintf("reader-%03d", i),
			Status:         "fresh",
			VisibleVersion: 1,
			LastSeenAt:     time.Now().UTC().Add(-time.Hour),
		})
		if err != nil {
			t.Fatalf("put heartbeat %d: %v", i, err)
		}
	}
	objects.gets = 0
	objects.deletes = 0
	objects.heartbeatGets = 0
	objects.heartbeatDeletes = 0
	options := ReaderHeartbeatListOptions{MaxAge: time.Minute, Limit: 4096, ScanLimit: 64, DeleteExpired: true}
	if _, err := store.ListReaderHeartbeatsWithOptions(ctx, "tenant-a", options); !errors.Is(err, errReaderHeartbeatScanIncomplete) {
		t.Fatalf("first cleanup err = %v, want incomplete", err)
	}
	if objects.gets != 64 || objects.deletes != 64 {
		t.Fatalf("first cleanup gets/deletes = %d/%d, want 64/64", objects.gets, objects.deletes)
	}
	for attempt := 0; attempt < 3; attempt++ {
		_, err := store.ListReaderHeartbeatsWithOptions(ctx, "tenant-a", options)
		if err == nil {
			break
		}
		if !errors.Is(err, errReaderHeartbeatScanIncomplete) {
			t.Fatalf("cleanup attempt %d: %v", attempt, err)
		}
	}
	items, err := store.Objects.List(ctx, store.readerHeartbeatPrefix("tenant-a"))
	if err != nil {
		t.Fatalf("list remaining heartbeats: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("remaining heartbeat objects = %d, want 0", len(items))
	}
}

func TestGCFailsClosedWhenHeartbeatInventoryExceedsScanBudget(t *testing.T) {
	ctx := context.Background()
	objects := &putCountingStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	for i := 0; i < 70; i++ {
		_, err := store.PutReaderHeartbeat(ctx, "tenant-a", ReaderHeartbeat{
			ReaderID:       fmt.Sprintf("reader-%03d", i),
			Status:         "fresh",
			VisibleVersion: 1,
			LastSeenAt:     time.Now().UTC().Add(-time.Hour),
		})
		if err != nil {
			t.Fatalf("put heartbeat %d: %v", i, err)
		}
	}
	objects.gets = 0
	objects.deletes = 0
	_, err := store.RunGC(ctx, "tenant-a", GCOptions{ReaderMaxAge: time.Minute, ReaderScanLimit: 64, MaxDeletes: 1})
	if !errors.Is(err, errReaderHeartbeatScanIncomplete) {
		t.Fatalf("gc err = %v, want incomplete heartbeat scan", err)
	}
	if objects.heartbeatGets > 64 || objects.heartbeatDeletes > 64 {
		t.Fatalf("gc heartbeat gets/deletes = %d/%d, want <=64", objects.heartbeatGets, objects.heartbeatDeletes)
	}
}

func TestCollectorStatusCacheIsBoundedAndExpires(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	for i := 0; i <= collectorStatusCacheLimit; i++ {
		key := fmt.Sprintf("status-%05d", i)
		store.setCachedCollectorStatus(key, CollectorStatus{CollectorID: key}, ObjectMeta{Key: key})
	}
	if got := len(store.collectorStatusCache); got != collectorStatusCacheLimit {
		t.Fatalf("collector status cache size = %d, want %d", got, collectorStatusCacheLimit)
	}
	key := "expired"
	store.setCachedCollectorStatus(key, CollectorStatus{CollectorID: key}, ObjectMeta{Key: key})
	store.lockMu.Lock()
	cached := store.collectorStatusCache[key]
	cached.lastAccess = time.Now().Add(-collectorStatusCacheTTL - time.Second)
	store.collectorStatusCache[key] = cached
	store.lockMu.Unlock()
	if _, _, ok := store.getCachedCollectorStatus(key); ok {
		t.Fatal("expired collector status remained cached")
	}
}

func TestTaskAdmissionDeduplicatesAndBoundsQueue(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	tasks := make([]Task, 0, defaultTaskQueueLimit)
	for i := 0; i < defaultTaskQueueLimit; i++ {
		task := Task{ID: fmt.Sprintf("task-%03d", i), TenantID: "tenant-a", Type: fmt.Sprintf("type-%03d", i), Status: TaskStatusQueued}
		if _, reused, err := store.admitTask(task); err != nil || reused {
			t.Fatalf("admit task %d: reused=%v err=%v", i, reused, err)
		}
		tasks = append(tasks, task)
	}
	duplicate := tasks[0]
	duplicate.ID = "duplicate"
	active, reused, err := store.admitTask(duplicate)
	if err != nil || !reused || active.ID != tasks[0].ID {
		t.Fatalf("duplicate admission = active %#v reused=%v err=%v", active, reused, err)
	}
	if _, _, err := store.admitTask(Task{ID: "overflow", TenantID: "tenant-b", Type: "overflow"}); err == nil {
		t.Fatal("task admission accepted queue overflow")
	}
	for _, task := range tasks {
		store.releaseTaskAdmission(task)
	}
	if len(store.taskActive) != 0 || len(store.taskQueueSlots) != 0 {
		t.Fatalf("task admission state active=%d queued=%d", len(store.taskActive), len(store.taskQueueSlots))
	}
}

func TestQueuedTaskCancellationPersistsBeforeExecutionSlot(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	task := Task{ID: "task-canceled-in-queue", TenantID: "tenant-a", Type: TaskTypeCompact, Status: TaskStatusQueued, Phase: TaskStatusQueued, StartedAt: time.Now().UTC()}
	if _, reused, err := store.admitTask(task); err != nil || reused {
		t.Fatalf("admit task: reused=%v err=%v", reused, err)
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatalf("save task: %v", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	cancel()
	store.runTaskAdmitted(runCtx, func() {}, task)
	loaded, err := store.GetTask(ctx, task.TenantID, task.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if loaded.Status != TaskStatusCanceled || loaded.FinishedAt.IsZero() {
		t.Fatalf("queued task = %#v, want persisted cancellation", loaded)
	}
}

func TestGCBoundsCommitDecodeWorkWhenDeleteBudgetSet(t *testing.T) {
	ctx := context.Background()
	objects := &putCountingStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	for i := 1; i <= 200; i++ {
		key := store.commitKey("tenant-a", int64(i), fmt.Sprintf("invalid-%03d", i))
		if err := store.Objects.Put(ctx, key, []byte("invalid")); err != nil {
			t.Fatalf("put invalid commit: %v", err)
		}
	}
	objects.gets = 0
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{MaxDeletes: 1})
	if err != nil {
		t.Fatalf("run gc: %v", err)
	}
	if !report.Checkpoint.Paused || report.Checkpoint.NextCursor == "" {
		t.Fatalf("gc checkpoint = %#v, want bounded pause", report.Checkpoint)
	}
	if objects.gets > 70 {
		t.Fatalf("gc object gets = %d, want bounded page", objects.gets)
	}
}

func TestGCPausesAfterBoundedNoDeletePage(t *testing.T) {
	ctx := context.Background()
	objects := &putCountingStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	prefix := store.tenantObjectPrefix("tenant-a") + "ingest/source-a/batches/collector-a/"
	for i := 0; i < 200; i++ {
		if err := store.Objects.Put(ctx, prefix+fmt.Sprintf("batch-%03d.parquet", i), []byte("not-a-deadletter")); err != nil {
			t.Fatalf("put ingest object: %v", err)
		}
	}
	report, err := store.RunGC(ctx, "tenant-a", GCOptions{DeadLetterMaxAge: time.Hour, MaxDeletes: 1})
	if err != nil {
		t.Fatalf("run gc: %v", err)
	}
	if !report.Checkpoint.Paused || report.Checkpoint.Deleted != 0 {
		t.Fatalf("gc checkpoint = %#v, want no-delete bounded pause", report.Checkpoint)
	}
	if objects.lastPrefix != store.tenantObjectPrefix("tenant-a")+"ingest/" || objects.lastPageLen > 64 {
		t.Fatalf("last page prefix=%q len=%d, want bounded ingest page", objects.lastPrefix, objects.lastPageLen)
	}
	cursor := report.Checkpoint.NextCursor
	for run := 0; run < 10 && report.Checkpoint.Paused; run++ {
		report, err = store.RunGC(ctx, "tenant-a", GCOptions{DeadLetterMaxAge: time.Hour, MaxDeletes: 1, CheckpointCursor: cursor})
		if err != nil {
			t.Fatalf("resume gc: %v", err)
		}
		if report.Checkpoint.Paused && report.Checkpoint.NextCursor == cursor {
			t.Fatalf("gc cursor did not advance from %q", cursor)
		}
		cursor = report.Checkpoint.NextCursor
	}
	if report.Checkpoint.Paused || !report.Checkpoint.Completed {
		t.Fatalf("resumed gc checkpoint = %#v, want completion", report.Checkpoint)
	}
}

func TestGCResumesAllNestedCommitSegmentPages(t *testing.T) {
	ctx := context.Background()
	objects := &putCountingStore{ObjectStore: NewMemoryStore()}
	store := NewTenantStore(objects, "test")
	if _, err := store.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	for i := 0; i < 130; i++ {
		key := store.commitSegmentPrefix("tenant-a") + fmt.Sprintf("%020d-%020d-invalid.parquet", i+1, i+1)
		if err := store.Objects.Put(ctx, key, []byte("invalid-segment")); err != nil {
			t.Fatalf("put segment: %v", err)
		}
	}
	cursor := ""
	seenInvalid := map[string]struct{}{}
	completed := false
	for run := 0; run < 10; run++ {
		report, err := store.RunGC(ctx, "tenant-a", GCOptions{MaxDeletes: 1, CheckpointCursor: cursor})
		if err != nil {
			t.Fatalf("run gc %d: %v", run, err)
		}
		for _, key := range report.CommitCleanup.InvalidKeys {
			seenInvalid[key] = struct{}{}
		}
		if !report.Checkpoint.Paused {
			completed = report.Checkpoint.Completed
			break
		}
		if report.Checkpoint.NextCursor == "" || report.Checkpoint.NextCursor == cursor {
			t.Fatalf("segment cursor did not advance: %#v", report.Checkpoint)
		}
		cursor = report.Checkpoint.NextCursor
	}
	if !completed || len(seenInvalid) != 130 {
		t.Fatalf("completed=%v invalid segments=%d, want 130", completed, len(seenInvalid))
	}
}
