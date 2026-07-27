package storage

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type postgresS3Fixture struct {
	ctx         context.Context
	dsn         string
	schema      string
	namespace   string
	prefix      string
	objects     *S3Store
	coordinator *PostgresCoordinator
}

func TestPostgresCoordinatorS3ConcurrentWriters(t *testing.T) {
	fixture := newPostgresS3Fixture(t, "s3-concurrent", 2*time.Minute)
	const writers = 8
	stores := make([]*TenantStore, 0, writers)
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup

	for writerID := 0; writerID < writers; writerID++ {
		store := fixture.newWriter(t, writerID)
		stores = append(stores, store)
		wg.Add(1)
		go func(writerID int, store *TenantStore) {
			defer wg.Done()
			<-start
			_, err := store.Commit(fixture.ctx, "tenant-a", graph.Mutations{
				UpsertEntities: []graph.Entity{{
					ID:     fmt.Sprintf("host:%d", writerID),
					Kind:   "host",
					Fields: graph.Fields{"writer": writerID},
				}},
			}, CommitOptions{IdempotencyKey: fmt.Sprintf("writer-%d", writerID)})
			errs <- err
		}(writerID, store)
	}

	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent PostgreSQL/S3 commit: %v", err)
		}
	}

	head, exists, err := fixture.coordinator.Head(fixture.ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("coordinator head exists=%v err=%v", exists, err)
	}
	if head.GraphVersion != writers || head.Revision != writers {
		t.Fatalf("coordinator head version/revision = %d/%d, want %d/%d",
			head.GraphVersion, head.Revision, writers, writers)
	}
	g, manifest, err := stores[0].Load(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load authoritative graph: %v", err)
	}
	if manifest.Version != writers || len(g.Entities) != writers {
		t.Fatalf("authoritative graph version/entities = %d/%d, want %d/%d",
			manifest.Version, len(g.Entities), writers, writers)
	}

	synced, err := stores[0].SyncLegacyManifests(fixture.ctx)
	if err != nil {
		t.Fatalf("sync legacy manifests: %v", err)
	}
	if synced != 1 {
		t.Fatalf("coalesced synced legacy manifests = %d, want 1", synced)
	}
	legacy := NewTenantStore(fixture.objects, fixture.prefix)
	legacyGraph, legacyManifest, err := legacy.Load(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load through 1.0-compatible manifest: %v", err)
	}
	if legacyManifest.Version != writers || len(legacyGraph.Entities) != writers {
		t.Fatalf("legacy graph version/entities = %d/%d, want %d/%d",
			legacyManifest.Version, len(legacyGraph.Entities), writers, writers)
	}
	if err := legacy.EnsureLocalWriterAllowed(fixture.ctx); err == nil {
		t.Fatal("local writer was allowed despite PostgreSQL coordination marker")
	}

	immutable, err := fixture.objects.List(
		fixture.ctx, stores[0].coordinatorManifestPrefix("tenant-a"),
	)
	if err != nil {
		t.Fatalf("list immutable manifests: %v", err)
	}
	if len(immutable) < writers {
		t.Fatalf("immutable manifest count = %d, want at least %d", len(immutable), writers)
	}
}

func TestPostgresCoordinatorS3CompactWhileWriterAdvancesHead(t *testing.T) {
	fixture := newPostgresS3Fixture(t, "s3-compact-concurrent", 2*time.Minute)
	writer := fixture.newWriter(t, 0)
	compactor := fixture.newWriter(t, 1)
	if _, err := writer.Commit(fixture.ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "sample:1", Kind: "sample"}},
	}, CommitOptions{IdempotencyKey: "compact-seed"}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}

	blocking := &blockOncePutStore{
		ObjectStore: fixture.objects,
		substring:   "/snapshots/sharded/",
		paused:      make(chan struct{}),
		resume:      make(chan struct{}),
	}
	compactor.Objects = blocking
	compactDone := make(chan struct {
		manifest Manifest
		err      error
	}, 1)
	go func() {
		manifest, err := compactor.Compact(fixture.ctx, "tenant-a")
		compactDone <- struct {
			manifest Manifest
			err      error
		}{manifest: manifest, err: err}
	}()

	select {
	case <-blocking.paused:
	case <-time.After(10 * time.Second):
		t.Fatal("compact did not pause while writing its snapshot")
	}
	if _, err := writer.Commit(fixture.ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "sample:2", Kind: "sample"}},
	}, CommitOptions{IdempotencyKey: "compact-concurrent"}); err != nil {
		t.Fatalf("commit while compacting: %v", err)
	}
	close(blocking.resume)

	result := <-compactDone
	if result.err != nil {
		t.Fatalf("compact after concurrent commit: %v", result.err)
	}
	if result.manifest.Version != 2 || result.manifest.SnapshotVersion != 1 {
		t.Fatalf(
			"compacted manifest version/snapshot = %d/%d, want 2/1",
			result.manifest.Version, result.manifest.SnapshotVersion,
		)
	}
	if tail := manifestCommitTailLength(result.manifest); tail != 1 {
		t.Fatalf("compacted commit tail = %d, want 1", tail)
	}
	head, exists, err := fixture.coordinator.Head(fixture.ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("load compacted head exists=%v err=%v", exists, err)
	}
	if head.GraphVersion != 2 || head.Revision != 3 {
		t.Fatalf("head graph version/revision = %d/%d, want 2/3", head.GraphVersion, head.Revision)
	}
	g, manifest, err := writer.Load(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load graph after concurrent compact: %v", err)
	}
	if manifest.Version != 2 || len(g.Entities) != 2 {
		t.Fatalf("loaded graph version/entities = %d/%d, want 2/2", manifest.Version, len(g.Entities))
	}
}

func newPostgresS3Fixture(t *testing.T, namespace string, timeout time.Duration) *postgresS3Fixture {
	t.Helper()
	dsn := os.Getenv("GRAPHDB_TEST_POSTGRES_DSN")
	if dsn == "" || os.Getenv("GRAPHDB_MINIO_INTEGRATION") != "1" {
		t.Skip("set GRAPHDB_TEST_POSTGRES_DSN and GRAPHDB_MINIO_INTEGRATION=1")
	}
	pathStyle, err := envBool("S3_PATH_STYLE")
	if err != nil {
		t.Fatal(err)
	}
	objects, err := NewS3StoreWithOptions(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_BUCKET"),
		envOr("S3_REGION", "us-east-1"),
		envOr("S3_ACCESS_KEY_ID", os.Getenv("AWS_ACCESS_KEY_ID")),
		envOr("S3_SECRET_ACCESS_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY")),
		S3Options{PathStyle: pathStyle},
	)
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	now := time.Now().UnixNano()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	schema := fmt.Sprintf("graphdb_s3_test_%d", now)
	namespace = fmt.Sprintf("%s-%d", namespace, now)
	coordinator, err := NewPostgresCoordinator(ctx, dsn, schema, namespace)
	if err != nil {
		cancel()
		t.Fatalf("new PostgreSQL coordinator: %v", err)
	}
	if err := coordinator.Migrate(ctx); err != nil {
		coordinator.Close()
		cancel()
		t.Fatalf("migrate PostgreSQL coordinator: %v", err)
	}
	fixture := &postgresS3Fixture{
		ctx:         ctx,
		dsn:         dsn,
		schema:      schema,
		namespace:   namespace,
		prefix:      fmt.Sprintf("graphdb-pg-s3-test/%d", now),
		objects:     objects,
		coordinator: coordinator,
	}
	markerStore := NewTenantStore(objects, fixture.prefix)
	markerStore.SetCoordinator(coordinator)
	if err := markerStore.PutCoordinationMarker(ctx, CoordinationPostgres, namespace); err != nil {
		fixture.cleanup(t)
		cancel()
		t.Fatalf("put coordination marker: %v", err)
	}
	t.Cleanup(func() {
		fixture.cleanup(t)
		cancel()
	})
	return fixture
}

func (f *postgresS3Fixture) newWriter(t *testing.T, writerID int) *TenantStore {
	t.Helper()
	coordinator, err := NewPostgresCoordinator(f.ctx, f.dsn, f.schema, f.namespace)
	if err != nil {
		t.Fatalf("new writer coordinator %d: %v", writerID, err)
	}
	t.Cleanup(coordinator.Close)
	objects := NewWriterObjectCache(f.objects, WriterObjectCacheConfig{
		MaxBytes:    512 * 1024 * 1024,
		MaxKeys:     200000,
		NegativeTTL: 5 * time.Minute,
	})
	store := NewTenantStore(objects, f.prefix)
	store.InstanceID = fmt.Sprintf("writer-%d", writerID)
	store.CoordinatorRetryLimit = 8
	store.SetCoordinator(coordinator)
	return store
}

func (f *postgresS3Fixture) cleanup(t *testing.T) {
	t.Helper()
	objectCtx, cancelObjects := context.WithTimeout(context.Background(), 5*time.Minute)
	objects, err := f.objects.List(objectCtx, f.prefix+"/")
	if err == nil {
		keys := make([]string, 0, len(objects))
		for _, object := range objects {
			keys = append(keys, object.Key)
		}
		if deleteErr := f.objects.DeleteBatch(objectCtx, keys); deleteErr != nil {
			t.Errorf("delete %d S3 integration objects: %v", len(keys), deleteErr)
		}
	} else {
		t.Errorf("list S3 integration objects for cleanup: %v", err)
	}
	cancelObjects()

	dropCtx, cancelDrop := context.WithTimeout(context.Background(), time.Minute)
	defer cancelDrop()
	if _, err := f.coordinator.pool.Exec(dropCtx, `DROP SCHEMA IF EXISTS "`+f.schema+`" CASCADE`); err != nil {
		t.Errorf("drop PostgreSQL integration schema: %v", err)
	}
	f.coordinator.Close()
}
