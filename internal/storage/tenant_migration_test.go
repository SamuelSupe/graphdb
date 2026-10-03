package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestCopyTenantObjectsDryRunAndCopy(t *testing.T) {
	ctx := context.Background()
	source := NewTenantStore(NewMemoryStore(), "source")
	target := NewTenantStore(NewMemoryStore(), "target")
	if _, err := source.CreateTenant(ctx, "tenant-a", TenantCreateOptions{Name: "Tenant A"}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "host", ToKind: "host", Directed: true}},
		UpsertEntities: []graph.Entity{
			{ID: "host:a", Kind: "host", Fields: graph.Fields{"name": "a"}},
			{ID: "host:z", Kind: "host", Fields: graph.Fields{"name": "z"}},
		},
		UpsertEdges: []graph.Edge{{Type: "links", From: "host:a", To: "host:z"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := source.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}

	dryRun, err := CopyTenantObjects(ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{DryRun: true})
	if err != nil {
		t.Fatalf("dry-run migration: %v", err)
	}
	if !dryRun.DryRun || dryRun.Objects == 0 || dryRun.Copied != 0 || dryRun.Skipped != dryRun.Objects {
		t.Fatalf("dry-run report = %#v", dryRun)
	}
	if exists, err := target.tenantRestoreDataExists(ctx, "tenant-a"); err != nil || exists {
		t.Fatalf("dry-run target exists=%v err=%v", exists, err)
	}

	report, err := CopyTenantObjects(ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{})
	if err != nil {
		t.Fatalf("copy migration: %v", err)
	}
	if report.Copied == 0 || report.Objects == 0 || report.Bytes == 0 {
		t.Fatalf("copy report = %#v", report)
	}
	g, manifest, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migrated tenant: %v", err)
	}
	if manifest.TenantID != "tenant-a" {
		t.Fatalf("migrated manifest tenant = %q", manifest.TenantID)
	}
	lease, err := target.GetWriterLease(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get target writer lease: %v", err)
	}
	if manifest.WriterFence != lease.FenceToken || manifest.WriterFenceEpoch != lease.FenceEpoch {
		t.Fatalf("migrated fence = (%q,%d), lease = (%q,%d)", manifest.WriterFence, manifest.WriterFenceEpoch, lease.FenceToken, lease.FenceEpoch)
	}
	if _, ok := g.GetEntity("host:a"); !ok {
		t.Fatalf("migrated graph missing entity")
	}
	indexCatalog, err := target.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("get migrated index catalog: %v", err)
	}
	reverseCatalog, err := target.GetReverseIndexCatalog(ctx, "tenant-a", indexCatalog.Version)
	if err != nil {
		t.Fatalf("get migrated reverse catalog: %v", err)
	}
	lookup := &PersistedIndexLookup{Store: target, TenantID: "tenant-a", Version: indexCatalog.Version, Catalog: indexCatalog, ReverseCatalog: &reverseCatalog}
	incoming, ok, err := lookup.InEdges(ctx, "host:z", map[string]struct{}{"links": {}})
	if err != nil || !ok || len(incoming) != 1 || incoming[0].From != "host:a" {
		t.Fatalf("migrated reverse edges=%#v ok=%v err=%v", incoming, ok, err)
	}
	if _, err := target.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit after migration: %v", err)
	}
}

func TestCopyTenantObjectsFailurePreservesLocalTarget(t *testing.T) {
	for _, failure := range []string{"missing_commit", "wrong_digest", "read_error"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			sourceObjects := NewMemoryStore()
			source := NewTenantStore(sourceObjects, "source")
			result, err := source.Commit(ctx, "tenant-a", graph.Mutations{
				UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
			}, CommitOptions{})
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			files, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			target := NewTenantStore(files, "target")
			if _, err := target.Commit(ctx, "tenant-a", graph.Mutations{
				UpsertEntities: []graph.Entity{{ID: "host:target", Kind: "host"}},
			}, CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			before, err := files.Get(ctx, target.manifestKey("tenant-a"))
			if err != nil {
				t.Fatal(err)
			}
			switch failure {
			case "missing_commit":
				if err := sourceObjects.Delete(ctx, result.CommitKeys[0]); err != nil {
					t.Fatal(err)
				}
			case "wrong_digest":
				manifest := result
				manifest.DataHash = "sha256-shards-v2:" + strings.Repeat("0", 64)
				if err := putManifestFixture(ctx, source, "tenant-a", manifest); err != nil {
					t.Fatal(err)
				}
			case "read_error":
				source = NewTenantStore(&tenantMigrationReadFailureStore{ObjectStore: sourceObjects, key: result.CommitKeys[0]}, "source")
			}
			_, copyErr := CopyTenantObjects(ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{Overwrite: true})
			after, headErr := files.Get(ctx, target.manifestKey("tenant-a"))
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			reopened, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			cold := NewTenantStore(reopened, "target")
			g, _, loadErr := cold.Load(ctx, "tenant-a")
			preserved := false
			if loadErr == nil {
				_, preserved = g.GetEntity("host:target")
			}
			if copyErr == nil || headErr != nil || string(before) != string(after) || !preserved {
				t.Fatalf("failed copy must reject input and preserve published target: copy=%v head=%v cold_load=%v preserved=%v head_equal=%v", copyErr, headErr, loadErr, preserved, string(before) == string(after))
			}
		})
	}
}

type tenantMigrationReadFailureStore struct {
	ObjectStore
	key string
}

func (s *tenantMigrationReadFailureStore) Get(ctx context.Context, key string) ([]byte, error) {
	if key == s.key {
		return nil, errors.New("injected source read failure")
	}
	return s.ObjectStore.Get(ctx, key)
}

func TestCopyTenantObjectsRejectsTenantRename(t *testing.T) {
	ctx := context.Background()
	source := NewTenantStore(NewMemoryStore(), "source")
	target := NewTenantStore(NewMemoryStore(), "target")
	if _, err := source.InitTenant(ctx, "tenant-a"); err != nil {
		t.Fatalf("init tenant: %v", err)
	}
	if _, err := CopyTenantObjects(ctx, source, "tenant-a", target, "tenant-b", TenantMigrationOptions{}); err == nil || !strings.Contains(err.Error(), "backup/restore") {
		t.Fatalf("rename migration err = %v", err)
	}
}

func TestCopyTenantObjectsPinsManifestBeforeListing(t *testing.T) {
	ctx := context.Background()
	sourceObjects := &commitAfterTenantListStore{
		ObjectStore: NewMemoryStore(),
		prefix:      "source/tenants/tenant-a/",
	}
	source := NewTenantStore(sourceObjects, "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	sourceObjects.afterList = func() error {
		_, err := source.Commit(ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: "host:second", Kind: "host"}},
		}, CommitOptions{})
		return err
	}

	target := NewTenantStore(NewMemoryStore(), "target")
	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); err != nil {
		t.Fatalf("copy concurrent source snapshot: %v", err)
	}
	if !sourceObjects.fired {
		t.Fatal("source did not advance after the migration list snapshot")
	}
	g, manifest, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migrated tenant: %v", err)
	}
	if manifest.Version != 1 {
		t.Fatalf("migrated version = %d, want pinned version 1", manifest.Version)
	}
	if _, ok := g.GetEntity("host:first"); !ok {
		t.Fatal("migrated tenant is missing pinned data")
	}
	if _, ok := g.GetEntity("host:second"); ok {
		t.Fatal("migrated tenant mixed in a commit newer than its pinned manifest")
	}
}

func TestCopyTenantObjectsPublishesManifestLast(t *testing.T) {
	ctx := context.Background()
	source := NewTenantStore(NewMemoryStore(), "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := source.Compact(ctx, "tenant-a"); err != nil {
		t.Fatalf("compact source: %v", err)
	}

	targetObjects := &manifestPublishObserver{
		ObjectStore: NewMemoryStore(),
		key:         "target/tenants/tenant-a/manifest.parquet",
	}
	target := NewTenantStore(targetObjects, "target")
	targetObjects.afterPublish = func() error {
		_, _, err := target.Load(ctx, "tenant-a")
		return err
	}
	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); err != nil {
		t.Fatalf("copy compacted tenant: %v", err)
	}
	if !targetObjects.fired {
		t.Fatal("target manifest was not published")
	}
	if targetObjects.loadErr != nil {
		t.Fatalf("target was unreadable when manifest became visible: %v", targetObjects.loadErr)
	}
}

func TestCopyTenantObjectsRejectsIdenticalLocation(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	source := NewTenantStore(objects, "graphdb")
	target := NewTenantStore(objects, "graphdb")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	if _, err := CopyTenantObjects(
		ctx,
		source,
		"tenant-a",
		target,
		"tenant-a",
		TenantMigrationOptions{Overwrite: true},
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("identical-location migration err = %v, want ErrConflict", err)
	}
	g, _, err := source.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load preserved source: %v", err)
	}
	if _, ok := g.GetEntity("host:source"); !ok {
		t.Fatal("identical-location migration changed the source tenant")
	}
}

func TestCopyTenantObjectsReadsEachPayloadOnce(t *testing.T) {
	ctx := context.Background()
	sourceObjects := &countingGetStore{
		ObjectStore: NewMemoryStore(),
		gets:        map[string]int{},
	}
	source := NewTenantStore(sourceObjects, "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	sourceObjects.gets = map[string]int{}

	target := NewTenantStore(NewMemoryStore(), "target")
	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); err != nil {
		t.Fatalf("copy tenant: %v", err)
	}
	for key, count := range sourceObjects.gets {
		if count > 1 {
			t.Fatalf("source payload %q was fetched %d times", key, count)
		}
	}
}

func TestCopyTenantObjectsUsesBoundedObjectPages(t *testing.T) {
	ctx := context.Background()
	sourceBase := NewMemoryStore()
	seed := NewTenantStore(sourceBase, "source")
	if _, err := seed.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	sourceObjects := &pagingOnlyStore{ObjectStore: sourceBase}
	targetObjects := &pagingOnlyStore{ObjectStore: NewMemoryStore()}
	source := NewTenantStore(sourceObjects, "source")
	target := NewTenantStore(targetObjects, "target")

	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", target, "tenant-a", TenantMigrationOptions{},
	); err != nil {
		t.Fatalf("copy tenant: %v", err)
	}
	if sourceObjects.listCalls != 0 || sourceObjects.pageCalls == 0 {
		t.Fatalf(
			"source list calls=%d page calls=%d",
			sourceObjects.listCalls, sourceObjects.pageCalls,
		)
	}
	if targetObjects.listCalls != 0 || targetObjects.pageCalls == 0 {
		t.Fatalf(
			"target list calls=%d page calls=%d",
			targetObjects.listCalls, targetObjects.pageCalls,
		)
	}
}

func TestCopyTenantObjectsRechecksTargetAfterWriterFence(t *testing.T) {
	ctx := context.Background()
	source := NewTenantStore(NewMemoryStore(), "source")
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}

	targetObjects := &hideNextTenantListStore{
		ObjectStore: NewMemoryStore(),
		prefix:      "target/tenants/tenant-a/",
	}
	owner := NewTenantStore(targetObjects, "target")
	if _, err := owner.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:target", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	targetObjects.hideNextList()

	migrator := NewTenantStore(targetObjects, "target")
	if _, err := CopyTenantObjects(
		ctx, source, "tenant-a", migrator, "tenant-a", TenantMigrationOptions{},
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("migration err = %v, want ErrConflict", err)
	}
	g, _, err := migrator.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load preserved target: %v", err)
	}
	if _, ok := g.GetEntity("host:target"); !ok {
		t.Fatal("migration without overwrite replaced a target that appeared before fencing")
	}
}

type commitAfterTenantListStore struct {
	ObjectStore
	prefix    string
	afterList func() error
	fired     bool
}

func (s *commitAfterTenantListStore) List(
	ctx context.Context,
	prefix string,
) ([]ObjectInfo, error) {
	objects, err := s.ObjectStore.List(ctx, prefix)
	if err != nil || prefix != s.prefix || s.afterList == nil || s.fired {
		return objects, err
	}
	s.fired = true
	return objects, s.afterList()
}

type manifestPublishObserver struct {
	ObjectStore
	key          string
	afterPublish func() error
	fired        bool
	loadErr      error
}

func (s *manifestPublishObserver) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, condition)
	if err == nil && key == s.key && !s.fired {
		s.fired = true
		if s.afterPublish != nil {
			s.loadErr = s.afterPublish()
		}
	}
	return meta, err
}

type countingGetStore struct {
	ObjectStore
	mu   sync.Mutex
	gets map[string]int
}

func (s *countingGetStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	s.gets[key]++
	s.mu.Unlock()
	return s.ObjectStore.Get(ctx, key)
}

type hideNextTenantListStore struct {
	ObjectStore
	mu       sync.Mutex
	prefix   string
	hideNext bool
}

func (s *hideNextTenantListStore) hideNextList() {
	s.mu.Lock()
	s.hideNext = true
	s.mu.Unlock()
}

func (s *hideNextTenantListStore) List(
	ctx context.Context,
	prefix string,
) ([]ObjectInfo, error) {
	s.mu.Lock()
	hide := s.hideNext && prefix == s.prefix
	if hide {
		s.hideNext = false
	}
	s.mu.Unlock()
	if hide {
		return []ObjectInfo{}, nil
	}
	return s.ObjectStore.List(ctx, prefix)
}
