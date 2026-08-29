package storage

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestRollbackCoordinatorReportsModeRestoreFailure(t *testing.T) {
	tests := []struct {
		name           string
		restoreChanged bool
		restoreErr     error
		wantConflict   bool
	}{
		{
			name:           "restore error",
			restoreChanged: false,
			restoreErr:     errors.New("injected mode restore failure"),
		},
		{
			name:           "restore conflict",
			restoreChanged: false,
			wantConflict:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := NewTenantStore(NewMemoryStore(), "test")
			if err := store.PutCoordinationMarker(ctx, CoordinationPostgres, "test"); err != nil {
				t.Fatalf("put coordination marker: %v", err)
			}
			syncErr := errors.New("injected legacy sync failure")
			coordinator := &rollbackFailureCoordinator{
				mode:           CoordinationPostgres,
				syncErr:        syncErr,
				restoreChanged: test.restoreChanged,
				restoreErr:     test.restoreErr,
			}

			_, err := store.RollbackCoordinator(ctx, coordinator, false)
			if !errors.Is(err, syncErr) {
				t.Fatalf("rollback err = %v, want legacy sync failure", err)
			}
			if test.restoreErr != nil {
				if err == nil || !errors.Is(err, coordinator.restoreErr) {
					t.Fatalf("rollback err = %v, want mode restore failure", err)
				}
			} else if test.wantConflict && !errors.Is(err, ErrConflict) {
				t.Fatalf("rollback err = %v, want restore conflict", err)
			}
			if coordinator.restoreAttempts != 1 {
				t.Fatalf("restore attempts = %d, want 1", coordinator.restoreAttempts)
			}
		})
	}
}

func TestRollbackCoordinatorRestoresPostgresModeWhenMarkerRemovalFails(t *testing.T) {
	ctx := context.Background()
	deleteErr := errors.New("injected marker removal failure")
	objects := &failOnceDeleteKeyStore{
		ObjectStore: NewMemoryStore(),
		key:         "test/coordination/mode.json",
		err:         deleteErr,
	}
	store := NewTenantStore(objects, "test")
	if err := store.PutCoordinationMarker(ctx, CoordinationPostgres, "test"); err != nil {
		t.Fatalf("put coordination marker: %v", err)
	}
	coordinator := &rollbackFailureCoordinator{
		mode:           CoordinationPostgres,
		restoreChanged: true,
	}

	report, err := store.RollbackCoordinator(ctx, coordinator, false)
	if !errors.Is(err, deleteErr) {
		t.Fatalf("rollback err = %v, want marker removal failure", err)
	}
	if coordinator.mode != CoordinationPostgres || report.ModeAfter != CoordinationPostgres {
		t.Fatalf("coordinator mode/report = %q/%q, want postgres/postgres", coordinator.mode, report.ModeAfter)
	}
	if coordinator.restoreAttempts != 1 {
		t.Fatalf("restore attempts = %d, want 1", coordinator.restoreAttempts)
	}
	if err := store.EnsureLocalWriterAllowed(ctx); err == nil {
		t.Fatal("coordination marker disappeared after injected delete failure")
	}
}

type rollbackFailureCoordinator struct {
	WriteCoordinator
	mode            string
	syncErr         error
	restoreChanged  bool
	restoreErr      error
	restoreAttempts int
}

func (*rollbackFailureCoordinator) Backend() string   { return CoordinationPostgres }
func (*rollbackFailureCoordinator) Namespace() string { return "test" }

func (c *rollbackFailureCoordinator) CoordinationMode(context.Context) (string, error) {
	return c.mode, nil
}

func (c *rollbackFailureCoordinator) CompareAndSwapCoordinationMode(
	_ context.Context,
	from string,
	to string,
) (bool, error) {
	if to == CoordinationPostgres && from != CoordinationPostgres {
		c.restoreAttempts++
		if c.restoreErr != nil {
			return false, c.restoreErr
		}
		if !c.restoreChanged {
			return false, nil
		}
	}
	if c.mode != from {
		return false, nil
	}
	c.mode = to
	return true, nil
}

func (c *rollbackFailureCoordinator) ClaimLegacyManifest(
	context.Context,
	string,
	time.Duration,
) (LegacyManifestJob, bool, error) {
	return LegacyManifestJob{}, false, c.syncErr
}

func (*rollbackFailureCoordinator) Status(context.Context) (CoordinatorStatus, error) {
	return CoordinatorStatus{}, nil
}

func (*rollbackFailureCoordinator) ListHeads(context.Context) ([]CoordinationHead, error) {
	return []CoordinationHead{}, nil
}

type failOnceDeleteKeyStore struct {
	ObjectStore
	key    string
	err    error
	failed bool
}

func (s *failOnceDeleteKeyStore) DeleteConditional(
	ctx context.Context,
	key string,
	condition PutCondition,
) error {
	if key == s.key && !s.failed {
		s.failed = true
		return s.err
	}
	return s.ObjectStore.DeleteConditional(ctx, key, condition)
}

func TestPostgresCoordinatorS3RollbackDrill(t *testing.T) {
	fixture := newPostgresS3Fixture(t, "s3-rollback", 3*time.Minute)
	writer := fixture.newWriter(t, 0)
	for sequence := 1; sequence <= 3; sequence++ {
		suffix := strconv.Itoa(sequence)
		if _, err := writer.Commit(fixture.ctx, "tenant-a", graph.Mutations{
			UpsertEntities: []graph.Entity{{
				ID:     "sample:" + suffix,
				Kind:   "sample",
				Fields: graph.Fields{"sequence": sequence},
			}},
		}, CommitOptions{IdempotencyKey: "rollback-seed-" + suffix}); err != nil {
			t.Fatalf("seed commit %d: %v", sequence, err)
		}
	}

	operator := NewTenantStore(fixture.objects, fixture.prefix)
	operator.SetCoordinator(fixture.coordinator)
	dryRun, err := operator.RollbackCoordinator(fixture.ctx, fixture.coordinator, true)
	if err == nil {
		t.Fatal("rollback dry-run passed before the legacy mirror drained")
	}
	if dryRun.Applied {
		t.Fatal("rollback dry-run changed coordinator state")
	}

	applied, err := operator.RollbackCoordinator(fixture.ctx, fixture.coordinator, false)
	if err != nil {
		t.Fatalf("apply rollback: %v", err)
	}
	if !applied.Applied || !applied.MarkerRemoved ||
		applied.ModeAfter != CoordinationLocal ||
		applied.CoordinatorStatus.OutboxBacklog != 0 ||
		applied.CoordinatorStatus.MaxMirrorLag != 0 {
		t.Fatalf("rollback report = %#v", applied)
	}
	if err := operator.EnsureLocalWriterAllowed(fixture.ctx); err != nil {
		t.Fatalf("local writer remains fenced after rollback: %v", err)
	}
	if mode, err := fixture.coordinator.CoordinationMode(fixture.ctx); err != nil || mode != CoordinationLocal {
		t.Fatalf("coordination mode = %q err=%v", mode, err)
	}

	_, err = writer.Commit(fixture.ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "sample:stale-pg", Kind: "sample"}},
	}, CommitOptions{IdempotencyKey: "rollback-stale-pg"})
	if !errors.Is(err, ErrCoordinatorFenced) {
		t.Fatalf("stale PostgreSQL writer err = %v, want ErrCoordinatorFenced", err)
	}

	local := NewTenantStore(fixture.objects, fixture.prefix)
	manifest, err := local.Commit(fixture.ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "sample:local", Kind: "sample"}},
	}, CommitOptions{IdempotencyKey: "rollback-local"})
	if err != nil {
		t.Fatalf("local writer commit after rollback: %v", err)
	}
	if manifest.Version != 4 {
		t.Fatalf("local manifest version = %d, want 4", manifest.Version)
	}
	graphData, loaded, err := local.Load(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("local load after rollback: %v", err)
	}
	if loaded.Version != 4 || len(graphData.Entities) != 4 {
		t.Fatalf("local graph version/entities = %d/%d, want 4/4", loaded.Version, len(graphData.Entities))
	}
	writeRollbackDrillReport(t, rollbackDrillReport{
		SchemaVersion: 1,
		Success:       true,
		Commit: firstNonEmptyEnvironment(
			"GRAPHDB_TEST_BUILD_COMMIT",
			"GITHUB_SHA",
			"CI_COMMIT_SHA",
		),
		Namespace:             fixture.namespace,
		PostgresHeadVersion:   applied.Tenants[0].GraphVersion,
		LegacyManifestVersion: applied.Tenants[0].GraphVersion,
		LocalVersion:          loaded.Version,
		OutboxBacklog:         applied.CoordinatorStatus.OutboxBacklog,
		LegacyMirrorLag:       applied.CoordinatorStatus.MaxMirrorLag,
		MarkerRemoved:         applied.MarkerRemoved,
		PostgresWriterFenced:  true,
		LocalWriterSucceeded:  true,
	})
}
