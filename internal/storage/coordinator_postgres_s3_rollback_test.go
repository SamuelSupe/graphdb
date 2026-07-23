package storage

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

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
