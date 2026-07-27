package storage

import (
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresMigrationDoesNotMixGraphAndWriteContextRevisions(t *testing.T) {
	ctx, sourceCoordinator := newPostgresIntegrationCoordinator(
		t, "migration-context-source",
	)
	_, targetCoordinator := newPostgresIntegrationCoordinator(
		t, "migration-context-target",
	)
	sourceBase := NewMemoryStore()
	sourceObjects := &blockingObjectGetStore{
		ObjectStore: sourceBase,
		entered:     make(chan struct{}),
		release:     make(chan struct{}),
	}
	defer func() {
		select {
		case <-sourceObjects.release:
		default:
			close(sourceObjects.release)
		}
	}()
	source := NewTenantStore(sourceObjects, "source")
	source.SetCoordinator(sourceCoordinator)
	quota := 100
	if _, err := source.CreateTenant(
		ctx,
		"tenant-a",
		TenantCreateOptions{Config: &TenantConfig{
			Quota: TenantQuotaConfig{MaxEntitiesPerTenant: &quota},
		}},
	); err != nil {
		t.Fatalf("create source: %v", err)
	}
	if _, err := source.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "document:a", Kind: "document"},
			{ID: "document:b", Kind: "document"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed source: %v", err)
	}
	head, _, err := sourceCoordinator.Head(ctx, "tenant-a")
	if err != nil || head.WriteContextKey == "" {
		t.Fatalf("load source head=%#v err=%v", head, err)
	}
	sourceObjects.arm(head.WriteContextKey)

	target := NewTenantStore(NewMemoryStore(), "target")
	target.SetCoordinator(targetCoordinator)
	result := make(chan error, 1)
	go func() {
		_, err := CopyTenantObjects(
			ctx,
			source,
			"tenant-a",
			target,
			"tenant-a",
			TenantMigrationOptions{},
		)
		result <- err
	}()
	select {
	case <-sourceObjects.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("migration did not reach write-context capture")
	}

	concurrent := NewTenantStore(sourceBase, "source")
	concurrent.SetCoordinator(sourceCoordinator)
	if _, err := concurrent.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "depends_custom", FromKind: "document",
			ToKind: "document", Directed: true,
		}},
		UpsertEdges: []graph.Edge{{
			Type: "depends_custom", From: "document:a", To: "document:b",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("advance source graph: %v", err)
	}
	if _, err := concurrent.PutRelationSchema(
		ctx,
		"tenant-a",
		RelationSchema{RelationType: "depends_custom"},
	); err != nil {
		t.Fatalf("advance source context: %v", err)
	}
	close(sourceObjects.release)
	if err := <-result; err != nil {
		t.Fatalf("migrate tenant: %v", err)
	}

	g, _, err := target.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migrated graph: %v", err)
	}
	_, hasRelationType := g.RelationTypes["depends_custom"]
	schemas, err := target.GetRelationSchemas(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load migrated schemas: %v", err)
	}
	_, hasSchema := schemas.Schema("depends_custom")
	if hasRelationType != hasSchema {
		t.Fatalf(
			"migration mixed revisions: relation_type=%v schema=%v",
			hasRelationType, hasSchema,
		)
	}
}
