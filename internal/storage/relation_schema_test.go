package storage

import (
	"context"
	"encoding/json"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestRelationSchemaSidecarValidatesEdgesAndKeepsCoreLayout(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	seedRelationSchemaGraph(t, ctx, store)

	catalog, err := store.PutRelationSchema(ctx, "tenant-a", RelationSchema{
		RelationType: "depends_on",
		Strict:       true,
		Fields: map[string]graph.FieldSpec{
			"status": {Type: "string", Required: true, Enum: []any{"active", "inactive"}},
			"weight": {Type: "number", Default: float64(1)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if catalog.LayoutVersion != relationSchemaLayoutVersion || catalog.Revision != 1 || catalog.GraphVersion != 1 {
		t.Fatalf("catalog=%#v", catalog)
	}

	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{{
		Type: "depends_on", From: "document:b", To: "document:a", Fields: graph.Fields{"status": "inactive"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if result.Manifest.LayoutVersion != CurrentObjectLayoutVersion {
		t.Fatalf("manifest layout=%d want=%d", result.Manifest.LayoutVersion, CurrentObjectLayoutVersion)
	}
	loaded, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	edgeID := graph.CanonicalEdgeIDParts("depends_on", "document:b", "document:a")
	edge, ok := loaded.Edges[edgeID]
	if !ok || edge.Fields["weight"] != float64(1) {
		t.Fatalf("defaulted edge=%#v", edge)
	}
	beforeInvalid := manifest.Version
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{{
		Type: "depends_on", From: "document:a", To: "document:b", Fields: graph.Fields{"status": float64(1), "extra": true},
	}}}, CommitOptions{}); err == nil {
		t.Fatal("invalid relation properties unexpectedly committed")
	}
	_, afterInvalid, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if afterInvalid.Version != beforeInvalid {
		t.Fatalf("invalid commit advanced version from %d to %d", beforeInvalid, afterInvalid.Version)
	}

	key := store.relationSchemaCatalogKey("tenant-a")
	data, err := objects.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if isParquetBytes(data) {
		t.Fatal("1.1 relation schema sidecar was written into the core parquet format")
	}
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil || raw["layout_version"] != float64(1) {
		t.Fatalf("sidecar=%s err=%v", data, err)
	}

	cold := NewTenantStore(objects, "test")
	coldCatalog, err := cold.GetRelationSchemas(ctx, "tenant-a")
	if err != nil || coldCatalog.Revision != catalog.Revision {
		t.Fatalf("cold catalog=%#v err=%v", coldCatalog, err)
	}
	if coldCatalog.GraphVersion != manifest.Version {
		t.Fatalf("schema validation graph version=%d want=%d", coldCatalog.GraphVersion, manifest.Version)
	}
	if _, _, err := cold.Load(ctx, "tenant-a"); err != nil {
		t.Fatalf("1.0-compatible core load failed with sidecar present: %v", err)
	}
}

func TestRelationSchemaRejectsExistingIncompatibleEdges(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedRelationSchemaGraph(t, ctx, store)
	_, err := store.PutRelationSchema(ctx, "tenant-a", RelationSchema{
		RelationType: "depends_on",
		Fields: map[string]graph.FieldSpec{
			"ticket": {Type: "string", Required: true},
		},
	})
	if err == nil {
		t.Fatal("incompatible required field unexpectedly accepted")
	}
	catalog, getErr := store.GetRelationSchemas(ctx, "tenant-a")
	if getErr != nil {
		t.Fatal(getErr)
	}
	if catalog.Revision != 0 || len(catalog.RelationSchemas) != 0 {
		t.Fatalf("failed schema update leaked sidecar state: %#v", catalog)
	}
}

func TestRelationSchemaPreventsDeletingItsRelationType(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedRelationSchemaGraph(t, ctx, store)
	if _, err := store.PutRelationSchema(ctx, "tenant-a", RelationSchema{
		RelationType: "depends_on",
		Fields:       map[string]graph.FieldSpec{"status": {Type: "string"}},
	}); err != nil {
		t.Fatal(err)
	}
	_, before, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{DeleteRelationTypes: []string{"depends_on"}}, CommitOptions{}); err == nil {
		t.Fatal("relation type with a property schema was deleted")
	}
	_, after, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if after.Version != before.Version {
		t.Fatalf("failed delete advanced version from %d to %d", before.Version, after.Version)
	}
}

func TestRelationSchemaDetectsWritesMadeByVersion10Writer(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	seedRelationSchemaGraph(t, ctx, store)
	if _, err := store.PutRelationSchema(ctx, "tenant-a", RelationSchema{
		RelationType: "depends_on",
		Fields:       map[string]graph.FieldSpec{"status": {Type: "string", Required: true}},
	}); err != nil {
		t.Fatal(err)
	}
	schemaKey := store.relationSchemaCatalogKey("tenant-a")
	schemaData, err := objects.Get(ctx, schemaKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Delete(ctx, schemaKey); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEdges: []graph.Edge{{
		Type: "depends_on", From: "document:b", To: "document:a",
	}}}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := objects.Put(ctx, schemaKey, schemaData); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "document:c", Kind: "document",
	}}}, CommitOptions{}); err == nil {
		t.Fatal("1.1 writer did not detect an invalid edge written while the schema sidecar was ignored")
	}
}

func TestRelationSchemasSurviveCloneAndBackupRestore(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedRelationSchemaGraph(t, ctx, store)
	if _, err := store.PutRelationSchema(ctx, "tenant-a", RelationSchema{
		RelationType: "depends_on",
		Fields:       map[string]graph.FieldSpec{"status": {Type: "string"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CloneTenant(ctx, "tenant-a", TenantCloneOptions{TargetTenantID: "tenant-b"}); err != nil {
		t.Fatal(err)
	}
	cloned, err := store.GetRelationSchemas(ctx, "tenant-b")
	if err != nil || len(cloned.RelationSchemas) != 1 || cloned.RelationSchemas[0].RelationType != "depends_on" {
		t.Fatalf("cloned schemas=%#v err=%v", cloned, err)
	}

	backup, err := store.StartTask(ctx, "tenant-a", TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup = waitForTask(t, ctx, store, "tenant-a", backup.ID)
	manifestKey, _ := backup.Result["backup_manifest_key"].(string)
	if manifestKey == "" || backup.Result["relation_schemas"] != float64(1) {
		t.Fatalf("backup result=%#v", backup.Result)
	}
	restore, err := store.StartTask(ctx, "tenant-c", TaskTypeTenantRestore, map[string]any{"backup_key": manifestKey})
	if err != nil {
		t.Fatal(err)
	}
	restore = waitForTask(t, ctx, store, "tenant-c", restore.ID)
	if restore.Status != TaskStatusSucceeded {
		t.Fatalf("restore=%#v", restore)
	}
	restored, err := store.GetRelationSchemas(ctx, "tenant-c")
	if err != nil || len(restored.RelationSchemas) != 1 || restored.RelationSchemas[0].Fields["status"].Type != "string" {
		t.Fatalf("restored schemas=%#v err=%v", restored, err)
	}
}

func seedRelationSchemaGraph(t *testing.T, ctx context.Context, store *TenantStore) {
	t.Helper()
	_, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "document:a", Kind: "document"},
			{ID: "document:b", Kind: "document"},
		},
		UpsertRelationTypes: []graph.RelationType{{
			Name: "depends_on", FromKind: "document", ToKind: "document", Directed: true,
		}},
		UpsertEdges: []graph.Edge{{
			Type: "depends_on", From: "document:a", To: "document:b", Fields: graph.Fields{"status": "active"},
		}},
	}, CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
}
