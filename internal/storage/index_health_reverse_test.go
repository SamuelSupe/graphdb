package storage

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestIndexCatalogPublicationWaitsForReverseCatalog(t *testing.T) {
	for _, mode := range []string{"rebuild", "incremental"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			files, err := OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = files.Close() })
			objects := &localIngestPublicationFault{ObjectStore: files, err: ErrObjectStoreUnavailable}
			store := NewTenantStore(objects, "test")
			t.Cleanup(func() { _ = store.ShutdownTasks(ctx) })
			const tenant = "tenant-a"
			if _, err := store.Commit(ctx, tenant, indexMutations(), CommitOptions{}); err != nil {
				t.Fatal(err)
			}
			if mode == "incremental" {
				if _, err := store.RebuildIndexes(ctx, tenant); err != nil {
					t.Fatal(err)
				}
			}
			objects.key = store.reverseIndexCatalogKey(tenant)
			objects.failing.Store(true)
			version := int64(1)
			if mode == "rebuild" {
				if _, err := store.RebuildIndexes(ctx, tenant); !errors.Is(err, ErrObjectStoreUnavailable) {
					t.Fatalf("rebuild error = %v", err)
				}
			} else {
				result, err := store.CommitWithReport(ctx, tenant, graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host"}}}, CommitOptions{})
				if err != nil || result.Version != 2 || len(result.IndexWarnings) == 0 {
					t.Fatalf("commit result = %+v, err = %v", result, err)
				}
				version = result.Version
			}
			health, err := store.IndexHealthWithOptions(ctx, tenant, IndexHealthOptions{})
			if err != nil || health.ManifestVersion != version || health.CatalogVersion >= version || health.Status == "ready" {
				t.Fatalf("published a ready catalog before the reverse write succeeded: %+v, err = %v", health, err)
			}
			objects.failing.Store(false)
			if _, err := store.RebuildIndexes(ctx, tenant); err != nil {
				t.Fatal(err)
			}
			health, err = store.IndexHealth(ctx, tenant)
			if err != nil || health.Status != "ready" || health.CatalogVersion != version {
				t.Fatalf("catalogs did not recover together: %+v, err = %v", health, err)
			}
		})
	}
}

func TestIndexHealthFailsWhenReverseIndexCatalogIsMissing(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{ID: "host:a", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{{
			Type: "runs_on", From: "service:api", To: "host:a",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	if err := objects.Delete(
		ctx, store.reverseIndexCatalogKey("tenant-a"),
	); err != nil {
		t.Fatalf("delete reverse catalog: %v", err)
	}

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("index health: %v", err)
	}
	if health.Status != "error" {
		t.Fatalf("health = %#v, want error", health)
	}
	if !hasIssueContaining(health.Issues, "reverse index catalog is missing") {
		t.Fatalf("issues = %#v, want missing reverse catalog", health.Issues)
	}
}

func TestIndexHealthFailsWhenCatalogOmitsRequiredIndexes(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
			},
		}},
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "service:api", Kind: "service"},
			{
				ID: "host:a", Kind: "host",
				Fields: map[string]any{"hostname": "app-a"},
			},
		},
		UpsertEdges: []graph.Edge{{
			Type: "runs_on", From: "service:api", To: "host:a",
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	catalog, err := store.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	catalog.Indexes = nil
	catalog.EdgeShards = nil
	catalog.EntityPages = nil
	data, err := marshalParquetIndexCatalog(ctx, catalog)
	if err != nil {
		t.Fatalf("marshal truncated catalog: %v", err)
	}
	if err := objects.Put(ctx, store.indexCatalogKey("tenant-a"), data); err != nil {
		t.Fatalf("write truncated catalog: %v", err)
	}

	healthStore := NewTenantStore(objects, "test")
	health, err := healthStore.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("index health: %v", err)
	}
	if health.Status != "error" {
		t.Fatalf("health = %#v, want error", health)
	}
	for _, fragment := range []string{
		"field index host.hostname is missing from catalog",
		"edge shard runs_on/",
		"entity page ",
	} {
		if !hasIssueContaining(health.Issues, fragment) {
			t.Fatalf("issues = %#v, want %q", health.Issues, fragment)
		}
	}
}

func hasIssueContaining(issues []string, fragment string) bool {
	for _, issue := range issues {
		if strings.Contains(issue, fragment) {
			return true
		}
	}
	return false
}
