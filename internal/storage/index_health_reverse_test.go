package storage

import (
	"context"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

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
