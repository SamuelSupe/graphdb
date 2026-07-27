package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestLoadDoesNotReuseCacheAcrossTenantRecreate(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	original := NewTenantStore(objects, "test")
	if _, err := original.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("create original tenant: %v", err)
	}
	if _, err := original.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:old", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit original tenant: %v", err)
	}

	if err := expireWriterLeaseForTakeover(ctx, objects, "tenant-a"); err != nil {
		t.Fatalf("expire original writer lease: %v", err)
	}
	replacement := NewTenantStore(objects, "test")
	if _, err := replacement.SetTenantStatus(ctx, "tenant-a", TenantStatusDeleted); err != nil {
		t.Fatalf("soft delete original tenant: %v", err)
	}
	if _, err := replacement.PurgeTenant(ctx, "tenant-a", false); err != nil {
		t.Fatalf("purge original tenant: %v", err)
	}
	if _, err := replacement.CreateTenant(ctx, "tenant-a", TenantCreateOptions{}); err != nil {
		t.Fatalf("recreate tenant: %v", err)
	}
	if _, err := replacement.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:new", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("commit recreated tenant: %v", err)
	}

	for name, load := range map[string]func() (*graph.Graph, Manifest, error){
		"load":          func() (*graph.Graph, Manifest, error) { return original.Load(ctx, "tenant-a") },
		"load_at_least": func() (*graph.Graph, Manifest, error) { return original.LoadAtLeast(ctx, "tenant-a", 1) },
	} {
		t.Run(name, func(t *testing.T) {
			loaded, manifest, err := load()
			if err != nil {
				t.Fatalf("load recreated tenant: %v", err)
			}
			if manifest.Version != 1 {
				t.Fatalf("manifest version = %d, want 1", manifest.Version)
			}
			if _, ok := loaded.Entities["host:new"]; !ok {
				t.Fatalf("recreated entity missing: %#v", loaded.Entities)
			}
			if _, ok := loaded.Entities["host:old"]; ok {
				t.Fatalf("stale entity survived tenant recreation: %#v", loaded.Entities)
			}
		})
	}
}
