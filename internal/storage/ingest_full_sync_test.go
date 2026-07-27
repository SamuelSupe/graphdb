package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestFullSyncFailureSuppressesStaleSweep(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "seed",
		Items: []IngestItem{
			{
				ExternalID: "i-1",
				Entity:     &graph.Entity{Kind: "host"},
			},
			{
				ExternalID: "i-2",
				Entity:     &graph.Entity{Kind: "host"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "partial-full-sync",
		FullSync:    true,
		StaleAction: "delete",
		Items: []IngestItem{
			{
				ExternalID: "i-1",
				Entity:     &graph.Entity{Kind: "host"},
			},
			{ExternalID: "i-2"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Applied != 1 || result.Failed != 1 {
		t.Fatalf("partial full-sync result = %#v", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	entityID := graph.CanonicalEntityIDParts("host", "aws", "i-2")
	entity, ok := g.GetEntity(entityID)
	if !ok {
		t.Fatal("failed full-sync item caused existing entity deletion")
	}
	for _, source := range entity.Sources {
		if source.Source == "aws" && source.Stale {
			t.Fatalf("failed full-sync item caused stale mark: %#v", entity.Sources)
		}
	}
}

func TestIngestFullSyncObservedIDsRespectSourceAndKind(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "seed-scope",
		Items: []IngestItem{
			{
				ExternalID: "shared-kind",
				Entity:     &graph.Entity{Kind: "host"},
			},
			{
				ExternalID: "shared-source",
				Entity:     &graph.Entity{Kind: "host"},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	result, err := store.Ingest(ctx, "tenant-a", IngestRequest{
		Source:      "aws",
		CollectorID: "collector-a",
		BatchID:     "scoped-full-sync",
		FullSync:    true,
		StaleAction: "delete",
		StaleKind:   "host",
		Items: []IngestItem{
			{
				ExternalID: "shared-kind",
				Entity:     &graph.Entity{Kind: "service"},
			},
			{
				ExternalID: "shared-source",
				Entity: &graph.Entity{
					Kind: "host", Source: "manual",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Failed != 0 {
		t.Fatalf("scoped full-sync result = %#v", result)
	}
	g, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, externalID := range []string{"shared-kind", "shared-source"} {
		entityID := graph.CanonicalEntityIDParts("host", "aws", externalID)
		if _, ok := g.GetEntity(entityID); ok {
			t.Fatalf("out-of-scope observation protected %q", entityID)
		}
	}
}
