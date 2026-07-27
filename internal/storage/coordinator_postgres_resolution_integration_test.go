package storage

import (
	"errors"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorResolvesLifecycleAndContextOutcomes(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(t, "transition-resolution")
	store := NewTenantStore(NewMemoryStore(), "test")
	store.SetCoordinator(coordinator)
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:first", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}

	disabled, err := coordinator.TransitionTenant(
		ctx, "tenant-a", TenantStatusDisabled, true,
	)
	if err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	resolved, err := coordinator.resolveAmbiguousTenantTransition(disabled, true)
	if err != nil || resolved.Generation != disabled.Generation {
		t.Fatalf("resolve disabled head=%#v err=%v", resolved, err)
	}

	active, err := coordinator.TransitionTenant(
		ctx, "tenant-a", TenantStatusActive, true,
	)
	if err != nil {
		t.Fatalf("enable tenant: %v", err)
	}
	if _, err := coordinator.resolveAmbiguousTenantTransition(disabled, true); !errors.Is(err, ErrCoordinatorUnavailable) {
		t.Fatalf("resolve stale transition err = %v, want coordinator unavailable", err)
	}

	firstContext, published, err := coordinator.PublishWriteContext(
		ctx,
		WriteContextPublishRequest{
			TenantID:           "tenant-a",
			ExpectedRevision:   active.Revision,
			ExpectedGeneration: active.Generation,
			ExpectedContext:    active.WriteContextRevision,
			WriteContextKey:    "contexts/first.json",
			WriteContextHash:   "first-hash",
		},
	)
	if err != nil || !published {
		t.Fatalf("publish first context=%#v published=%v err=%v", firstContext, published, err)
	}
	resolvedContext, resolvedPublished, err := coordinator.resolveAmbiguousWriteContext(firstContext)
	if err != nil || !resolvedPublished ||
		resolvedContext.WriteContextHash != firstContext.WriteContextHash {
		t.Fatalf(
			"resolve first context=%#v published=%v err=%v",
			resolvedContext, resolvedPublished, err,
		)
	}

	_, published, err = coordinator.PublishWriteContext(
		ctx,
		WriteContextPublishRequest{
			TenantID:           "tenant-a",
			ExpectedRevision:   firstContext.Revision,
			ExpectedGeneration: firstContext.Generation,
			ExpectedContext:    firstContext.WriteContextRevision,
			WriteContextKey:    "contexts/second.json",
			WriteContextHash:   "second-hash",
		},
	)
	if err != nil || !published {
		t.Fatalf("publish second context published=%v err=%v", published, err)
	}
	if _, _, err := coordinator.resolveAmbiguousWriteContext(firstContext); !errors.Is(err, ErrCoordinatorUnavailable) {
		t.Fatalf("resolve stale context err = %v, want coordinator unavailable", err)
	}

	purging, err := coordinator.TransitionTenant(
		ctx, "tenant-a", TenantStatusDeleted, true,
	)
	if err != nil {
		t.Fatalf("transition tenant for purge: %v", err)
	}
	if err := coordinator.resolveAmbiguousTenantPurge(
		"tenant-a", purging.Generation,
	); !errors.Is(err, ErrCoordinatorUnavailable) {
		t.Fatalf("resolve unfinished purge err = %v, want coordinator unavailable", err)
	}
	if err := coordinator.FinalizeTenantPurge(
		ctx, "tenant-a", purging.Generation,
	); err != nil {
		t.Fatalf("finalize tenant purge: %v", err)
	}
	if err := coordinator.resolveAmbiguousTenantPurge(
		"tenant-a", purging.Generation,
	); err != nil {
		t.Fatalf("resolve finalized purge: %v", err)
	}
}
