package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"graphdb/internal/query"
)

func TestSaveQueryRequiresWriterLease(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	owner := NewTenantStore(objects, "test")
	owner.LeaseTTL = time.Hour
	if _, err := owner.SaveQuery(ctx, "tenant-a", SavedQuery{Name: "hosts", Request: query.Request{Op: "match", Kind: "host"}}); err != nil {
		t.Fatalf("owner save query: %v", err)
	}

	other := NewTenantStore(objects, "test")
	_, err := other.SaveQuery(ctx, "tenant-a", SavedQuery{Name: "hosts-2", Request: query.Request{Op: "match", Kind: "host"}})
	if !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("save query err = %v, want ErrLeaseHeld", err)
	}
	if _, err := owner.GetSavedQuery(ctx, "tenant-a", "hosts-2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unexpected hosts-2 query err = %v", err)
	}
}

func TestSaveQueryDoesNotOverwriteAfterLeaseTakeover(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	store := NewTenantStore(base, "test")
	objects := &takeoverDuringSaveQueryStore{
		ObjectStore: base,
		base:        base,
		tenantID:    "tenant-a",
		triggerKey:  store.savedQueryKey("tenant-a", "hosts"),
	}
	stale := NewTenantStore(objects, "test")
	stale.LeaseTTL = time.Nanosecond
	_, err := stale.SaveQuery(ctx, "tenant-a", SavedQuery{
		Name:        "hosts",
		Description: "stale",
		Request:     query.Request{Op: "match", Kind: "host"},
	})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("stale save err = %v, want ErrConflict", err)
	}
	if !objects.Triggered() {
		t.Fatal("test store did not trigger takeover")
	}

	saved, err := store.GetSavedQuery(ctx, "tenant-a", "hosts")
	if err != nil {
		t.Fatalf("get saved query: %v", err)
	}
	if saved.Description != "fresh" {
		t.Fatalf("saved query was overwritten: %#v", saved)
	}
}

type takeoverDuringSaveQueryStore struct {
	ObjectStore
	base       *MemoryStore
	tenantID   string
	triggerKey string

	mu        sync.Mutex
	triggered bool
}

func (s *takeoverDuringSaveQueryStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if s.shouldTrigger(key) {
		time.Sleep(time.Millisecond)
		takeover := NewTenantStore(s.base, "test")
		takeover.LeaseTTL = time.Hour
		if _, err := takeover.SaveQuery(ctx, s.tenantID, SavedQuery{
			Name:        "hosts",
			Description: "fresh",
			Request:     query.Request{Op: "match", Kind: "host"},
		}); err != nil {
			return ObjectMeta{}, err
		}
	}
	return s.ObjectStore.PutConditional(ctx, key, data, condition)
}

func (s *takeoverDuringSaveQueryStore) shouldTrigger(key string) bool {
	if key != s.triggerKey {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.triggered {
		return false
	}
	s.triggered = true
	return true
}

func (s *takeoverDuringSaveQueryStore) Triggered() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.triggered
}
