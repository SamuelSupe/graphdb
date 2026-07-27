package storage

import (
	"context"
	"errors"
	"testing"
)

func TestCoordinationMarkerCannotChangeNamespace(t *testing.T) {
	store := NewTenantStore(NewMemoryStore(), "test")
	ctx := context.Background()
	if err := store.PutCoordinationMarker(ctx, CoordinationPostgres, "cluster-a"); err != nil {
		t.Fatalf("put initial marker: %v", err)
	}
	if err := store.PutCoordinationMarker(ctx, CoordinationPostgres, "cluster-a"); err != nil {
		t.Fatalf("repeat marker: %v", err)
	}
	if err := store.PutCoordinationMarker(ctx, CoordinationPostgres, "cluster-b"); !errors.Is(err, ErrConflict) {
		t.Fatalf("replace marker = %v, want ErrConflict", err)
	}
}
