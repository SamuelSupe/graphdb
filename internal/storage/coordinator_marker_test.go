package storage

import (
	"context"
	"testing"
)

func TestLocalWriterRejectsForeignOrInvalidCoordinationMarker(t *testing.T) {
	for _, marker := range []string{`{"layout_version":1,"backend":"postgres","namespace":"cluster-a"}`, `{"layout_version":2,"backend":"local"}`, `invalid`} {
		store := NewTenantStore(NewMemoryStore(), "test")
		ctx := context.Background()
		if err := store.Objects.Put(ctx, store.coordinationMarkerKey(), []byte(marker)); err != nil {
			t.Fatal(err)
		}
		if err := store.EnsureLocalWriterAllowed(ctx); err == nil {
			t.Fatalf("accepted incompatible marker %s", marker)
		}
	}
}
