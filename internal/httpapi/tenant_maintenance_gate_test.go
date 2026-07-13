package httpapi

import (
	"net/http"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPDisabledTenantRejectsMaintenanceMutations(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.CreateTenant(t.Context(), "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := store.SetTenantStatus(t.Context(), "tenant-a", storage.TenantStatusDisabled); err != nil {
		t.Fatalf("disable tenant: %v", err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	for _, path := range []string{
		"/v1/compact",
		"/v1/control/recover",
		"/v1/control/repair",
		"/v1/control/cleanup-commits",
		"/v1/control/gc",
	} {
		t.Run(path, func(t *testing.T) {
			rr := serveJSON(handler, http.MethodPost, path, "tenant-a", nil)
			if rr.Code != http.StatusForbidden {
				t.Fatalf("status = %d body=%s, want 403", rr.Code, rr.Body.String())
			}
		})
	}
}
