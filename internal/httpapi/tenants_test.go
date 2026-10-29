package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHTTPMissingTenantMetadataRequiresExplicitState(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "legacy", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := store.Objects.Delete(ctx, "test/tenants/legacy/metadata.parquet"); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	response := serveJSON(handler, http.MethodPut, "/v1/tenants/legacy", "", map[string]any{"name": "recovered"})
	var failure struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &failure); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusConflict || failure.Code != string(ErrorCodeRepairRequired) || failure.Retryable {
		t.Fatalf("missing state silently accepted: %d %s", response.Code, response.Body.String())
	}
	response = serveJSON(handler, http.MethodPost, "/v1/tenants/legacy/disable", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("explicit disable: %d %s", response.Code, response.Body.String())
	}
	response = serveJSON(handler, http.MethodPut, "/v1/tenants/legacy", "", map[string]any{"name": "recovered"})
	var info storage.TenantInfo
	if err := json.Unmarshal(response.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || info.Status != storage.TenantStatusDisabled || info.ManifestVersion != 1 {
		t.Fatalf("metadata update changed confirmed state: %d %s", response.Code, response.Body.String())
	}
}

func TestHTTPStandaloneMetadataRecoversWithoutConfiguration(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := storage.NewTenantStore(files, "test")
	defer store.ShutdownTasks(ctx)
	if _, err := store.Commit(ctx, "legacy", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"test/tenants/legacy/metadata.parquet", "test/tenants/legacy/control/tenant-metadata-backup.parquet"} {
		if err := files.Delete(ctx, key); err != nil {
			t.Fatal(err)
		}
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	response := serveJSON(handler, http.MethodPut, "/v1/tenants/legacy", "", map[string]any{"name": "recovered"})
	if response.Code != http.StatusOK {
		t.Fatalf("ordinary PUT requires manual recovery: %d %s", response.Code, response.Body.String())
	}
	response = serveJSON(handler, http.MethodPost, "/v1/tenants/legacy/disable", "", nil)
	if response.Code != http.StatusOK {
		t.Fatalf("disable: %d %s", response.Code, response.Body.String())
	}
	if err := files.Put(ctx, "test/tenants/legacy/metadata.parquet", []byte("damaged")); err != nil {
		t.Fatal(err)
	}
	response = serveJSON(handler, http.MethodGet, "/v1/tenants/legacy", "", nil)
	var info storage.TenantInfo
	if err := json.Unmarshal(response.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || info.Status != storage.TenantStatusDisabled || info.Name != "recovered" || info.ManifestVersion != 1 {
		t.Fatalf("GET did not preserve recovered state: %d %s", response.Code, response.Body.String())
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		primary, err := files.Get(ctx, "test/tenants/legacy/metadata.parquet")
		backup, backupErr := files.Get(ctx, "test/tenants/legacy/control/tenant-metadata-backup.parquet")
		if err == nil && backupErr == nil && string(primary) == string(backup) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("GET did not trigger durable online recovery")
}

func TestHTTPRestoreSourceRequiresExplicitPlatformMigration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	defer store.ShutdownTasks(context.Background())
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	backup, err := store.StartTask(ctx, "tenant-a", storage.TaskTypeTenantBackup, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup = waitForHTTPTask(t, store, "tenant-a", backup.ID)
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	for _, test := range []struct {
		target, scope, source, terminal string
		status                          int
	}{
		{"implicit", "", "", storage.TaskStatusFailed, http.StatusAccepted},
		{"explicit", "", "tenant-a", storage.TaskStatusSucceeded, http.StatusAccepted},
		{"restricted", "restricted", "tenant-a", "", http.StatusForbidden},
	} {
		response := serveJSON(handler, http.MethodPost, "/v1/tenants/"+test.target+"/restore", test.scope, map[string]any{"backup_key": backup.ResultKey, "source_tenant_id": test.source})
		if response.Code != test.status {
			t.Fatalf("target=%s status=%d body=%s", test.target, response.Code, response.Body)
		}
		if test.terminal == "" {
			continue
		}
		var task storage.Task
		if err := json.Unmarshal(response.Body.Bytes(), &task); err != nil {
			t.Fatal(err)
		}
		for {
			current, err := store.GetTask(ctx, test.target, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			if current.Status != storage.TaskStatusQueued && current.Status != storage.TaskStatusRunning {
				if current.Status != test.terminal {
					t.Fatalf("target=%s status=%s error=%s", test.target, current.Status, current.Error)
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(time.Millisecond):
			}
		}
	}
}

func TestHTTPTenantLifecycleCreateDisableEnableDeletePurge(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	create := serveJSON(handler, http.MethodPost, "/v1/tenants", "", tenantRequest{
		TenantID: "tenant-a", Name: "Tenant A", Labels: map[string]string{"env": "test"},
	})
	if create.Code != http.StatusOK || !strings.Contains(create.Body.String(), `"status":"active"`) {
		t.Fatalf("create tenant = %d body=%s", create.Code, create.Body.String())
	}
	commit := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}})
	if commit.Code != http.StatusOK {
		t.Fatalf("commit active = %d body=%s", commit.Code, commit.Body.String())
	}
	disabled := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/disable", "", nil)
	if disabled.Code != http.StatusOK || !strings.Contains(disabled.Body.String(), `"status":"disabled"`) {
		t.Fatalf("disable tenant = %d body=%s", disabled.Code, disabled.Body.String())
	}
	blocked := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:b", Kind: "host"}},
	}})
	var body ErrorResponse
	if err := json.Unmarshal(blocked.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode disabled response: %v body=%s", err, blocked.Body.String())
	}
	if blocked.Code != http.StatusForbidden || body.Code != ErrorCodeTenantDisabled {
		t.Fatalf("disabled commit = %d body=%#v raw=%s", blocked.Code, body, blocked.Body.String())
	}
	enabled := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/enable", "", nil)
	if enabled.Code != http.StatusOK || !strings.Contains(enabled.Body.String(), `"status":"active"`) {
		t.Fatalf("enable tenant = %d body=%s", enabled.Code, enabled.Body.String())
	}
	deleted := serveJSON(handler, http.MethodDelete, "/v1/tenants/tenant-a", "", nil)
	if deleted.Code != http.StatusOK || !strings.Contains(deleted.Body.String(), `"status":"deleted"`) {
		t.Fatalf("delete tenant = %d body=%s", deleted.Code, deleted.Body.String())
	}
	readDeleted := serveJSON(handler, http.MethodGet, "/v1/entities/host:a", "tenant-a", nil)
	if err := json.Unmarshal(readDeleted.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode deleted response: %v body=%s", err, readDeleted.Body.String())
	}
	if readDeleted.Code != http.StatusGone || body.Code != ErrorCodeTenantDeleted {
		t.Fatalf("read deleted = %d body=%#v raw=%s", readDeleted.Code, body, readDeleted.Body.String())
	}
	purged := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/purge", "", nil)
	if purged.Code != http.StatusOK || !strings.Contains(purged.Body.String(), `"deleted":`) {
		t.Fatalf("purge tenant = %d body=%s", purged.Code, purged.Body.String())
	}
}

func TestHTTPTenantClone(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	if rr := serveJSON(handler, http.MethodPost, "/v1/tenants", "", tenantRequest{TenantID: "tenant-a", Name: "source"}); rr.Code != http.StatusOK {
		t.Fatalf("create source = %d body=%s", rr.Code, rr.Body.String())
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}},
	}}); rr.Code != http.StatusOK {
		t.Fatalf("commit source = %d body=%s", rr.Code, rr.Body.String())
	}
	clone := serveJSON(handler, http.MethodPost, "/v1/tenants/tenant-a/clone", "", tenantCloneRequest{TargetTenantID: "tenant-b"})
	if clone.Code != http.StatusAccepted || !strings.Contains(clone.Body.String(), `"tenant_id":"tenant-b"`) || !strings.Contains(clone.Body.String(), `"cloned_from":"tenant-a"`) {
		t.Fatalf("clone tenant = %d body=%s", clone.Code, clone.Body.String())
	}
	readClone := serveJSON(handler, http.MethodGet, "/v1/entities/host:a", "tenant-b", nil)
	if readClone.Code != http.StatusOK || !strings.Contains(readClone.Body.String(), `"host:a"`) {
		t.Fatalf("read clone = %d body=%s", readClone.Code, readClone.Body.String())
	}
}

func TestHTTPTenantListUsesManagedRegistryByDefault(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	if _, err := store.Commit(context.Background(), "legacy-tenant", graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "host:legacy", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit legacy tenant: %v", err)
	}
	if rr := serveJSON(handler, http.MethodPost, "/v1/tenants", "", tenantRequest{TenantID: "managed-tenant"}); rr.Code != http.StatusOK {
		t.Fatalf("create managed tenant = %d body=%s", rr.Code, rr.Body.String())
	}
	list := serveJSON(handler, http.MethodGet, "/v1/tenants", "", nil)
	if list.Code != http.StatusOK || !strings.Contains(list.Body.String(), `"managed-tenant"`) || !strings.Contains(list.Body.String(), `"legacy-tenant"`) {
		t.Fatalf("managed list = %d body=%s", list.Code, list.Body.String())
	}
	legacy := serveJSON(handler, http.MethodGet, "/v1/tenants?include_legacy=true", "", nil)
	if legacy.Code != http.StatusOK || !strings.Contains(legacy.Body.String(), `"managed-tenant"`) || !strings.Contains(legacy.Body.String(), `"legacy-tenant"`) {
		t.Fatalf("legacy list = %d body=%s", legacy.Code, legacy.Body.String())
	}
}

func TestLocalHTTPLifecyclePurgeWithTenantHeader(t *testing.T) {
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	store := storage.NewTenantStore(files, "review")
	if _, err = store.CreateTenant(context.Background(), "tenant-a", storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/tenants/tenant-a/purge?force=true", nil).WithContext(ctx)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	recorder := httptest.NewRecorder()
	start := time.Now()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("purge holding its own read view returned %d after %v: %s", recorder.Code, time.Since(start), recorder.Body.String())
	}
}

func TestHTTPTenantPaginationAndSearch(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	for _, id := range []string{"team-a", "team-b", "other"} {
		if _, err := store.CreateTenant(context.Background(), id, storage.TenantCreateOptions{Name: id}); err != nil {
			t.Fatal(err)
		}
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	for _, sample := range []struct {
		path  string
		id    string
		total int
		next  string
	}{
		{"/v1/tenants?limit=1&q=TEAM", "team-a", 2, "team-a"},
		{"/v1/tenants?limit=1&q=team&cursor=team-a", "team-b", 2, ""},
	} {
		rr := serveJSON(handler, http.MethodGet, sample.path, "", nil)
		var page storage.TenantInfoPage
		if rr.Code != http.StatusOK || json.Unmarshal(rr.Body.Bytes(), &page) != nil || len(page.Tenants) != 1 || page.Tenants[0].TenantID != sample.id || page.Total != sample.total || page.NextCursor != sample.next {
			t.Fatalf("%s: %d %s", sample.path, rr.Code, rr.Body.String())
		}
	}
	for _, query := range []string{"limit=0", "limit=201", "limit=abc", "cursor=../invalid", "q=" + strings.Repeat("a", 129)} {
		rr := serveJSON(handler, http.MethodGet, "/v1/tenants?"+query, "", nil)
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("query %s: %d %s", query, rr.Code, rr.Body.String())
		}
	}
}

func TestTenantLifecycleRejectsMismatchedAuthenticatedScope(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	if _, err := store.CreateTenant(context.Background(), "tenant-b", storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, path string }{{"POST", "/v1/tenants/tenant-b/purge"}, {"DELETE", "/v1/tenants/tenant-b"}, {"GET", "/v1/tenants/tenant-b/backups"}} {
		rr := serveJSON(handler, tc.method, tc.path, "tenant-a", nil)
		if rr.Code != http.StatusForbidden {
			t.Fatalf("%s: %d %s", tc.path, rr.Code, rr.Body.String())
		}
	}
	if _, err := store.GetTenantInfo(context.Background(), "tenant-b"); err != nil {
		t.Fatal(err)
	}
}
