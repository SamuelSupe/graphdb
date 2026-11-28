package httpapi

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestErrorCodeContractTableIsExhaustive(t *testing.T) {
	declared := declaredErrorCodeConstants(t)
	if len(declared) != len(stableErrorCodes) {
		t.Fatalf("declared ErrorCode const count = %d, stableErrorCodes len = %d", len(declared), len(stableErrorCodes))
	}
	for _, code := range stableErrorCodes {
		contract, ok := errorCodeContracts[code]
		if !ok {
			t.Fatalf("stable ErrorCode %q missing from errorCodeContracts", code)
		}
		if contract.Status < 400 || contract.Status >= 600 {
			t.Fatalf("ErrorCode %q has invalid HTTP status %d", code, contract.Status)
		}
		if got := storageErrorStatus(code); got != contract.Status {
			t.Fatalf("storageErrorStatus(%q) = %d, want %d", code, got, contract.Status)
		}
	}
	if len(errorCodeContracts) != len(stableErrorCodes) {
		t.Fatalf("errorCodeContracts has %d entries, want %d", len(errorCodeContracts), len(stableErrorCodes))
	}
	if got := storageErrorStatus(ErrorCode("unregistered_code")); got != http.StatusInternalServerError {
		t.Fatalf("unregistered error code fell back to %d, want 500 (never 400)", got)
	}
}

func TestErrorCodeContractMatchesDocsStatuses(t *testing.T) {
	docs := readTextFile(t, "../../docs/error_codes.md")
	rows := documentedErrorCodeStatuses(t, docs)
	for _, code := range stableErrorCodes {
		allowedStatuses, ok := rows[string(code)]
		if !ok {
			t.Fatalf("docs/error_codes.md missing status row for %q", code)
		}
		got := storageErrorStatus(code)
		matched := false
		for _, allowed := range allowedStatuses {
			if got == allowed {
				matched = true
				break
			}
		}
		if !matched {
			t.Fatalf("ErrorCode %q storageErrorStatus=%d, docs allow %v", code, got, allowedStatuses)
		}
	}
}

func TestBackpressureResponsesAlwaysEmitRetryAfterHeader(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	server := &Server{Store: store}
	reasonCodes := []string{
		"tenant_entity_quota_exceeded",
		"tenant_edge_quota_exceeded",
		"object_store_latency_high",
		"object_store_errors_high",
		"object_store_unavailable",
		"manifest_cas_conflicts_high",
		"index_rebuild_running",
		"gc_running",
		"commit_tail_too_long",
		"tenant_object_count_high",
		"tenant_bytes_high",
		"write_admission_queue_timeout",
		"ingest_wal_queue_full",
		"unknown_pressure_reason",
	}
	for _, reasonCode := range reasonCodes {
		t.Run(reasonCode, func(t *testing.T) {
			pressure := &storage.BackpressureError{
				Reasons:    []storage.BackpressureReason{{Code: reasonCode, Message: reasonCode}},
				RetryAfter: 1500 * time.Millisecond,
			}
			// Path 1: writeBackpressure
			rr := httptest.NewRecorder()
			server.writeBackpressure(rr, "tenant-a", pressure)
			if got := rr.Header().Get("Retry-After"); got != "2" {
				t.Fatalf("writeBackpressure(%s) Retry-After=%q, want 2", reasonCode, got)
			}
			var body ErrorResponse
			decodeResponse(t, rr, &body)
			if !isStableErrorCode(body.Code) {
				t.Fatalf("writeBackpressure(%s) returned unstable code %q", reasonCode, body.Code)
			}

			// Path 2: writeStorageError
			rr2 := httptest.NewRecorder()
			writeStorageError(rr2, pressure)
			if got := rr2.Header().Get("Retry-After"); got != "2" {
				t.Fatalf("writeStorageError(%s) Retry-After=%q, want 2", reasonCode, got)
			}
			var body2 ErrorResponse
			decodeResponse(t, rr2, &body2)
			if body2.Code != body.Code {
				t.Fatalf("writeStorageError(%s) code=%q != writeBackpressure code=%q", reasonCode, body2.Code, body.Code)
			}
		})
	}
}

func declaredErrorCodeConstants(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, "response.go", nil, 0)
	if err != nil {
		t.Fatalf("parse response.go: %v", err)
	}
	var names []string
	for _, decl := range parsed.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			typeIdent, ok := vs.Type.(*ast.Ident)
			if !ok || typeIdent.Name != "ErrorCode" {
				continue
			}
			for _, name := range vs.Names {
				names = append(names, name.Name)
			}
		}
	}
	sort.Strings(names)
	return names
}

func documentedErrorCodeStatuses(t *testing.T, docs string) map[string][]int {
	t.Helper()
	out := map[string][]int{}
	for _, line := range strings.Split(docs, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "| `") {
			continue
		}
		parts := strings.Split(trimmed, "|")
		if len(parts) < 4 {
			t.Fatalf("malformed error code row: %q", line)
		}
		code := strings.Trim(strings.TrimSpace(parts[1]), "`")
		rawStatus := strings.TrimSpace(parts[2])
		var statuses []int
		for _, part := range strings.Split(rawStatus, "/") {
			v, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil {
				t.Fatalf("parse status %q for %q: %v", rawStatus, code, err)
			}
			statuses = append(statuses, v)
		}
		out[code] = statuses
	}
	return out
}

func TestErrorCodesMatchGoAndPythonSDKs(t *testing.T) {
	want := make(map[string]struct{}, len(stableErrorCodes))
	for _, code := range stableErrorCodes {
		want[string(code)] = struct{}{}
	}

	// Verify Go SDK constants match stableErrorCodes 1:1.
	goSrc := readTextFile(t, "../../sdk/go/graphdb/error.go")
	goRe := regexp.MustCompile(`ErrorCode[A-Za-z0-9_]+\s*=\s*"([^"]+)"`)
	goMatches := goRe.FindAllStringSubmatch(goSrc, -1)
	goGot := make(map[string]struct{}, len(goMatches))
	for _, m := range goMatches {
		goGot[m[1]] = struct{}{}
	}
	for code := range want {
		if _, ok := goGot[code]; !ok {
			t.Errorf("Go SDK sdk/go/graphdb/error.go missing ErrorCode %q", code)
		}
	}
	for code := range goGot {
		if _, ok := want[code]; !ok {
			t.Errorf("Go SDK sdk/go/graphdb/error.go has unknown ErrorCode %q", code)
		}
	}

	// Verify Python SDK constants match stableErrorCodes 1:1.
	pySrc := readTextFile(t, "../../sdk/python/graphdb_sdk/errors.py")
	pyRe := regexp.MustCompile(`ERROR_CODE_[A-Z0-9_]+\s*=\s*"([^"]+)"`)
	pyMatches := pyRe.FindAllStringSubmatch(pySrc, -1)
	pyGot := make(map[string]struct{}, len(pyMatches))
	for _, m := range pyMatches {
		pyGot[m[1]] = struct{}{}
	}
	for code := range want {
		if _, ok := pyGot[code]; !ok {
			t.Errorf("Python SDK sdk/python/graphdb_sdk/errors.py missing ERROR_CODE %q", code)
		}
	}
	for code := range pyGot {
		if _, ok := want[code]; !ok {
			t.Errorf("Python SDK sdk/python/graphdb_sdk/errors.py has unknown ERROR_CODE %q", code)
		}
	}
}

func TestAuthTokenAndRequireTenantScopeMiddleware(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	srv := &Server{
		Store:              store,
		AuthToken:          "data-secret",
		AdminToken:         "admin-secret",
		RequireTenantScope: true,
	}
	h := srv.Handler()

	// Health bypasses auth.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/health", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /v1/health = %d, want 200", rr.Code)
	}

	// Data route without token rejected with 401.
	req := httptest.NewRequest(http.MethodGet, "/v1/entities", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET /v1/entities = %d, want 401", rr.Code)
	}

	// Admin route rejects data token, accepts admin token.
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer data-secret")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("data token on /v1/tenants = %d, want 401", rr.Code)
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer admin-secret")
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("admin token on /v1/tenants = %d, want 200", rr.Code)
	}

	// Privileged admin routes beyond /v1/tenants must also reject data token.
	for _, tc := range []struct{ method, path string }{
		{http.MethodPut, "/v1/tenant-config"},
		{http.MethodPost, "/v1/control/gc"},
		{http.MethodPost, "/v1/tasks"},
		{http.MethodGet, "/v1/indexes/health"},
	} {
		req = httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", "Bearer data-secret")
		req.Header.Set("X-Tenant-ID", "t1")
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("data token on admin route %s %s = %d, want 401", tc.method, tc.path, rr.Code)
		}
	}

	// InternalHandler bypasses external bearer auth for Raft replay / private cluster data.
	internalH := srv.InternalHandler()
	req = httptest.NewRequest(http.MethodGet, "/v1/entities", nil)
	req.Header.Set("X-Tenant-ID", "t1")
	rr = httptest.NewRecorder()
	internalH.ServeHTTP(rr, req)
	if rr.Code == http.StatusUnauthorized {
		t.Fatalf("InternalHandler unexpectedly returned 401")
	}

	// GET /v1/tenants without X-Tenant-ID must be rejected when RequireTenantScope=true and caller is not admin.
	scopedOnly := &Server{
		Store:              store,
		AuthToken:          "data-secret",
		RequireTenantScope: true,
	}
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer data-secret")
	rr = httptest.NewRecorder()
	scopedOnly.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("non-admin GET /v1/tenants without X-Tenant-ID = %d, want 403", rr.Code)
	}

	// RequireTenantScope fails closed when X-Tenant-ID is missing on lifecycle route for non-admin.
	srvScoped := &Server{Store: store, AuthToken: "data-secret", RequireTenantScope: true}
	req = httptest.NewRequest(http.MethodGet, "/v1/tenants/t1", nil)
	req.Header.Set("Authorization", "Bearer data-secret")
	rr = httptest.NewRecorder()
	srvScoped.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("missing X-Tenant-ID with RequireTenantScope = %d, want 403 (body=%s)", rr.Code, rr.Body.String())
	}
}

func TestStreamIndexedSnapshotEmitsSchemaTypes(t *testing.T) {
	ctx := t.Context()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes:       []graph.CIType{{Name: "host"}},
		UpsertRelationTypes: []graph.RelationType{{Name: "runs_on", FromKind: "host", ToKind: "host"}},
		UpsertEntities:      []graph.Entity{{ID: "ci:1", Kind: "host"}, {ID: "ci:2", Kind: "host"}},
		UpsertEdges:         []graph.Edge{{ID: "e:1", Type: "runs_on", From: "ci:1", To: "ci:2"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatalf("rebuild indexes: %v", err)
	}
	srv := &Server{Store: store, Mode: "reader"}
	req := httptest.NewRequest(http.MethodGet, "/v1/export/snapshot/stream?inline=true", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /v1/export/snapshot/stream = %d, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, `"indexed_read":true`) {
		t.Fatalf("expected indexed_read snapshot stream, got: %s", body)
	}
	if !strings.Contains(body, `"ci_type":`) || !strings.Contains(body, `"relation_type":`) {
		t.Fatalf("expected ci_type and relation_type records in indexed snapshot stream, got: %s", body)
	}
}
