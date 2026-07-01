package graphdb

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/httpapi"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestGoSDKCompleteFlowAgainstRealServer(t *testing.T) {
	ctx := context.Background()
	server := newSDKTestServer(t)
	defer server.Close()

	client := newSDKTestClient(t, server.URL, "go-sdk-e2e")
	if health, err := client.Health(ctx); err != nil || health["status"] != "ok" {
		t.Fatalf("health = %#v err=%v", health, err)
	}
	created, err := client.CreateTenant(ctx, TenantCreateRequest{TenantID: "go-sdk-e2e", Name: "Go SDK E2E"})
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if created.TenantID != "go-sdk-e2e" || created.Status != "active" {
		t.Fatalf("created tenant = %#v", created)
	}
	if tenants, err := client.ListTenants(ctx, false); err != nil || !hasTenant(tenants, "go-sdk-e2e") {
		t.Fatalf("list tenants = %#v err=%v", tenants, err)
	}
	if tenant, err := client.GetTenant(ctx, "go-sdk-e2e"); err != nil || tenant.Status != "active" {
		t.Fatalf("get tenant = %#v err=%v", tenant, err)
	}

	policy := SourcePolicy{DefaultPriority: 0, Sources: []SourcePolicyItem{
		{Name: "manual", Priority: 1000},
		{Name: "agent", Priority: 100},
	}, FieldAliases: []FieldAliasRule{
		{Source: "agent", Kind: "host", Aliases: map[string]string{"host_name": "hostname"}},
	}, FieldPriorities: []FieldPriorityRule{
		{Source: "agent", Kind: "host", Fields: map[string]int{"hostname": 150}},
	}}
	if out, err := client.PutSourcePolicy(ctx, policy); err != nil || !out.Configured || len(out.Policy.Sources) != 2 || len(out.Policy.FieldAliases) != 1 || len(out.Policy.FieldPriorities) != 1 {
		t.Fatalf("put source policy = %#v err=%v", out, err)
	}
	if out, err := client.GetSourcePolicy(ctx); err != nil || !out.Configured || out.Policy.FieldAliases[0].Aliases["host_name"] != "hostname" || out.Policy.FieldPriorities[0].Fields["hostname"] != 150 {
		t.Fatalf("get source policy = %#v err=%v", out, err)
	}
	if out, err := client.PutTenantConfig(ctx, map[string]any{"retention": map[string]any{"keep_snapshots": 2}}); err != nil || !out.Configured {
		t.Fatalf("put tenant config = %#v err=%v", out, err)
	}
	if out, err := client.GetTenantConfig(ctx); err != nil || !out.Configured {
		t.Fatalf("get tenant config = %#v err=%v", out, err)
	}

	commit := seedSDKGraph(t, ctx, client)
	if commit.Version == 0 || len(commit.CanonicalEdges) != 1 {
		t.Fatalf("commit = %#v", commit)
	}
	readOptions := &ReadOptions{MinVersion: commit.Version}
	if entity, err := client.GetEntity(ctx, "host:go", readOptions); err != nil || entity.Fields["hostname"] != "go-host" {
		t.Fatalf("get entity = %#v err=%v", entity, err)
	}
	if entities, err := client.ListEntities(ctx, EntityListOptions{Kind: "host", Limit: 10}); err != nil || len(entities.Entities) != 1 {
		t.Fatalf("list entities = %#v err=%v", entities, err)
	}
	if edges, err := client.ListEdges(ctx, EdgeListOptions{Type: "runs_on", Limit: 10}); err != nil || len(edges.Edges) != 1 {
		t.Fatalf("list edges = %#v err=%v", edges, err)
	}
	stream, err := client.StreamEntities(ctx, EntityListOptions{Kind: "host", Limit: 10})
	if count := countSDKStreamItems(t, mustStream(t, stream, err), "entity"); count != 1 {
		t.Fatalf("stream entities count = %d", count)
	}
	stream, err = client.StreamEdges(ctx, EdgeListOptions{Type: "runs_on", Limit: 10})
	if count := countSDKStreamItems(t, mustStream(t, stream, err), "edge"); count != 1 {
		t.Fatalf("stream edges count = %d", count)
	}
	if snapshot, err := client.ExportSnapshot(ctx, ReadOptions{}); err != nil || snapshot["version"] == nil {
		t.Fatalf("export snapshot = %#v err=%v", snapshot, err)
	}
	stream, err = client.StreamSnapshot(ctx, ReadOptions{}, true)
	if count := countSDKStream(t, mustStream(t, stream, err)); count == 0 {
		t.Fatalf("stream snapshot count = %d", count)
	}
	if ciTypes, err := client.ListCITypes(ctx, nil); err != nil || ciTypes["ci_types"] == nil {
		t.Fatalf("ci types = %#v err=%v", ciTypes, err)
	}
	if relationTypes, err := client.ListRelationTypes(ctx, nil); err != nil || relationTypes["relation_types"] == nil {
		t.Fatalf("relation types = %#v err=%v", relationTypes, err)
	}

	match, err := client.Query(ctx, QueryRequest{Op: "match", Kind: "host", Filters: Fields{"hostname": "go-host"}, Limit: 10})
	if err != nil || len(match.Results) != 1 || match.Results[0].Entity.ID != "host:go" {
		t.Fatalf("query match = %#v err=%v", match, err)
	}
	gql, err := client.GQL(ctx, `FIND host WHERE hostname = "go-host" LIMIT 10`)
	if err != nil || len(gql.Results) != 1 || gql.Results[0].Entity.ID != "host:go" {
		t.Fatalf("gql = %#v err=%v", gql, err)
	}
	stream, err = client.QueryStream(ctx, QueryRequest{Op: "match", Kind: "host", Limit: 10})
	if count := countSDKStream(t, mustStream(t, stream, err)); count == 0 {
		t.Fatalf("query stream count = %d", count)
	}
	stream, err = client.GQLStream(ctx, "FIND host LIMIT 10")
	if count := countSDKStream(t, mustStream(t, stream, err)); count == 0 {
		t.Fatalf("gql stream count = %d", count)
	}
	saved, err := client.SaveQuery(ctx, SavedQuery{Name: "go-host-by-name", Request: QueryRequest{Op: "match", Kind: "host", Filters: Fields{"hostname": "go-host"}, Limit: 1}})
	if err != nil || saved.Name != "go-host-by-name" {
		t.Fatalf("save query = %#v err=%v", saved, err)
	}
	if savedQueries, err := client.ListQueries(ctx); err != nil || len(savedQueries["queries"]) != 1 {
		t.Fatalf("list queries = %#v err=%v", savedQueries, err)
	}
	if run, err := client.RunSavedQuery(ctx, "go-host-by-name"); err != nil || len(run.Results) != 1 {
		t.Fatalf("run saved query = %#v err=%v", run, err)
	}
	if running, err := client.ListRunningQueries(ctx); err != nil || running["queries"] == nil {
		t.Fatalf("running queries = %#v err=%v", running, err)
	}

	if rebuilt, err := client.RebuildIndexes(ctx, false); err != nil || rebuilt["version"] == nil {
		t.Fatalf("rebuild indexes = %#v err=%v", rebuilt, err)
	}
	if catalog, err := client.IndexCatalog(ctx); err != nil || catalog["version"] == nil {
		t.Fatalf("index catalog = %#v err=%v", catalog, err)
	}
	if health, err := client.IndexHealth(ctx, false); err != nil || health["status"] == nil {
		t.Fatalf("index health = %#v err=%v", health, err)
	}
	if createdIndex, err := client.CreateIndex(ctx, IndexDefinition{Kind: "host", Field: "hostname"}); err != nil || createdIndex["task"] == nil {
		t.Fatalf("create index = %#v err=%v", createdIndex, err)
	}
	if definitions, err := client.ListIndexDefinitions(ctx); err != nil || len(definitions) == 0 {
		t.Fatalf("index definitions = %#v err=%v", definitions, err)
	}
	waitForSDKTaskQuietly(ctx, client, 2*time.Second)
	if dropped, err := client.DropIndex(ctx, "host.hostname"); err != nil || dropped["task"] == nil {
		t.Fatalf("drop index = %#v err=%v", dropped, err)
	}

	if freshness, err := client.ReaderFreshness(ctx); err != nil || freshness["visible_version"] == nil {
		t.Fatalf("reader freshness = %#v err=%v", freshness, err)
	}
	readinessQuery := url.Values{}
	readinessQuery.Set("allow_stale", "true")
	if readiness, err := client.ReaderFleetReadiness(ctx, readinessQuery); err != nil || readiness["ready"] == nil {
		t.Fatalf("fleet readiness = %#v err=%v", readiness, err)
	}
	if traffic, err := client.ReaderTrafficGate(ctx, readinessQuery); err != nil || traffic["serve_traffic"] == nil {
		t.Fatalf("traffic gate = %#v err=%v", traffic, err)
	}
	if lease, err := client.WriterLease(ctx); err != nil || lease["owner_id"] == nil {
		t.Fatalf("writer lease = %#v err=%v", lease, err)
	}
	if audit, err := client.IntegrityAudit(ctx, false); err != nil || audit["status"] == nil {
		t.Fatalf("integrity audit = %#v err=%v", audit, err)
	}
	if repair, err := client.Repair(ctx, false); err != nil || repair["status"] == nil {
		t.Fatalf("repair = %#v err=%v", repair, err)
	}
	if gc, err := client.GC(ctx, map[string]any{"dry_run": true, "keep_snapshots": 2}); err != nil || gc["tenant_id"] == nil {
		t.Fatalf("gc = %#v err=%v", gc, err)
	}
	if compact, err := client.Compact(ctx); err != nil || compact.Version == 0 {
		t.Fatalf("compact = %#v err=%v", compact, err)
	}

	task, err := client.StartTask(ctx, "export_snapshot", nil)
	if err != nil || task.Type != "export_snapshot" {
		t.Fatalf("start task = %#v err=%v", task, err)
	}
	task = waitForSDKTask(t, ctx, client, task.ID)
	if got, err := client.GetTask(ctx, task.ID); err != nil || got.ID != task.ID {
		t.Fatalf("get task = %#v err=%v", got, err)
	}
	if tasks, err := client.ListTasks(ctx, TaskListOptions{Type: "export_snapshot", Limit: 10}); err != nil || !hasTask(tasks, task.ID) {
		t.Fatalf("list tasks = %#v err=%v", tasks, err)
	}

	ingested, err := client.Ingest(ctx, IngestRequest{
		Source: "agent", CollectorID: "go-sdk", BatchID: "go-batch-1", IdempotencyKey: "go-batch-1",
		Items: []IngestItem{{ExternalID: "host-go-2", Entity: &Entity{ID: "host:go-2", Kind: "host", Fields: Fields{"hostname": "go-host-2"}}}},
	})
	if err != nil || ingested.Failed != 0 {
		t.Fatalf("ingest = %#v err=%v", ingested, err)
	}
	if entity, err := client.GetEntity(ctx, "host:go-2", nil); err != nil || entity.Fields["hostname"] != "go-host-2" {
		t.Fatalf("get ingested entity = %#v err=%v", entity, err)
	}

	if disabled, err := client.DisableTenant(ctx, "go-sdk-e2e"); err != nil || disabled.Status != "disabled" {
		t.Fatalf("disable tenant = %#v err=%v", disabled, err)
	}
	_, err = client.Commit(ctx, Mutations{UpsertEntities: []Entity{{ID: "host:blocked", Kind: "host"}}}, nil)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 403 {
		t.Fatalf("disabled commit err = %#v", err)
	}
	if enabled, err := client.EnableTenant(ctx, "go-sdk-e2e"); err != nil || enabled.Status != "active" {
		t.Fatalf("enable tenant = %#v err=%v", enabled, err)
	}
	if clone, err := client.CloneTenant(ctx, "go-sdk-e2e", TenantCloneRequest{TargetTenantID: "go-sdk-e2e-clone"}); err != nil || clone.TenantID != "go-sdk-e2e-clone" {
		t.Fatalf("clone tenant = %#v err=%v", clone, err)
	}
	if entity, err := client.ForTenant("go-sdk-e2e-clone").GetEntity(ctx, "host:go", nil); err != nil || entity.ID != "host:go" {
		t.Fatalf("read clone entity = %#v err=%v", entity, err)
	}
	if deleted, err := client.DeleteTenant(ctx, "go-sdk-e2e-clone"); err != nil || deleted.Status != "deleted" {
		t.Fatalf("delete clone tenant = %#v err=%v", deleted, err)
	}
	if purged, err := client.PurgeTenant(ctx, "go-sdk-e2e-clone", false); err != nil || purged.TenantID != "go-sdk-e2e-clone" {
		t.Fatalf("purge clone tenant = %#v err=%v", purged, err)
	}
}

func TestPythonSDKCompleteFlowAgainstRealServer(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not found")
	}
	server := newSDKTestServer(t)
	defer server.Close()
	root := repoRootFromTest(t)
	cmd := exec.Command(python, "-m", "unittest", "sdk/python/tests/test_e2e.py")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PYTHONPATH="+filepath.Join(root, "sdk", "python"),
		"GRAPHDB_TEST_BASE_URL="+server.URL,
		"GRAPHDB_TEST_TENANT=python-sdk-e2e",
	)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python SDK e2e failed: %v\n%s", err, output)
	}
}

func newSDKTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "sdk-e2e")
	server := &httpapi.Server{Store: store, Mode: "all"}
	return httptest.NewServer(server.Handler())
}

func newSDKTestClient(t *testing.T, baseURL string, tenantID string) *Client {
	t.Helper()
	client, err := NewClient(baseURL, WithTenant(tenantID), WithHTTPClient(&http.Client{Timeout: 5 * time.Second}))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func seedSDKGraph(t *testing.T, ctx context.Context, client *Client) CommitResult {
	t.Helper()
	result, err := client.Commit(ctx, Mutations{
		UpsertCITypes: []CIType{
			{Name: "host", Fields: map[string]FieldSpec{"hostname": {Type: "string", Indexed: true}}},
			{Name: "service"},
		},
		UpsertRelationTypes: []RelationType{{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true}},
		UpsertEntities: []Entity{
			{ID: "host:go", Kind: "host", Source: "agent", SourcePriority: 100, Fields: Fields{"hostname": "go-host", "cpu": 16}},
			{ID: "service:go", Kind: "service", Source: "manual", SourcePriority: 1000, Fields: Fields{"name": "go-service"}},
		},
		UpsertEdges: []Edge{{
			ID: "edge:go-runs-on", Type: "runs_on", From: "service:go", To: "host:go",
			Source: "manual", SourcePriority: 1000, Fields: Fields{"status": "active"},
		}},
	}, &CommitOptions{IdempotencyKey: "go-sdk-e2e-commit"})
	if err != nil {
		t.Fatalf("commit seed: %v", err)
	}
	return result
}

func mustStream(t *testing.T, stream *Stream, err error) *Stream {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func countSDKStream(t *testing.T, stream *Stream) int {
	t.Helper()
	defer stream.Close()
	count := 0
	for {
		var item map[string]any
		if !stream.Next(&item) {
			break
		}
		count++
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}

func countSDKStreamItems(t *testing.T, stream *Stream, key string) int {
	t.Helper()
	defer stream.Close()
	count := 0
	for {
		var item map[string]any
		if !stream.Next(&item) {
			break
		}
		if _, ok := item[key]; ok {
			count++
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}

func waitForSDKTask(t *testing.T, ctx context.Context, client *Client, taskID string) Task {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		task, err := client.GetTask(ctx, taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if task.Status != "queued" && task.Status != "running" {
			if task.Status == "failed" {
				t.Fatalf("task failed: %#v", task)
			}
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not finish", taskID)
	return Task{}
}

func waitForSDKTaskQuietly(ctx context.Context, client *Client, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tasks, err := client.ListTasks(ctx, TaskListOptions{})
		if err == nil {
			running := false
			for _, task := range tasks {
				if task.Status == "queued" || task.Status == "running" {
					running = true
					break
				}
			}
			if !running {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func hasTenant(tenants []TenantInfo, tenantID string) bool {
	for _, tenant := range tenants {
		if tenant.TenantID == tenantID {
			return true
		}
	}
	return false
}

func hasTask(tasks []Task, taskID string) bool {
	for _, task := range tasks {
		if task.ID == taskID {
			return true
		}
	}
	return false
}

func repoRootFromTest(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}
