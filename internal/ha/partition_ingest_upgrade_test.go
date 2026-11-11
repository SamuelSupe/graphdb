package ha

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHAPartitionIngestUpgradePreservesSourceReceipts(t *testing.T) {
	a, _, catalog, router, call := newTestPartitionedGraph(t, true, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cfg := catalog.nodes[0].cfg.Raft
	directory := sharding.Shard{ID: "catalog", ClusterID: cfg.ClusterID, Peers: cfg.Peers}
	if err := router.Client.JSON(ctx, directory, http.MethodPost, "/cluster/action", sharding.Action{Operation: "assign", Tenant: "legacy-wal", Target: "a"}, nil); err != nil {
		t.Fatal(err)
	}
	for {
		var state sharding.Catalog
		if err := router.Client.JSON(ctx, directory, http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
			t.Fatal(err)
		}
		if state.Tenants["legacy-wal"].State == "active" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	call(http.MethodPost, "/v1/tenants", "", map[string]any{"tenant_id": "legacy-wal"}, http.StatusOK)
	call(http.MethodPost, "/v1/commits", "legacy-wal", partition.CommitRequest{Mutations: graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host"}}}}, http.StatusOK)
	request := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "historical", IdempotencyKey: "old-key", FullSync: true,
		Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "old-host", Kind: "host"}}}}
	call(http.MethodPost, "/v1/ingest/batches", "big", request, http.StatusNotImplemented)
	response := call(http.MethodPost, "/v1/ingest/batches", "legacy-wal", request, http.StatusAccepted)
	statusURL := response.Header().Get("Location")
	if statusURL == "" {
		t.Fatal("acceptance has no status URL")
	}
	stale := int64(0)
	rejected := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "old-rejected", ExpectedVersion: &stale, Items: request.Items}
	rejectedURL := call(http.MethodPost, "/v1/ingest/batches", "legacy-wal", rejected, http.StatusAccepted).Header().Get("Location")
	if err := a.nodes[a.leader(-1)].cluster.flushPendingTenant(ctx, "legacy-wal"); err != nil {
		t.Fatal(err)
	}
	readStatus := func() storage.IngestBatchStatus {
		t.Helper()
		response := call(http.MethodGet, statusURL, "legacy-wal", nil, http.StatusOK)
		var status storage.IngestBatchStatus
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil || status.State != "committed" || status.Result == nil || status.Result.Version != 2 {
			t.Fatalf("historical status: %s (%v)", response.Body.String(), err)
		}
		return status
	}
	original := readStatus()
	// A replicated direct-mode write has a storage receipt but no WAL LSN.
	direct := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "old-direct", IdempotencyKey: "old-direct-key",
		Items: []storage.IngestItem{{Entity: &graph.Entity{ID: "old-direct-host", Kind: "host"}}}}
	cmd, err := newCommand("http")
	if err != nil {
		t.Fatal(err)
	}
	cmd.Tenant, cmd.RouteEpoch, cmd.Method, cmd.URI = "legacy-wal", 1, http.MethodPost, "/v1/ingest/batches"
	cmd.Header = make(http.Header)
	cmd.Header.Set("X-Tenant-ID", cmd.Tenant)
	cmd.Header.Set("Content-Type", "application/json")
	cmd.Body, err = json.Marshal(direct)
	if err != nil {
		t.Fatal(err)
	}
	data, err := a.nodes[a.leader(-1)].cluster.propose(ctx, cmd)
	var directResponse httpResult
	if err != nil || json.Unmarshal(data, &directResponse) != nil || directResponse.Status != http.StatusOK {
		t.Fatalf("source direct ingest: %s (%v)", data, err)
	}
	call(http.MethodPost, "/v1/indexes/rebuild", "legacy-wal", nil, http.StatusOK)
	call(http.MethodPost, "/v1/cluster/graphs", "", map[string]any{"tenant_id": "legacy-wal", "data_groups": []string{"a", "b"}}, http.StatusAccepted)
	readGraph := func() partition.Graph {
		t.Helper()
		response := call(http.MethodGet, "/v1/cluster/graphs/legacy-wal", "", nil, http.StatusOK)
		var g partition.Graph
		if err := json.Unmarshal(response.Body.Bytes(), &g); err != nil {
			t.Fatal(err)
		}
		return g
	}
	var converted partition.Graph
	for ctx.Err() == nil {
		converted = readGraph()
		if converted.Format == partition.FormatVersion && converted.State == "active" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if converted.Version != 3 || converted.EntityCount != 2 {
		t.Fatalf("unexpected converted graph version %d, entities %d", converted.Version, converted.EntityCount)
	}
	if status := readStatus(); status.AcceptedLSN != original.AcceptedLSN || !status.AcceptedAt.Equal(original.AcceptedAt) {
		t.Fatal("conversion changed the historical acceptance")
	}
	for i := range a.nodes {
		a.stop(i)
	}
	for i := range 2 {
		a.nodes[i].cfg.Raft.Protocol = partition.IngestProtocol
		a.start(i)
	}
	a.leader(2)
	a.start(2)
	leader := a.leader(2)
	alias := request
	alias.BatchID = "changed-batch-id"
	encoded, err := json.Marshal(alias)
	if err != nil {
		t.Fatal(err)
	}
	probe := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", bytes.NewReader(encoded)).WithContext(ctx)
	probe.Header.Set("Content-Type", "application/json")
	probe.Header.Set("X-Tenant-ID", "legacy-wal")
	probe.Header.Set(sharding.EpochHeader, "1")
	response = httptest.NewRecorder()
	a.nodes[leader].handler.ServeHTTP(response, probe)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "configure every replica") {
		t.Fatalf("expected a protocol activation error: %d %s", response.Code, response.Body.String())
	}
	a.stop(2)
	a.nodes[2].cfg.Raft.Protocol = partition.IngestProtocol
	a.start(2)
	changed := alias
	changed.Cursor = "different payload"
	call(http.MethodPost, "/v1/ingest/batches", "legacy-wal", changed, http.StatusConflict)
	for _, replay := range []storage.IngestRequest{alias, request} {
		response = call(http.MethodPost, "/v1/ingest/batches", "legacy-wal", replay, http.StatusAccepted)
		if response.Header().Get("Location") != statusURL {
			t.Fatalf("retry changed status URL: %s", response.Body.String())
		}
	}
	call(http.MethodPost, "/v1/ingest/batches", "legacy-wal", rejected, http.StatusAccepted)
	const directURL = "/v1/ingest/batches/agent/collector/old-direct"
	call(http.MethodGet, directURL, "legacy-wal", nil, http.StatusOK)
	direct.BatchID = "old-direct-retry"
	if replay := call(http.MethodPost, "/v1/ingest/batches", "legacy-wal", direct, http.StatusAccepted); replay.Header().Get("Location") != directURL {
		t.Fatalf("direct replay changed the canonical URL: %s", replay.Body.String())
	}
	a.stop(leader)
	a.leader(leader)
	status := readStatus()
	if status.AcceptedLSN != original.AcceptedLSN || !status.AcceptedAt.Equal(original.AcceptedAt) {
		t.Fatal("retry or failover changed the historical acceptance")
	}
	response = call(http.MethodGet, rejectedURL, "legacy-wal", nil, http.StatusOK)
	var failed storage.IngestBatchStatus
	if err := json.Unmarshal(response.Body.Bytes(), &failed); err != nil || failed.State != "failed" || failed.Result == nil || failed.Result.ErrorCode != storage.IngestErrorVersionConflict {
		t.Fatalf("rejected source batch lost its original result: %s (%v)", response.Body.String(), err)
	}
	if g := readGraph(); g.Version != converted.Version || g.DataHash != converted.DataHash {
		t.Fatalf("replay published new graph data at version %d", g.Version)
	}
}
