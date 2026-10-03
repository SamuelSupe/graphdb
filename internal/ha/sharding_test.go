package ha

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHAShardRejectsInvalidGraphTransferWithoutStoppingReplicas(t *testing.T) {
	group := newTestClusterRole(t, true, "b", false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := group.leader(-1)
	action := func(input sharding.Action, want int) {
		t.Helper()
		result, err := group.nodes[leader].cluster.shardAction(ctx, input)
		if err != nil {
			t.Fatalf("%s stopped shard application: %v", input.Operation, err)
		}
		var outcome httpResult
		if json.Unmarshal(result, &outcome) != nil || outcome.Status != want {
			t.Fatalf("%s status want %d: %s", input.Operation, want, result)
		}
	}
	for _, failure := range []string{"missing-commit", "malformed-manifest", "malformed-index"} {
		tenant, move := "tenant-"+failure, "move-"+failure
		objects := storage.NewMemoryStore()
		source := storage.NewTenantStore(objects, "graphdb")
		manifest, err := source.Commit(ctx, tenant, graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}}}, storage.CommitOptions{})
		if err != nil {
			t.Fatal(err)
		}
		listed, err := objects.List(ctx, "graphdb/tenants/"+tenant+"/")
		if err != nil {
			t.Fatal(err)
		}
		transfer := sharding.Transfer{Tenant: tenant, MoveID: move}
		for _, item := range listed {
			if failure == "missing-commit" && item.Key == manifest.CommitKeys[0] {
				continue
			}
			data, err := objects.Get(ctx, item.Key)
			if err != nil {
				t.Fatal(err)
			}
			if failure == "malformed-manifest" && strings.HasSuffix(item.Key, "/manifest.parquet") {
				data = []byte("broken manifest")
			}
			transfer.Objects = append(transfer.Objects, sharding.Object{Key: item.Key, Data: data})
		}
		if failure == "malformed-index" {
			transfer.Objects = append(transfer.Objects, sharding.Object{Key: "graphdb/tenants/" + tenant + "/indexes/catalog.parquet", Data: []byte("broken index catalog")})
		}
		app := group.nodes[leader].cluster.App
		transfer.Objects = append(transfer.Objects, sharding.Object{Key: app.generationKey(tenant), Data: []byte("3")}, sharding.Object{Key: app.purgeKey(tenant)})
		encoded, err := json.Marshal(transfer)
		if err != nil {
			t.Fatal(err)
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(encoded))
		input := sharding.Action{Tenant: tenant, MoveID: move, Epoch: 1, Digest: digest, Parts: 1, Bytes: int64(len(encoded))}
		input.Operation = "reserve"
		action(input, http.StatusOK)
		input.Operation, input.Data = "stage", encoded
		action(input, http.StatusOK)
		input.Operation, input.Data = "install", nil
		action(input, http.StatusConflict)
		checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		for i, replica := range group.nodes {
			group.waitApplied(i, checkpoint.Index)
			replica.cluster.App.mu.RLock()
			owner, ownerErr := replica.cluster.App.ownership(ctx, tenant)
			tenantObjects, listErr := replica.files.List(ctx, "graphdb/tenants/"+tenant+"/")
			_, generationErr := replica.files.Get(ctx, app.generationKey(tenant))
			_, purgeErr := replica.files.Get(ctx, app.purgeKey(tenant))
			replica.cluster.App.mu.RUnlock()
			if ownerErr != nil || listErr != nil || owner.State != "importing" || len(tenantObjects) != 0 || !errors.Is(generationErr, storage.ErrNotFound) || !errors.Is(purgeErr, storage.ErrNotFound) {
				t.Fatalf("replica %d published invalid input: owner=%+v objects=%d errors=%v/%v/%v/%v", i, owner, len(tenantObjects), ownerErr, listErr, generationErr, purgeErr)
			}
		}
	}
	action(sharding.Action{Operation: "own", Tenant: "tenant-a", Epoch: 1}, http.StatusOK)
	for _, input := range []struct{ path, body string }{
		{"/v1/tenants", `{"tenant_id":"tenant-a"}`},
		{"/v1/commits", `{"mutations":{"upsert_entities":[{"id":"host:after-rejection","kind":"host"}]}}`},
	} {
		request := httptest.NewRequest(http.MethodPost, input.path, strings.NewReader(input.body)).WithContext(ctx)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Tenant-ID", "tenant-a")
		request.Header.Set(sharding.EpochHeader, "1")
		response := httptest.NewRecorder()
		group.nodes[leader].handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("write after rejection: %d %s", response.Code, response.Body.String())
		}
	}
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i, replica := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
		replica.cluster.App.mu.RLock()
		g, _, err := replica.store.Load(ctx, "tenant-a")
		replica.cluster.App.mu.RUnlock()
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := g.GetEntity("host:after-rejection"); !ok {
			t.Fatalf("replica %d cannot apply a write after rejecting input", i)
		}
	}
}

func TestHAShardExportRejectsMissingPublishedCommit(t *testing.T) {
	ctx := context.Background()
	files := storage.NewFileStore(t.TempDir())
	store := storage.NewTenantStore(files, "graphdb")
	manifest, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:source", Kind: "host"}}}, storage.CommitOptions{})
	if err != nil {
		t.Fatal(err)
	}
	app := &Application{Files: files, Store: store, MaxSnapshotBytes: 8 << 20}
	action := sharding.Action{Tenant: "tenant-a", MoveID: "move-corrupt", Epoch: 1}
	owner, _ := json.Marshal(sharding.Ownership{State: "frozen", Epoch: 1, MoveID: action.MoveID})
	if err := files.Put(ctx, app.ownershipKey(action.Tenant), owner); err != nil {
		t.Fatal(err)
	}
	if err := files.Delete(ctx, manifest.CommitKeys[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := app.tenantTransfer(ctx, action.Tenant, action.MoveID, action.Epoch); err == nil {
		t.Error("legacy export accepted a missing published commit")
	}
	cluster := &Cluster{App: app}
	exported, err := cluster.buildTenantExport(ctx, action)
	if exported != nil {
		exported.close()
	}
	if err == nil {
		t.Fatal("chunked export accepted a missing published commit")
	}
}

func TestHAShardDelayedFlushAfterRetirement(t *testing.T) {
	group := newTestClusterRole(t, true, "a", false)
	leader := group.leader(-1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	propose := func(cmd command) []byte {
		t.Helper()
		identity, err := newCommand(cmd.Kind)
		if err != nil {
			t.Fatal(err)
		}
		cmd.ID, cmd.At = identity.ID, identity.At
		data, err := group.nodes[leader].cluster.propose(ctx, cmd)
		if err != nil {
			t.Fatalf("%s command stopped the shard application: %v", cmd.Kind, err)
		}
		return data
	}
	ownership := func(operation, tenant, move string, epoch uint64) {
		t.Helper()
		body, _ := json.Marshal(sharding.Action{Operation: operation, Tenant: tenant, Epoch: epoch, MoveID: move})
		data := propose(command{Kind: "sharding", Body: body})
		var result httpResult
		if json.Unmarshal(data, &result) != nil || result.Status != http.StatusOK {
			t.Fatalf("%s: %s", operation, data)
		}
	}
	request := func(method, uri, body string, status int) {
		t.Helper()
		r := httptest.NewRequest(method, uri, strings.NewReader(body)).WithContext(ctx)
		r.Header.Set("X-Tenant-ID", "tenant-a")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set(sharding.EpochHeader, "1")
		w := httptest.NewRecorder()
		group.nodes[leader].handler.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s %s: %d: %s", method, uri, w.Code, w.Body.String())
		}
	}
	ownership("own", "tenant-a", "", 1)
	request("POST", "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	request("POST", "/v1/ingest/batches", `{"source":"agent","collector_id":"collector","batch_id":"delayed","items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host"}}]}`, http.StatusAccepted)
	cluster := group.nodes[leader].cluster
	queue, err := cluster.App.pendingSnapshot(ctx)
	if err != nil || len(queue) != 1 {
		t.Fatalf("accepted queue: %d, %v", len(queue), err)
	}
	delayed := command{Kind: "flush", Tenant: "tenant-a", RouteEpoch: 1}
	for key := range queue {
		delayed.IDs = append(delayed.IDs, key)
	}
	if err := cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	ownership("freeze", "tenant-a", "move-delayed", 1)
	ownership("thaw", "tenant-a", "move-delayed", 2)
	data := propose(delayed)
	var outcome httpResult
	if json.Unmarshal(data, &outcome) != nil || outcome.Status != http.StatusConflict {
		t.Fatalf("previous ownership epoch flush was not rejected: %s", data)
	}
	ownership("freeze", "tenant-a", "move-delayed", 2)
	ownership("retire", "tenant-a", "move-delayed", 2)
	data = propose(delayed)
	if json.Unmarshal(data, &outcome) != nil || outcome.Status != http.StatusConflict {
		t.Fatalf("retired tenant flush was not rejected: %s", data)
	}
	delayed.RouteEpoch = 0
	data = propose(delayed)
	if json.Unmarshal(data, &outcome) != nil || outcome.Status != http.StatusConflict {
		t.Fatalf("legacy flush recreated a retired tenant: %s", data)
	}
	checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	for i, replica := range group.nodes {
		group.waitApplied(i, checkpoint.Index)
		if objects, err := replica.files.List(ctx, cluster.App.ingestPrefix()); err != nil || len(objects) != 0 {
			t.Fatalf("replica %d recreated retired ingestion: %d, %v", i, len(objects), err)
		}
	}
	group.stop(leader)
	leader = group.leader(leader)
	ownership("own", "tenant-b", "", 1)
	for i := range group.nodes {
		if i == leader {
			continue
		}
		if group.nodes[i].cluster != nil {
			checkpoint, err := group.nodes[leader].files.ReplicationCheckpoint()
			if err != nil {
				t.Fatal(err)
			}
			group.waitApplied(i, checkpoint.Index)
			owner, err := group.nodes[i].cluster.App.ownership(ctx, "tenant-b")
			if err != nil || owner.State != "active" {
				t.Fatalf("shard did not continue after failover: %+v, %v", owner, err)
			}
		}
	}
}

func TestHAShardExpansionMigrationAndCancellation(t *testing.T) {
	catalog := newTestClusterRole(t, false, "", true)
	a := newTestClusterRole(t, true, "a", false)
	b := newTestClusterRole(t, true, "b", false)
	// This case exercises a large resumable transfer. Generating a complete
	// compatibility snapshot every five chunks under race instrumentation
	// overwhelms heartbeat timing; snapshot faults have dedicated cases.
	for _, group := range []*testCluster{catalog, a, b} {
		for i := range group.nodes {
			group.stop(i)
		}
		for i, replica := range group.nodes {
			replica.cfg.Raft.SnapshotEntries = 100
			replica.cfg.Raft.Tick = 500 * time.Millisecond
			group.start(i)
		}
	}
	definition := func(group *testCluster, id string) sharding.Shard {
		cfg := group.nodes[0].cfg.Raft
		return sharding.Shard{ID: id, ClusterID: cfg.ClusterID, Peers: cfg.Peers}
	}
	catalog.leader(-1)
	a.leader(-1)
	b.leader(-1)
	token := catalog.nodes[0].cfg.Raft.Token
	client := sharding.NewClient(token)
	defer client.HTTP.CloseIdleConnections()
	router := sharding.NewRouter(definition(catalog, "catalog"), token)
	defer router.Client.HTTP.CloseIdleConnections()
	var readGeneration string
	request := func(method, uri, tenant string, body any, status int) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		safeRetry := method == http.MethodGet
		if fields, ok := body.(map[string]any); ok && (uri == "/v1/commits" || uri == "/v1/ingest/batches") {
			safeRetry = fields["idempotency_key"] != nil
		}
		var w *httptest.ResponseRecorder
		for {
			r := httptest.NewRequest(method, uri, bytes.NewReader(data)).WithContext(ctx)
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Authorization", "Bearer "+token)
			if tenant != "" {
				r.Header.Set("X-Tenant-ID", tenant)
			}
			if uri == "/v1/query" && tenant == "tenant-a" {
				r.Header.Set("X-GraphDB-Read-Generation", readGeneration)
			}
			w = httptest.NewRecorder()
			router.ServeHTTP(w, r)
			// Keep the original identity when a leader change leaves the
			// outcome unknown; mutations without one are never replayed.
			if !safeRetry || (status != http.StatusOK && status != http.StatusAccepted) || w.Code != http.StatusServiceUnavailable || ctx.Err() != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if w.Code != status {
			t.Fatalf("%s %s: %d, want %d: %s", method, uri, w.Code, status, w.Body.String())
		}
		return w
	}
	state := func() sharding.Catalog {
		t.Helper()
		var result sharding.Catalog
		if err := client.JSON(context.Background(), definition(catalog, "catalog"), "GET", "/cluster/catalog", nil, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	waitPlacement := func(tenant string, match func(sharding.Placement) bool) sharding.Placement {
		t.Helper()
		// The incompressible transfer spans several disk-backed passes. Race
		// instrumentation also encodes replicated entries and snapshots, so
		// allow the coordinator's existing two-minute operation budget.
		deadline := time.Now().Add(2 * time.Minute)
		var placement sharding.Placement
		for time.Now().Before(deadline) {
			placement = state().Tenants[tenant]
			if match(placement) {
				return placement
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("tenant placement did not progress: %+v, move: %+v", placement, placement.Move)
		return placement
	}
	request("POST", "/v1/cluster/shards", "", definition(a, "a"), http.StatusAccepted)
	request("POST", "/v1/tenants", "", map[string]any{"tenant_id": "tenant-a"}, http.StatusOK)
	body := map[string]any{"idempotency_key": "before-move", "mutations": map[string]any{"upsert_entities": []any{map[string]any{"id": "host:1", "kind": "host", "fields": map[string]any{"name": "original"}}}}}
	committed := request("POST", "/v1/commits", "tenant-a", body, http.StatusOK)
	readGeneration = committed.Header().Get("X-GraphDB-Tenant-Generation")
	if readGeneration == "" {
		t.Fatal("commit did not return a read generation")
	}
	request("POST", "/v1/cluster/shards", "", definition(b, "b"), http.StatusAccepted)
	if placement := state().Tenants["tenant-a"]; placement.Shard != "a" || placement.Epoch != 1 {
		t.Fatalf("adding a shard remapped an existing tenant: %+v", placement)
	}
	request("POST", "/v1/tenants", "", map[string]any{"tenant_id": "tenant-b"}, http.StatusOK)
	if placement := state().Tenants["tenant-b"]; placement.Shard != "b" {
		t.Fatalf("new shard did not receive new tenants: %+v", placement)
	}
	request("POST", "/v1/commits", "tenant-b", body, http.StatusOK)
	request("POST", "/v1/tenants/tenant-b/clone", "", map[string]any{"target_tenant_id": "tenant-clone"}, http.StatusAccepted)
	request("GET", "/v1/entities/host:1", "tenant-clone", nil, http.StatusOK)
	if placement := state().Tenants["tenant-clone"]; placement.Shard != "b" {
		t.Fatal("clone escaped its owned shard")
	}

	// Migration must drain accepted WAL and preserve both its data and identity.
	payload := make([]byte, 2<<20)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	batch := map[string]any{"source": "agent", "collector_id": "sharding", "batch_id": "before-move", "idempotency_key": "accepted-before-move", "items": []any{map[string]any{"external_id": "host:2", "entity": map[string]any{"id": "host:2", "kind": "host", "fields": map[string]any{"payload": base64.StdEncoding.EncodeToString(payload)}}}}}
	accepted := request("POST", "/v1/ingest/batches", "tenant-a", batch, http.StatusAccepted)
	var acceptance map[string]any
	if err := json.Unmarshal(accepted.Body.Bytes(), &acceptance); err != nil {
		t.Fatal(err)
	}
	for _, replica := range b.nodes {
		replica.blocked.Store(true)
	}
	request("POST", "/v1/cluster/moves", "", map[string]any{"tenant_id": "tenant-a", "target": "b"}, http.StatusAccepted)
	moving := waitPlacement("tenant-a", func(p sharding.Placement) bool { return p.Move != nil && p.Move.Error != "" })
	export := sharding.Action{Tenant: "tenant-a", MoveID: moving.Move.ID, Epoch: moving.Epoch}
	var info sharding.TransferInfo
	exportCtx, exportCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer exportCancel()
	for {
		err := client.JSON(exportCtx, definition(a, "a"), "POST", "/cluster/export/manifest", export, &info)
		if err == nil {
			break
		}
		var response *sharding.HTTPError
		if !errors.As(err, &response) || response.Status != http.StatusConflict || exportCtx.Err() != nil {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	sourceLeader := a.leader(-1)
	sourceCluster := a.nodes[sourceLeader].cluster
	sourceCluster.App.mu.RLock()
	legacy, err := sourceCluster.App.tenantTransfer(context.Background(), export.Tenant, export.MoveID, export.Epoch)
	sourceCluster.App.mu.RUnlock()
	if err != nil || info.Bytes != int64(len(legacy)) || info.Digest != fmt.Sprintf("%x", sha256.Sum256(legacy)) {
		t.Fatalf("streamed export changed the legacy payload: %+v, %v", info, err)
	}
	if info.Parts <= 8 {
		t.Fatalf("migration fixture did not exercise multiple transfer passes: %+v", info)
	}
	a.stop(sourceLeader)
	a.leader(sourceLeader)
	oldCatalogLeader := catalog.leader(-1)
	catalog.stop(oldCatalogLeader)
	catalog.leader(oldCatalogLeader)
	for _, replica := range b.nodes {
		replica.loseInstallResponse.Store(true)
		replica.loseStageResponse.Store(true)
		replica.blocked.Store(false)
	}
	placement := waitPlacement("tenant-a", func(p sharding.Placement) bool { return p.State == "active" && p.Shard == "b" && p.Move == nil })
	a.start(sourceLeader)
	if placement.Epoch != 2 {
		t.Fatalf("migration did not advance ownership epoch: %+v", placement)
	}
	var lostResponse bool
	for _, replica := range b.nodes {
		lostResponse = lostResponse || !replica.loseInstallResponse.Load()
		replica.loseInstallResponse.Store(false)
	}
	if !lostResponse {
		t.Fatal("migration did not exercise recovery from a lost install response")
	}
	var stageRequests int64
	for _, replica := range b.nodes {
		stageRequests += replica.stageRequests.Load()
		replica.loseStageResponse.Store(false)
	}
	if stageRequests != int64(info.Parts) {
		t.Fatalf("lost stage response caused retransmission of a committed chunk: %d", stageRequests)
	}
	if chunks, err := b.nodes[b.leader(-1)].files.List(context.Background(), "graphdb/control/sharding/transfers/tenant-a/"); err != nil || len(chunks) != 0 {
		t.Fatalf("install replay retained staging data: %d, %v", len(chunks), err)
	}
	catalog.start(oldCatalogLeader)
	replayed := request("POST", "/v1/commits", "tenant-a", body, http.StatusOK)
	var result map[string]any
	if json.Unmarshal(replayed.Body.Bytes(), &result) != nil || result["idempotent_replay"] != true || result["version"] != float64(1) {
		t.Fatalf("migration lost commit identity: %s", replayed.Body.String())
	}
	status := request("GET", "/v1/ingest/batches/agent/sharding/before-move", "tenant-a", nil, http.StatusOK)
	var batchStatus map[string]any
	if json.Unmarshal(status.Body.Bytes(), &batchStatus) != nil || batchStatus["accepted_lsn"] != acceptance["accepted_lsn"] || batchStatus["state"] != "committed" {
		t.Fatalf("migration lost accepted WAL state: %s", status.Body.String())
	}
	request("GET", "/v1/entities/host:2", "tenant-a", nil, http.StatusOK)
	request("GET", "/v1/ingest/writers/raft-a/batches/agent/sharding/before-move", "tenant-a", nil, http.StatusOK)
	query := request("POST", "/v1/query", "tenant-a", map[string]any{"op": "match", "kind": "host", "min_version": 2, "limit": 10}, http.StatusOK)
	if query.Header().Get("X-GraphDB-Shard-ID") != "b" || query.Header().Get(sharding.EpochHeader) != "2" {
		t.Fatalf("query was not routed to its new owner: %v", query.Header())
	}
	aleader := a.leader(-1)
	if objects, err := a.nodes[aleader].files.List(context.Background(), "graphdb/tenants/tenant-a/"); err != nil || len(objects) != 0 {
		t.Fatalf("old shard retained migrated tenant data: %d, %v", len(objects), err)
	}
	stale := httptest.NewRequest("POST", "/v1/commits", bytes.NewBufferString(`{"mutations":{}}`))
	stale.Header.Set("X-Tenant-ID", "tenant-a")
	stale.Header.Set(sharding.EpochHeader, "1")
	w := httptest.NewRecorder()
	a.nodes[aleader].handler.ServeHTTP(w, stale)
	if w.Code != http.StatusConflict {
		t.Fatalf("old owner accepted a stale request: %d %s", w.Code, w.Body.String())
	}
	for _, replica := range b.nodes {
		replica.legacyExportOnly.Store(true)
	}
	request("POST", "/v1/cluster/moves", "", map[string]any{"tenant_id": "tenant-a", "target": "a"}, http.StatusAccepted)
	waitPlacement("tenant-a", func(p sharding.Placement) bool {
		return p.State == "active" && p.Shard == "a" && p.Epoch == 3 && p.Move == nil
	})
	for _, replica := range b.nodes {
		replica.legacyExportOnly.Store(false)
	}
	request("GET", "/v1/entities/host:2", "tenant-a", nil, http.StatusOK)
	request("POST", "/v1/query", "tenant-a", map[string]any{"op": "match", "kind": "host", "min_version": 2, "limit": 10}, http.StatusOK)
	request("POST", "/v1/commits", "tenant-a", map[string]any{"mutations": map[string]any{"upsert_entities": []any{map[string]any{"id": "host:after-return", "kind": "host"}}}}, http.StatusOK)
	status = request("GET", "/v1/ingest/batches/agent/sharding/before-move", "tenant-a", nil, http.StatusOK)
	if json.Unmarshal(status.Body.Bytes(), &batchStatus) != nil || batchStatus["accepted_lsn"] != acceptance["accepted_lsn"] {
		t.Fatalf("return migration lost accepted WAL identity: %s", status.Body.String())
	}

	// Losing a data shard quorum does not stop independent shards or catalog.
	for _, replica := range a.nodes {
		replica.blocked.Store(true)
	}
	request("POST", "/v1/commits", "tenant-b", map[string]any{"mutations": map[string]any{"upsert_entities": []any{map[string]any{"id": "host:independent", "kind": "host"}}}}, http.StatusOK)
	for _, replica := range a.nodes {
		replica.blocked.Store(false)
	}
	a.leader(-1)
	request("POST", "/v1/cluster/placements", "", map[string]any{"tenant_id": "cancel-tenant", "target": "a"}, http.StatusAccepted)
	waitPlacement("cancel-tenant", func(p sharding.Placement) bool { return p.State == "active" })
	request("POST", "/v1/tenants", "", map[string]any{"tenant_id": "cancel-tenant"}, http.StatusOK)
	request("POST", "/v1/commits", "cancel-tenant", body, http.StatusOK)
	for _, replica := range b.nodes {
		replica.blocked.Store(true)
	}
	request("POST", "/v1/cluster/moves", "", map[string]any{"tenant_id": "cancel-tenant", "target": "b"}, http.StatusAccepted)
	cancelled := waitPlacement("cancel-tenant", func(p sharding.Placement) bool { return p.Move != nil && p.Move.Error != "" })
	request("POST", "/v1/cluster/moves/cancel-tenant/cancel", "", nil, http.StatusAccepted)
	waitPlacement("cancel-tenant", func(p sharding.Placement) bool { return p.State == "active" && p.Epoch == 2 })
	request("GET", "/v1/entities/host:1", "cancel-tenant", nil, http.StatusOK)
	for _, replica := range b.nodes {
		replica.blocked.Store(false)
	}
	waitPlacement("cancel-tenant", func(p sharding.Placement) bool { return p.Move == nil })
	for _, obsolete := range []struct {
		group *testCluster
		op    string
		epoch uint64
	}{{a, "freeze", 1}, {b, "reserve", 2}, {b, "activate", 2}, {catalog, "cutover", 2}} {
		err := client.JSON(context.Background(), definition(obsolete.group, obsolete.group.nodes[0].cfg.Raft.ClusterID), "POST", "/cluster/action", sharding.Action{Operation: obsolete.op, Tenant: "cancel-tenant", Epoch: obsolete.epoch, MoveID: cancelled.Move.ID}, nil)
		if response, ok := err.(*sharding.HTTPError); !ok || response.Status != http.StatusConflict {
			t.Fatalf("cancelled coordinator %s was not fenced: %v", obsolete.op, err)
		}
	}
	request("GET", "/v1/tenants", "", nil, http.StatusOK)
	groupNode := 0
	a.stop(groupNode)
	replica := a.nodes[groupNode]
	files, err := storage.OpenFileStore(replica.cfg.DataDir)
	if err != nil {
		t.Fatal(err)
	}
	misconfigured := replica.cfg
	misconfigured.Raft.ShardID = ""
	store := storage.NewTenantStoreWithOptions(files, "graphdb", storage.TenantStoreOptions{InstanceID: misconfigured.InstanceID})
	cluster := New(misconfigured, store, files)
	if err := cluster.Start(context.Background(), misconfigured.Raft); err == nil {
		cluster.Close()
		t.Fatal("a shard directory was opened without its ownership role")
	}
	files.Close()
	a.start(groupNode)
	a.leader(-1)
	request("GET", "/v1/entities/host:after-return", "tenant-a", nil, http.StatusOK)
	request("POST", "/v1/cluster/shards/a/drain", "", nil, http.StatusAccepted)
	request("POST", "/v1/cluster/shards/a/unregister", "", nil, http.StatusConflict)
	request("POST", "/v1/cluster/shards/b/drain", "", nil, http.StatusAccepted)
	request("POST", "/v1/cluster/placements", "", map[string]any{"tenant_id": "cannot-assign", "target": "b"}, http.StatusConflict)
	request("POST", "/v1/cluster/shards/b/resume", "", nil, http.StatusAccepted)
	for _, tenant := range []string{"tenant-a", "cancel-tenant"} {
		request("POST", "/v1/cluster/moves", "", map[string]any{"tenant_id": tenant, "target": "b"}, http.StatusAccepted)
		waitPlacement(tenant, func(p sharding.Placement) bool { return p.State == "active" && p.Shard == "b" && p.Move == nil })
	}
	request("POST", "/v1/cluster/shards/a/unregister", "", nil, http.StatusAccepted)
	for _, replica := range a.nodes {
		replica.blocked.Store(true)
	}
	request("GET", "/v1/tenants", "", nil, http.StatusOK)
	request("GET", "/v1/entities/host:after-return", "tenant-a", nil, http.StatusOK)
	for _, replica := range catalog.nodes {
		replica.blocked.Store(true)
	}
	request("GET", "/v1/entities/host:after-return", "tenant-a", nil, http.StatusOK)
	time.Sleep(6 * time.Second)
	request("GET", "/v1/entities/host:after-return", "tenant-a", nil, http.StatusOK)
	request("GET", "/v1/readiness", "", nil, http.StatusOK)
	request("POST", "/v1/commits", "tenant-a", map[string]any{"idempotency_key": "catalog-outage", "mutations": map[string]any{"upsert_entities": []any{map[string]any{"id": "host:catalog-outage", "kind": "host"}}}}, http.StatusOK)
	request("GET", "/v1/entities/host:catalog-outage", "tenant-a", nil, http.StatusOK)
	request("GET", "/v1/entities/missing", "unknown-tenant", nil, http.StatusServiceUnavailable)
	for _, replica := range catalog.nodes {
		replica.blocked.Store(false)
	}
	catalog.leader(-1)
	request("GET", "/v1/entities/host:after-return", "tenant-a", nil, http.StatusOK)
}
