package ha

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHAShardWALReceiptExpiryAfterTenantMove(t *testing.T) {
	const tenant = "tenant-a"
	const retention = 15 * time.Second
	catalog := newTestClusterRole(t, false, "", true)
	source := newTestClusterRole(t, true, "a", false)
	target := newTestClusterRole(t, true, "b", false)
	for _, group := range []*testCluster{source, target} {
		for i := range group.nodes {
			group.stop(i)
		}
		for i, node := range group.nodes {
			node.cfg.Raft.Protocol = ingestQueueCheckpointProtocol
			node.cfg.Raft.SnapshotEntries = 100
			node.cfg.IngestFlushInterval = time.Hour
			node.cfg.HAIngestReceiptRetention = retention
			group.start(i)
		}
	}
	definition := func(group *testCluster, id string) sharding.Shard {
		cfg := group.nodes[0].cfg.Raft
		return sharding.Shard{ID: id, ClusterID: cfg.ClusterID, Peers: cfg.Peers}
	}
	token := catalog.nodes[0].cfg.Raft.Token
	router := sharding.NewRouter(definition(catalog, "catalog"), token)
	t.Cleanup(router.Client.HTTP.CloseIdleConnections)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	call := func(method, path string, body any) *httptest.ResponseRecorder {
		t.Helper()
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(method, path, bytes.NewReader(data)).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Tenant-ID", tenant)
		reply := httptest.NewRecorder()
		router.ServeHTTP(reply, req)
		return reply
	}
	request := func(method, path string, body any, want int) *httptest.ResponseRecorder {
		t.Helper()
		reply := call(method, path, body)
		if reply.Code != want {
			t.Fatalf("%s %s: got %d, want %d: %s", method, path, reply.Code, want, reply.Body.String())
		}
		return reply
	}
	wait := func(condition func() bool, message string) {
		t.Helper()
		for !condition() {
			select {
			case <-ctx.Done():
				t.Fatal(message)
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
	catalog.leader(-1)
	old := source.nodes[source.leader(-1)]
	target.leader(-1)
	request(http.MethodPost, "/v1/cluster/shards", definition(source, "a"), http.StatusAccepted)
	request(http.MethodPost, "/v1/tenants", map[string]string{"tenant_id": tenant}, http.StatusOK)
	request(http.MethodPost, "/v1/cluster/shards", definition(target, "b"), http.StatusAccepted)
	pad := func(node *testReplica, epoch, index uint64) {
		t.Helper()
		for {
			checkpoint, err := node.files.ReplicationCheckpoint()
			if err != nil || checkpoint.Index > index {
				t.Fatalf("cannot reach test Raft index %d from %d: %v", index, checkpoint.Index, err)
			}
			if checkpoint.Index == index {
				return
			}
			cmd, err := newCommand("flush")
			if err != nil {
				t.Fatal(err)
			}
			cmd.Tenant, cmd.RouteEpoch = tenant, epoch
			if _, err := node.cluster.propose(ctx, cmd); err != nil {
				t.Fatal(err)
			}
		}
	}
	pad(old, 1, 79)
	batch := func(batchID, id string) map[string]any {
		return map[string]any{"source": "agent", "collector_id": "collector", "batch_id": batchID, "idempotency_key": "same-key",
			"items": []any{map[string]any{"entity": map[string]string{"id": id, "kind": "host"}}}}
	}
	accept := func(batchID, id string) (string, uint64) {
		t.Helper()
		reply := request(http.MethodPost, "/v1/ingest/batches", batch(batchID, id), http.StatusAccepted)
		var got struct {
			Index uint64 `json:"accepted_lsn"`
		}
		if err := json.Unmarshal(reply.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		return reply.Header().Get("Location"), got.Index
	}
	status := func(path string) storage.IngestBatchStatus {
		t.Helper()
		var result storage.IngestBatchStatus
		reply := request(http.MethodGet, path, nil, http.StatusOK)
		if err := json.Unmarshal(reply.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	expire := func(node *testReplica, at time.Time, candidate expiringPartitionReceipt) []string {
		t.Helper()
		cmd, err := newCommand("shard_ingest_expire")
		if err != nil {
			t.Fatal(err)
		}
		cmd.At = at
		deferred, err := node.cluster.proposeReceiptExpiry(ctx, cmd, []expiringPartitionReceipt{candidate})
		if err != nil {
			t.Fatal(err)
		}
		return deferred
	}

	originalURL, originalLSN := accept("same-batch", "before-move")
	path, err := url.Parse(originalURL)
	if err != nil || path.Query().Get("accepted_epoch") != "1" || path.Query().Get("accepted_lsn") != strconv.FormatUint(originalLSN, 10) {
		t.Fatalf("accepted receipt lacks its source fence: %s (%v)", originalURL, err)
	}
	if err := old.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	before := status(originalURL)
	if before.State != "committed" || before.Result == nil || before.ReceiptExpiresAt == nil || before.ReceiptExpiresAt.Sub(before.FinishedAt) != retention {
		t.Fatalf("wrong source terminal receipt: %+v", before)
	}
	srcApp := old.cluster.App
	generation, err := old.store.ReplicationTenantGeneration(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	key := srcApp.ingestKey(tenant, "agent", "collector", "same-batch", generation)
	receiptKey := srcApp.managedReceiptKey(key)
	aliasKey := srcApp.ingestAliasKey(tenant, generation, storage.IngestRequest{Source: "agent", CollectorID: "collector", IdempotencyKey: "same-key"})
	stale := expiringPartitionReceipt{Key: receiptKey, Index: originalLSN, Epoch: 1}

	for _, replica := range target.nodes {
		replica.blocked.Store(true)
	}
	request(http.MethodPost, "/v1/cluster/moves", map[string]string{"tenant_id": tenant, "target": "b"}, http.StatusAccepted)
	wait(func() bool {
		srcApp.mu.RLock()
		defer srcApp.mu.RUnlock()
		owner, err := srcApp.ownership(ctx, tenant)
		return err == nil && owner.State == "frozen"
	}, "source never froze")
	if deferred := expire(old, before.ReceiptExpiresAt.Add(time.Second), stale); len(deferred) != 1 || deferred[0] != receiptKey {
		t.Fatalf("expiry crossed a frozen ownership: %v", deferred)
	}
	for _, replica := range target.nodes {
		replica.blocked.Store(false)
	}
	wait(func() bool {
		var state sharding.Catalog
		if err := router.Client.JSON(ctx, definition(catalog, "catalog"), http.MethodGet, "/cluster/catalog", nil, &state); err != nil {
			return false
		}
		p := state.Tenants[tenant]
		return p.State == "active" && p.Shard == "b" && p.Epoch == 2 && p.Move == nil
	}, "tenant move did not complete")
	dst := target.nodes[target.leader(-1)]
	after := status(originalURL)
	if after.AcceptedLSN != before.AcceptedLSN || after.ReceiptExpiresAt == nil || !after.ReceiptExpiresAt.Equal(*before.ReceiptExpiresAt) ||
		after.Result == nil || after.Result.Version != before.Result.Version {
		t.Fatalf("migration changed the original receipt: %+v", after)
	}
	if retryURL, retryLSN := accept("retry-after-move", "before-move"); retryURL != originalURL || retryLSN != originalLSN {
		t.Fatalf("destination alias no longer references the original acceptance: %s %d", retryURL, retryLSN)
	}
	request(http.MethodPost, "/v1/ingest/batches", batch("conflicting-alias", "different"), http.StatusConflict)
	for _, key := range []string{receiptKey, aliasKey} {
		if _, err := old.files.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("source kept migrated WAL metadata: %s (%v)", key, err)
		}
	}
	wait(func() bool {
		return call(http.MethodGet, originalURL, nil).Code == http.StatusNotFound
	}, "destination did not resume expiry")

	pad(dst, 2, originalLSN-1)
	newURL, newLSN := accept("same-batch", "after-move")
	newPath, err := url.Parse(newURL)
	if err != nil || newLSN != originalLSN || newURL == originalURL || newPath.Query().Get("accepted_epoch") != "2" {
		t.Fatalf("failed to exercise coincident shard-local LSNs: original %s, replacement %s (%v)", originalURL, newURL, err)
	}
	if err := dst.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	current := status(newURL)
	if current.State != "committed" || current.Result == nil || current.Result.Version != before.Result.Version+1 || current.ReceiptExpiresAt == nil {
		t.Fatalf("expired alias blocked the replacement: %+v", current)
	}
	expire(dst, current.ReceiptExpiresAt.Add(time.Second), stale)
	request(http.MethodGet, originalURL, nil, http.StatusNotFound)
	request(http.MethodGet, fmt.Sprintf("%s?accepted_lsn=%d", newPath.Path, newLSN), nil, http.StatusNotFound)
	if current := status(newURL); current.AcceptedLSN != originalLSN || current.State != "committed" {
		t.Fatalf("stale expiry changed the replacement: %+v", current)
	}
	request(http.MethodGet, "/v1/entities/after-move", nil, http.StatusOK)
}
