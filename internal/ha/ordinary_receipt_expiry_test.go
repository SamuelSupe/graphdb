package ha

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHAOrdinaryWALReceiptExpiryReclaimsIdentityAfterFailover(t *testing.T) {
	const retention = 8 * time.Second
	group := newTestCluster(t, true)
	for i := range group.nodes {
		group.stop(i)
	}
	for i, replica := range group.nodes {
		replica.cfg.Raft.Protocol = 12
		group.start(i)
	}
	leader := group.leader(-1)
	ctx := t.Context()
	group.mustRequest(leader, http.MethodPost, "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	post := func(batch, alias, entity string) (string, storage.IngestBatchStatus) {
		t.Helper()
		body := fmt.Sprintf(`{"source":"agent","collector_id":"collector","batch_id":%q,"idempotency_key":%q,"items":[{"entity":{"id":%q,"kind":"host"}}]}`, batch, alias, entity)
		resp := group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", body, http.StatusAccepted)
		var status storage.IngestBatchStatus
		if err := json.Unmarshal(resp.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		return resp.Header().Get("Location"), status
	}
	flush := func() {
		t.Helper()
		if err := group.nodes[leader].cluster.flushPending(ctx); err != nil {
			t.Fatal(err)
		}
	}
	status := func(url string) storage.IngestBatchStatus {
		t.Helper()
		resp := group.mustRequest(leader, http.MethodGet, url, "", http.StatusOK)
		var result storage.IngestBatchStatus
		if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	legacyURL, legacy := post("keep", "keep-key", "retained")
	flush()
	queuedLegacyURL, queuedLegacy := post("old-pending", "old-pending-key", "old-pending-entity")
	queuedDuplicateURL, queuedDuplicate := post("old-pending-duplicate", "old-pending-key", "old-pending-entity")
	if queuedDuplicate.AcceptedLSN == queuedLegacy.AcceptedLSN {
		t.Fatal("protocol-12 fixture did not create two pending acceptances")
	}
	for i := range group.nodes {
		group.stop(i)
	}
	for i, replica := range group.nodes {
		replica.cfg.Raft.Protocol = ingestQueueCheckpointProtocol
		replica.cfg.HAIngestReceiptRetention = retention
		group.start(i)
	}
	leader = group.leader(-1)

	retryLegacyURL, retryLegacy := post("retry-old-pending", "old-pending-key", "old-pending-entity")
	if retryLegacyURL != queuedLegacyURL || retryLegacy.AcceptedLSN != queuedLegacy.AcceptedLSN {
		t.Fatalf("enabling retention duplicated a pending acceptance: %+v", retryLegacy)
	}
	if err := group.nodes[leader].cluster.flushReady(ctx, true); err != nil {
		t.Fatal(err)
	}
	current := status(queuedLegacyURL)
	if current.State != "committed" || current.Result == nil || current.ReceiptExpiresAt != nil {
		t.Fatalf("previously accepted batch changed its policy: %+v", current)
	}
	if duplicate := status(queuedDuplicateURL); duplicate.State != "committed" || duplicate.Result == nil ||
		!duplicate.Result.Skipped || duplicate.Result.Version != current.Result.Version {
		t.Fatalf("queued legacy alias was not resolved against its first publication: %+v", duplicate)
	}

	originalURL, original := post("expire", "repeat", "expired-receipt")
	retryURL, retry := post("alias-retry", "repeat", "expired-receipt")
	if !strings.Contains(originalURL, "?accepted_lsn=") || originalURL != retryURL || retry.AcceptedLSN != original.AcceptedLSN {
		t.Fatalf("pending alias retry changed identity: %q and %q", originalURL, retryURL)
	}
	group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches",
		`{"source":"agent","collector_id":"collector","batch_id":"changed","idempotency_key":"repeat","items":[{"entity":{"id":"changed","kind":"host"}}]}`, http.StatusConflict)
	flush()
	terminal := status(originalURL)
	if terminal.State != "committed" || terminal.Result == nil || terminal.ReceiptExpiresAt == nil || terminal.ReceiptExpiresAt.Sub(terminal.FinishedAt) != retention {
		t.Fatalf("wrong terminal receipt: %+v", terminal)
	}
	if version := group.manifest(leader).Version; version != terminal.Result.Version {
		t.Fatalf("published graph version %d differs from receipt %d", version, terminal.Result.Version)
	}
	pendingURL, pending := post("pending", "pending-key", "pending-receipt")
	oldLeader := leader
	group.stop(oldLeader)
	leader = group.leader(oldLeader)

	deadline := time.Now().Add(2*retention + 5*time.Second)
	for {
		resp := group.request(leader, http.MethodGet, originalURL, "")
		if resp.Code == http.StatusNotFound {
			break
		}
		if resp.Code != http.StatusOK || time.Now().After(deadline) {
			t.Fatalf("original receipt did not expire on the successor: %d %s", resp.Code, resp.Body.String())
		}
		time.Sleep(75 * time.Millisecond)
	}
	if current := status(pendingURL); current.State != "accepted" || current.ReceiptExpiresAt != nil || current.AcceptedLSN != pending.AcceptedLSN {
		t.Fatalf("pending receipt changed after expiry: %+v", current)
	}
	if current := status(legacyURL); current.State != "committed" || current.ReceiptExpiresAt != nil || current.AcceptedLSN != legacy.AcceptedLSN {
		t.Fatalf("legacy receipt changed after configuring retention: %+v", current)
	}

	request := storage.IngestRequest{Source: "agent", CollectorID: "collector", BatchID: "expire", IdempotencyKey: "repeat"}
	replica := group.nodes[leader]
	if _, err := replica.store.GetIngestBatch(ctx, "tenant-a", request.Source, request.CollectorID, request.BatchID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expired canonical record survived: %v", err)
	}
	newURL, reused := post("expire", "repeat", "different-after-expiry")
	if reused.AcceptedLSN <= original.AcceptedLSN || newURL == originalURL {
		t.Fatalf("expired identity was reused without a new acceptance: %+v", reused)
	}
	flush()
	if current := status(newURL); current.State != "committed" || current.Result == nil || current.Result.Version <= terminal.Result.Version {
		t.Fatalf("expired Parquet alias blocked the new payload: %+v", current)
	}
	group.mustRequest(leader, http.MethodGet, originalURL, "", http.StatusNotFound)

	app := replica.cluster.App
	key := app.ingestKey("tenant-a", request.Source, request.CollectorID, request.BatchID, 1)
	cmd, err := newCommand("ordinary_ingest_expire")
	if err != nil {
		t.Fatal(err)
	}
	cmd.At = cmd.At.Add(2 * retention)
	if _, err := replica.cluster.proposeReceiptExpiry(ctx, cmd, []expiringPartitionReceipt{{Key: app.managedReceiptKey(key), Index: original.AcceptedLSN}}); err != nil {
		t.Fatal(err)
	}
	if current := status(newURL); current.AcceptedLSN != reused.AcceptedLSN {
		t.Fatalf("stale Raft cleanup changed the replacement receipt: %+v", current)
	}
}

func TestHAOrdinaryWALReceiptExpiryRecoversSnapshotBudget(t *testing.T) {
	const retention = 7 * time.Second
	const budget = 192 << 10
	group := newTestCluster(t, true)
	for i := range group.nodes {
		group.stop(i)
	}
	for i, replica := range group.nodes {
		replica.cfg.Raft.Protocol = ingestQueueCheckpointProtocol
		replica.cfg.Raft.SnapshotEntries = 4
		replica.cfg.HAIngestReceiptRetention = retention
		group.start(i)
	}
	leader := group.leader(-1)
	replica := group.nodes[leader]
	ctx := t.Context()
	group.mustRequest(leader, http.MethodPost, "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	body := func(i int) string {
		return fmt.Sprintf(`{"source":"test","collector_id":"collector","batch_id":"batch-%03d","idempotency_key":"alias-%03d","items":[{"entity":{"id":"stable","kind":"host"}}]}`, i, i)
	}
	first := group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", body(0), http.StatusAccepted).Header().Get("Location")
	if err := replica.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	version := group.manifest(leader).Version
	for i := 1; i <= 24; i++ {
		group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", body(i), http.StatusAccepted)
	}
	if err := replica.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	if got := group.manifest(leader).Version; got != version {
		t.Fatalf("test grew the live graph: original version %d, final version %d", version, got)
	}
	for _, node := range group.nodes {
		app := node.cluster.App
		app.mu.Lock()
		app.MaxSnapshotBytes = budget
		app.mu.Unlock()
	}
	if _, err := replica.cluster.App.Snapshot(ctx); !errors.Is(err, replication.ErrSnapshotTooLarge) {
		t.Fatalf("retained receipts did not exceed the fixed application budget: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for replica.cluster.Node.Status()["snapshot_error"] == nil {
		cmd, err := newCommand("flush")
		if err != nil {
			t.Fatal(err)
		}
		cmd.Tenant = "tenant-a"
		if _, err := replica.cluster.propose(ctx, cmd); err != nil && !errors.Is(err, replication.ErrSnapshotTooLarge) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("Raft did not publish the snapshot budget error")
		}
		time.Sleep(30 * time.Millisecond)
	}
	blocked := group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", body(25), http.StatusServiceUnavailable)
	if !strings.Contains(blocked.Body.String(), replication.ErrSnapshotTooLarge.Error()) {
		t.Fatalf("wrong reason for write rejection: %s", blocked.Body.String())
	}
	deadline = time.Now().Add(2*retention + 5*time.Second)
	for {
		remaining, err := replica.files.List(ctx, replica.cluster.App.managedReceiptPrefix())
		if err != nil {
			t.Fatal(err)
		}
		if len(remaining) == 0 && replica.cluster.Node.Status()["snapshot_error"] == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot pressure persisted after expiration: receipts=%d status=%v", len(remaining), replica.cluster.Node.Status()["snapshot_error"])
		}
		time.Sleep(70 * time.Millisecond)
	}
	if _, err := replica.cluster.App.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot remained over budget: %v", err)
	}
	group.mustRequest(leader, http.MethodGet, first, "", http.StatusNotFound)
	group.mustRequest(leader, http.MethodGet, "/v1/entities/stable", "", http.StatusOK)
	group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", body(25), http.StatusAccepted)
}
