package ha

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Count object accesses across a cold application restart, including any
// attempt to enumerate receipts that have already reached a terminal state.
type queueRecoveryReads struct {
	storage.ObjectStore
	history            string
	gets, historyLists int
}

func (s *queueRecoveryReads) Get(ctx context.Context, key string) ([]byte, error) {
	if strings.HasPrefix(key, s.history) {
		s.gets++
	}
	return s.ObjectStore.Get(ctx, key)
}
func (s *queueRecoveryReads) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if prefix == s.history {
		s.historyLists++
	}
	return s.ObjectStore.List(ctx, prefix)
}

func TestHAProtocol13PersistsOrdinaryIngestQueue(t *testing.T) {
	group := newTestCluster(t, true)
	ctx := t.Context()
	leader := group.leader(-1)
	group.mustRequest(leader, http.MethodPost, "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	legacyBody := `{"source":"agent","collector_id":"collector","batch_id":"legacy","items":[{"entity":{"id":"host:legacy","kind":"host"}}]}`
	legacyURL := group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", legacyBody, http.StatusAccepted).Header().Get("Location")
	if err := group.nodes[leader].cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	readStatus := func(leader int, url string) storage.IngestBatchStatus {
		t.Helper()
		response := group.mustRequest(leader, http.MethodGet, url, "", http.StatusOK)
		var status storage.IngestBatchStatus
		if err := json.Unmarshal(response.Body.Bytes(), &status); err != nil || status.State != "committed" || status.Result == nil {
			t.Fatalf("terminal receipt: %s (%v)", response.Body.String(), err)
		}
		return status
	}
	legacy := readStatus(leader, legacyURL)
	t.Run("historical cleanup frees snapshot capacity", func(t *testing.T) {
		replica := group.nodes[leader]
		const budget = 64 << 20
		snapshot, err := replica.cluster.App.Snapshot(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		checkpoint, err := replica.files.ReplicationCheckpoint()
		if err != nil {
			t.Fatal(err)
		}
		files, err := storage.OpenFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { files.Close() })
		app := &Application{Store: storage.NewTenantStore(files, replica.store.Prefix), Files: files, MaxSnapshotBytes: budget}
		if err := app.Restore(t.Context(), checkpoint.Index, snapshot); err != nil {
			t.Fatal(err)
		}
		archive, err := gzip.NewReader(bytes.NewReader(snapshot[sha256.Size:]))
		if err != nil {
			t.Fatal(err)
		}
		rawBytes, err := io.Copy(io.Discard, archive)
		if err := errors.Join(err, archive.Close()); err != nil {
			t.Fatal(err)
		}
		app.MaxSnapshotBytes = rawBytes - 1
		if _, err := app.Snapshot(t.Context()); !errors.Is(err, replication.ErrSnapshotTooLarge) {
			t.Fatalf("legacy snapshot should exceed the byte budget: %v", err)
		}
		generation, err := app.Store.ReplicationTenantGeneration(t.Context(), legacy.TenantID)
		if err != nil {
			t.Fatal(err)
		}
		cmd, err := newCommand("ordinary_ingest_compact")
		if err != nil {
			t.Fatal(err)
		}
		cmd.IDs = []string{app.ingestKey(legacy.TenantID, legacy.Source, legacy.CollectorID, legacy.BatchID, generation)}
		data, err := json.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := app.Apply(t.Context(), checkpoint.Index+1, data); err != nil {
			t.Fatal(err)
		}
		if _, err := app.Snapshot(t.Context()); err != nil {
			t.Fatalf("compacted snapshot did not fit the same raw byte budget: %v", err)
		}
	})
	for i := range group.nodes {
		group.stop(i)
	}
	for i := range group.nodes {
		group.nodes[i].cfg.Raft.Protocol = ingestQueueCheckpointProtocol
		group.nodes[i].cfg.Raft.StreamSnapshots = true
		group.start(i)
	}
	leader = group.leader(-1)
	body := `{"source":"agent","collector_id":"collector","batch_id":"pending","idempotency_key":"once","items":[{"external_id":"host:1","entity":{"id":"host:1","kind":"host"}}]}`
	acceptance := group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", body, http.StatusAccepted)
	replica := group.nodes[leader]
	app := replica.cluster.App
	generation, err := replica.store.ReplicationTenantGeneration(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	key := app.ingestKey("tenant-a", "agent", "collector", "pending", generation)
	recoveryReads := &queueRecoveryReads{ObjectStore: replica.files, history: replica.cluster.App.ingestPrefix()}
	cold := &Application{Files: replica.files, Store: storage.NewTenantStore(recoveryReads, replica.store.Prefix)}
	queue, err := cold.pendingSnapshot(ctx)
	if err != nil || len(queue) != 1 || recoveryReads.historyLists != 0 || recoveryReads.gets != 1 {
		t.Fatalf("pending=%d list=%d reads=%d error=%v", len(queue), recoveryReads.historyLists, recoveryReads.gets, err)
	}
	if err := replica.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := replica.files.ReplicationCheckpoint()
	if err != nil || checkpoint.IngestQueue == nil || checkpoint.IngestQueue.Count != 0 {
		t.Fatalf("drained checkpoint queue=%+v error=%v", checkpoint.IngestQueue, err)
	}
	if _, err := replica.files.Get(ctx, key); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("terminal control object still exists: %v", err)
	}
	flush, err := newCommand("flush")
	if err != nil {
		t.Fatal(err)
	}
	flush.Tenant, flush.IDs = "tenant-a", []string{key}
	if data, err := replica.cluster.propose(ctx, flush); err != nil || string(data) != "[]" {
		t.Fatalf("repeated flush: %s (%v)", data, err)
	}
	statusURL := acceptance.Header().Get("Location")
	terminal := readStatus(leader, statusURL)
	legacyKey := app.ingestKey("tenant-a", "agent", "collector", "legacy", generation)
	if _, err := replica.cluster.ordinaryReceiptCleanupStep(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.files.Get(ctx, legacyKey); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("verified legacy control object still exists: %v", err)
	}
	alias := strings.Replace(body, `"pending"`, `"alias"`, 1)
	aliasURL := group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", alias, http.StatusAccepted).Header().Get("Location")
	if err := replica.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := replica.cluster.ordinaryReceiptCleanupStep(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if replay := readStatus(leader, aliasURL); replay.Result.SkipReason != storage.IngestSkipReasonIdempotentReplay {
		t.Fatalf("alias lost its original result: %+v", replay)
	}
	snapshot, err := app.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	checkpoint, err = replica.files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	target, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { target.Close() })
	restored := &Application{Store: storage.NewTenantStore(target, replica.store.Prefix), Files: target, MaxSnapshotBytes: 64 << 20}
	if err := restored.Restore(ctx, checkpoint.Index, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, want := range []storage.IngestBatchStatus{terminal, legacy} {
		got, err := restored.ordinaryIngestReceipt(ctx, want.TenantID, generation, want.Source, want.CollectorID, want.BatchID)
		if err != nil || got.Index != want.AcceptedLSN || got.Result == nil || got.Result.Version != want.Result.Version {
			t.Fatalf("snapshot-restored receipt: %+v (%v)", got, err)
		}
	}

	follower := (leader + 1) % len(group.nodes)
	group.stop(follower)
	lateBody := strings.NewReplacer(`"pending"`, `"late"`, `"once"`, `"late-key"`).Replace(body)
	lateURL := group.mustRequest(leader, http.MethodPost, "/v1/ingest/batches", lateBody, http.StatusAccepted).Header().Get("Location")
	if err := replica.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	late := readStatus(leader, lateURL)
	lateCheckpoint, err := replica.files.ReplicationCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	snapshotDeadline := time.Now().Add(10 * time.Second)
	for replica.cluster.Node.Status()["snapshot_index"].(uint64) < lateCheckpoint.Index {
		if time.Now().After(snapshotDeadline) {
			t.Fatalf("Raft did not compact the missing receipt entries: %v", replica.cluster.Node.Status())
		}
		advance, err := newCommand("flush")
		if err != nil {
			t.Fatal(err)
		}
		advance.Tenant = late.TenantID
		if _, err := replica.cluster.propose(ctx, advance); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	group.start(follower)
	group.waitApplied(follower, lateCheckpoint.Index)
	caughtUp, err := group.nodes[follower].cluster.App.ordinaryIngestReceipt(ctx, late.TenantID, generation, late.Source, late.CollectorID, late.BatchID)
	if err != nil || caughtUp.Index != late.AcceptedLSN || caughtUp.Result == nil || caughtUp.Result.Version != late.Result.Version {
		t.Fatalf("follower's streamed snapshot changed the receipt: %+v (%v)", caughtUp, err)
	}

	group.stop(leader)
	replacement := group.leader(leader)
	for _, item := range []struct {
		url  string
		body string
		want storage.IngestBatchStatus
	}{{statusURL, body, terminal}, {legacyURL, legacyBody, legacy}} {
		pinned := fmt.Sprintf("%s?accepted_lsn=%d", item.url, item.want.AcceptedLSN)
		got := readStatus(replacement, pinned)
		if got.AcceptedLSN != item.want.AcceptedLSN || !got.AcceptedAt.Equal(item.want.AcceptedAt) ||
			!got.FinishedAt.Equal(item.want.FinishedAt) || got.Result.Version != item.want.Result.Version {
			t.Fatalf("receipt changed after migration or failover: got %+v, want %+v", got, item.want)
		}
		replay := group.mustRequest(replacement, http.MethodPost, "/v1/ingest/batches", item.body, http.StatusAccepted)
		var replayed storage.IngestBatchStatus
		if err := json.Unmarshal(replay.Body.Bytes(), &replayed); err != nil || replayed.AcceptedLSN != got.AcceptedLSN {
			t.Fatalf("repeated accept: %s (%v)", replay.Body.String(), err)
		}
	}
	group.mustRequest(replacement, http.MethodGet, statusURL+"?accepted_lsn=0", "", http.StatusNotFound)
	group.mustRequest(replacement, http.MethodPost, "/v1/ingest/batches", strings.ReplaceAll(body, "host:1", "host:changed"), http.StatusConflict)
	request := httptest.NewRequest(http.MethodPost, "/v1/ingest/batches", strings.NewReader(body)).WithContext(ctx)
	request.Header.Set("X-Tenant-ID", "tenant-a")
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Prefer", "wait=committed")
	response := httptest.NewRecorder()
	group.nodes[replacement].handler.ServeHTTP(response, request)
	var result storage.IngestResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || response.Code != http.StatusOK ||
		response.Header().Get("Preference-Applied") != "wait=committed" || result.Version != terminal.Result.Version {
		t.Fatalf("committed wait: HTTP %d %s (%v)", response.Code, response.Body.String(), err)
	}
}

func TestHAPurgeReclaimsObsoleteIngestReceipts(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) { testHAPurgeReclaimsObsoleteIngestReceipts(t, "") })
	t.Run("tenant_shard", func(t *testing.T) { testHAPurgeReclaimsObsoleteIngestReceipts(t, "a") })
}

func testHAPurgeReclaimsObsoleteIngestReceipts(t *testing.T, shardID string) {
	t.Helper()
	group := newTestClusterRole(t, true, shardID, false)
	ctx := t.Context()
	request := func(node int, method, uri, body string, status int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(method, uri, strings.NewReader(body)).WithContext(ctx)
		if !strings.HasPrefix(uri, "/v1/tenants") {
			r.Header.Set("X-Tenant-ID", "tenant-a")
		}
		r.Header.Set("Content-Type", "application/json")
		if shardID != "" {
			r.Header.Set(sharding.EpochHeader, "1")
		}
		w := httptest.NewRecorder()
		group.nodes[node].handler.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s %s: got %d, want %d: %s", method, uri, w.Code, status, w.Body.String())
		}
		return w
	}
	leader := group.leader(-1)
	if shardID != "" {
		own, err := newCommand("sharding")
		if err != nil {
			t.Fatal(err)
		}
		own.Body, err = json.Marshal(sharding.Action{Operation: "own", Tenant: "tenant-a", Epoch: 1})
		if err != nil {
			t.Fatal(err)
		}
		data, err := group.nodes[leader].cluster.propose(ctx, own)
		var response httpResult
		if err != nil || json.Unmarshal(data, &response) != nil || response.Status != http.StatusOK {
			t.Fatalf("assign tenant: %s (%v)", data, err)
		}
	}
	request(leader, http.MethodPost, "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	const batch = `{"source":"agent","collector_id":"collector","batch_id":"reuse","items":[{"entity":{"id":"host:before","kind":"host"}}]}`
	request(leader, http.MethodPost, "/v1/ingest/batches", batch, http.StatusAccepted)
	if err := group.nodes[leader].cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	for i := range group.nodes {
		group.stop(i)
	}
	for i, node := range group.nodes {
		node.cfg.Raft.Protocol = ingestQueueCheckpointProtocol
		group.start(i)
	}
	leader = group.leader(-1)
	app := group.nodes[leader].cluster.App
	generation, err := app.Store.ReplicationTenantGeneration(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	pendingBatch := strings.ReplaceAll(batch, `"reuse"`, `"queued"`)
	pendingURL := request(leader, http.MethodPost, "/v1/ingest/batches", pendingBatch, http.StatusAccepted).Header().Get("Location")
	oldPending := app.ingestKey("tenant-a", "agent", "collector", "queued", generation)
	oldTerminal := app.ingestKey("tenant-a", "agent", "collector", "reuse", generation)
	request(leader, http.MethodPost, "/v1/tenants/tenant-a/purge?force=true", "", http.StatusOK)
	group.stop(leader)
	next := group.leader(leader)

	request(next, http.MethodPost, "/v1/tenants", `{"tenant_id":"tenant-a"}`, http.StatusOK)
	freshBatch := strings.ReplaceAll(batch, "host:before", "host:after")
	freshURL := request(next, http.MethodPost, "/v1/ingest/batches", freshBatch, http.StatusAccepted).Header().Get("Location")
	replica := group.nodes[next]
	deadline := time.Now().Add(6 * time.Second)
	for {
		_, pendingErr := replica.files.Get(ctx, oldPending)
		_, terminalErr := replica.files.Get(ctx, oldTerminal)
		if errors.Is(pendingErr, storage.ErrNotFound) && errors.Is(terminalErr, storage.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("obsolete receipts survived purge and failover: pending=%v terminal=%v", pendingErr, terminalErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// An already queued flush may be applied after the cleanup command.
	delayed, err := newCommand("flush")
	if err != nil {
		t.Fatal(err)
	}
	delayed.Tenant, delayed.IDs = "tenant-a", []string{oldPending}
	if shardID != "" {
		delayed.RouteEpoch = 1
	}
	if data, err := replica.cluster.propose(ctx, delayed); err != nil || string(data) != "[]" {
		t.Fatalf("delayed flush after reclamation: %s (%v)", data, err)
	}
	if err := replica.cluster.flushPending(ctx); err != nil {
		t.Fatal(err)
	}
	request(next, http.MethodGet, pendingURL, "", http.StatusNotFound)
	request(next, http.MethodGet, freshURL, "", http.StatusOK)
	request(next, http.MethodGet, "/v1/entities/host:after", "", http.StatusOK)
	request(next, http.MethodGet, "/v1/entities/host:before", "", http.StatusNotFound)

	checkpoint, err := replica.files.ReplicationCheckpoint()
	if err != nil || checkpoint.IngestQueue == nil || checkpoint.IngestQueue.Count != 0 {
		t.Fatalf("drained durable queue: %+v (%v)", checkpoint.IngestQueue, err)
	}
	group.start(leader)
	group.waitApplied(leader, checkpoint.Index)
	group.stop(next)
	reopened := group.leader(next)
	request(reopened, http.MethodGet, freshURL, "", http.StatusOK)
	request(reopened, http.MethodGet, pendingURL, "", http.StatusNotFound)
}

func TestPendingRecoveryAcrossLegacyCheckpointSpillAndRollback(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	app := &Application{Files: files, ShardID: "home", Store: storage.NewTenantStore(files, "graphdb")}
	reads := &queueRecoveryReads{ObjectStore: app.Store.Objects, history: app.ingestPrefix()}
	app.Store.Objects = reads
	key := func(i int) string { return app.ingestKey("t", "s", "c", fmt.Sprint(i), 1) }
	record := func(i int, state string) acceptedRequest {
		return acceptedRequest{Partitioned: true, Tenant: "t", Generation: 1, Index: uint64(i + 1), State: state, Request: storage.IngestRequest{Source: "s", CollectorID: "c", BatchID: fmt.Sprint(i)}}
	}
	const history = 100
	// Simulate the original representation, with no membership hint.
	for i := range history + 1 {
		state := "committed"
		if i == history {
			state = "accepted"
		}
		data, err := json.Marshal(record(i, state))
		if err != nil {
			t.Fatal(err)
		}
		if err := files.Put(ctx, key(i), data); err != nil {
			t.Fatal(err)
		}
	}
	cold := func(size, gets, lists int) {
		t.Helper()
		app = &Application{Files: files, ShardID: "home", Store: app.Store}
		reads.gets, reads.historyLists = 0, 0
		q, err := app.pendingSnapshot(ctx)
		if err != nil || len(q) != size || reads.gets != gets || reads.historyLists != lists {
			t.Fatalf("recovery: pending %d, reads %d, history lists %d, err %v", len(q), reads.gets, reads.historyLists, err)
		}
	}
	cold(1, history+1, 1)
	if _, err := app.Apply(ctx, 1, nil); err != nil {
		t.Fatal(err)
	}
	cold(1, 1, 0)
	apply := func(index uint64, change func(context.Context) error, abort bool) error {
		_, err := files.ApplyReplicated(ctx, index, "queue", time.Unix(int64(index), 0), func(tx context.Context) ([]byte, error) {
			if err := change(tx); err != nil {
				return nil, err
			}
			if err := app.checkpointPending(tx); err != nil {
				return nil, err
			}
			if abort {
				return nil, context.Canceled
			}
			return nil, nil
		})
		return err
	}
	const extra = inlinePendingLimit
	add := func(tx context.Context) error {
		for i := history + 1; i <= history+extra; i++ {
			if err := app.saveAccepted(tx, key(i), record(i, "accepted")); err != nil {
				return err
			}
		}
		return nil
	}
	if err := apply(2, add, true); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	cold(1, 1, 0)
	if err := apply(2, add, false); err != nil {
		t.Fatal(err)
	}
	cold(extra+1, extra+1, 0)
	// Completing the oldest acceptance and adding a new one in the same journal
	// leaves the next accepted LSN at the head, including after a cold restart.
	if err := apply(3, func(tx context.Context) error {
		if err := app.saveAccepted(tx, key(history), record(history, "committed")); err != nil {
			return err
		}
		return app.saveAccepted(tx, key(history+extra+1), record(history+extra+1, "accepted"))
	}, false); err != nil {
		t.Fatal(err)
	}
	cold(extra+1, extra+1, 0)
	first, err := app.firstPartitionIngest(ctx, "t", 1)
	if err != nil || first != key(history+1) {
		t.Fatalf("queue head: %s %v", first, err)
	}
	old, err := app.accepted(ctx, key(0))
	if err != nil || old.Index != 1 || old.State != "committed" {
		t.Fatalf("original receipt: %+v %v", old, err)
	}
	// An older writer publishes only canonical receipts and drops the new hint.
	// Surviving membership objects must not resurrect its completed batch.
	_, err = files.ApplyReplicated(ctx, 4, "legacy", time.Unix(4, 0), func(tx context.Context) ([]byte, error) {
		if err := files.SetReplicationIngestQueue(tx, nil); err != nil {
			return nil, err
		}
		data, err := json.Marshal(record(history+1, "committed"))
		if err != nil {
			return nil, err
		}
		return nil, files.Put(tx, key(history+1), data)
	})
	if err != nil {
		t.Fatal(err)
	}
	cold(extra, history+extra+2, 1)
	if err := apply(5, func(tx context.Context) error {
		return app.saveAccepted(tx, key(history+extra+2), record(history+extra+2, "accepted"))
	}, false); err != nil {
		t.Fatal(err)
	}
	cold(extra+1, extra+1, 0)
	t.Run("restore spilled queue onto fresh volume", func(t *testing.T) {
		const budget = 16 << 20
		app.MaxSnapshotBytes = budget
		snapshot, err := app.Snapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		target, err := storage.OpenFileStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { target.Close() })
		counted := &queueRecoveryReads{ObjectStore: target, history: app.ingestPrefix()}
		restored := &Application{Files: target, ShardID: app.ShardID, MaxSnapshotBytes: budget, Store: storage.NewTenantStore(counted, app.Store.Prefix)}
		if err := restored.Restore(ctx, 5, snapshot); err != nil {
			t.Fatal(err)
		}
		q, err := restored.pendingSnapshot(ctx)
		if err != nil || len(q) != extra+1 || counted.gets != extra+1 || counted.historyLists != 0 {
			t.Fatalf("restored queue: size %d, reads %d, history lists %d, error %v", len(q), counted.gets, counted.historyLists, err)
		}
		first, err := restored.firstPartitionIngest(ctx, "t", 1)
		if err != nil || first != key(history+2) {
			t.Fatalf("restored queue head: %s %v", first, err)
		}
		old, err := restored.accepted(ctx, key(0))
		if err != nil || old.State != "committed" || old.Index != 1 {
			t.Fatalf("restored historical receipt: %+v %v", old, err)
		}
	})
	t.Run("unrelated tenant assignment preserves spilled membership", func(t *testing.T) {
		cmd, err := newCommand("sharding")
		if err != nil {
			t.Fatal(err)
		}
		cmd.Body = []byte(`{"operation":"own","tenant_id":"other","epoch":1}`)
		data, err := json.Marshal(cmd)
		if err != nil {
			t.Fatal(err)
		}
		response, err := app.Apply(ctx, 6, data)
		var result httpResult
		if err != nil || json.Unmarshal(response, &result) != nil || result.Status != 200 {
			t.Fatalf("assign other tenant: %s %v", response, err)
		}
		cold(extra+1, extra+1, 0)
	})
}

func BenchmarkPartitionOnlyFlush(b *testing.B) {
	for _, tenants := range []int{1, 16, 64, 256} {
		b.Run(fmt.Sprint(tenants), func(b *testing.B) {
			cluster := &Cluster{App: &Application{pending: make(map[string]pendingAcceptance)}}
			for i := range 1024 {
				cluster.App.pending[fmt.Sprint(i)] = pendingAcceptance{partitioned: true, tenant: fmt.Sprint(i % tenants), requestBytes: 512}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := cluster.flushReady(context.Background(), true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
