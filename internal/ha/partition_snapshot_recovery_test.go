package ha

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHAPartitionSnapshotRecoveryGCResumesUnpreparedTransaction(t *testing.T) {
	t.Run("direct", func(t *testing.T) { testHAPartitionSnapshotRecoveryGC(t, false) })
	t.Run("accepted_wal", func(t *testing.T) { testHAPartitionSnapshotRecoveryGC(t, true) })
}

func testHAPartitionSnapshotRecoveryGC(t *testing.T, wal bool) {
	t.Helper()
	a, b, _, router, call := newTestPartitionedGraph(t, wal, partition.RecoveryGCProtocol)
	for i := range b.nodes {
		b.stop(i)
	}
	for i, replica := range b.nodes {
		replica.cfg.Raft.SnapshotEntries = 4
		replica.cfg.Raft.ElectionTicks = 30
		b.start(i)
	}
	data := b.nodes[b.leader(-1)]
	homeIndex := a.leader(-1)
	home := a.nodes[homeIndex].cluster.graphHome()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	remote := sharding.GraphRemote{Client: router.Client}
	describe := func() (partition.Graph, error) { return remote.Describe(ctx, home, "big") }
	initial, err := describe()
	if err != nil {
		t.Fatal(err)
	}
	entityIn := func(id, group string) string {
		for initial.Partitions[partition.Bucket(id)].Group != group {
			id += "x"
		}
		return id
	}
	healthyID, blockedID := entityIn("healthy", "a"), entityIn("blocked", "b")
	healthyBucket, blockedBucket := partition.Bucket(healthyID), partition.Bucket(blockedID)
	call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{Mutations: graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host"}},
	}}, http.StatusOK)
	seed := func(id, payload string) {
		call(http.MethodPost, "/v1/commits", "big", partition.CommitRequest{Mutations: graph.Mutations{
			UpsertEntities: []graph.Entity{{ID: id, Kind: "host", Fields: graph.Fields{"payload": payload}}},
		}}, http.StatusOK)
	}
	seed(healthyID, "pinned")
	pinnedGraph, err := describe()
	if err != nil {
		t.Fatal(err)
	}
	pinned, err := partition.AcquireReadView(ctx, remote, home, pinnedGraph)
	if err != nil {
		t.Fatal(err)
	}
	seed(blockedID, strings.Repeat("x", 640<<10))
	large, err := describe()
	if err != nil {
		t.Fatal(err)
	}
	seed(blockedID, "current")
	base, err := describe()
	if err != nil {
		t.Fatal(err)
	}
	old := large.Roots[blockedBucket]
	tree := data.cluster.App.partitionStore().Tree("big", base.Generation, blockedBucket)
	oldPage := tree.Prefix + "/" + old.Hash[:2] + "/" + old.Hash
	probe := partition.Action{Operation: "reserve", Tenant: "big", Generation: base.Generation, Partition: blockedBucket, Epoch: base.Partitions[blockedBucket].Epoch}
	if err := exceedPartitionSnapshotBudget(ctx, b, data.cluster, probe, 512<<10); err != nil {
		t.Fatal(err)
	}

	collect, releaseCollect := make(chan struct{}, 1), make(chan struct{})
	startGC := sync.OnceFunc(func() { close(releaseCollect) })
	defer startGC()
	homeHook := func(w http.ResponseWriter, r *http.Request, cluster *Cluster, action partition.Action) bool {
		if action.Operation == "graph_gc_collect" {
			select {
			case collect <- struct{}{}:
			default:
			}
			select {
			case <-releaseCollect:
			case <-r.Context().Done():
				return true
			}
		}
		return false
	}
	for _, node := range a.nodes {
		node.graphAction.Store(&homeHook)
		defer node.graphAction.Store(nil)
	}

	updates := []graph.Entity{{ID: healthyID, Kind: "host", Fields: graph.Fields{"payload": "new-a"}}, {ID: blockedID, Kind: "host", Fields: graph.Fields{"payload": "new-b"}}}
	direct := partition.CommitRequest{IdempotencyKey: "continue-after-recovery-gc", Mutations: graph.Mutations{UpsertEntities: updates}}
	var statusURL string
	var protected partition.Prepared
	if wal {
		input := storage.IngestRequest{Source: "test", CollectorID: "collector", BatchID: "recover-wal",
			Items: []storage.IngestItem{{Entity: &updates[0]}, {Entity: &updates[1]}}}
		accepted := call(http.MethodPost, "/v1/ingest/batches", "big", input, http.StatusAccepted)
		var receipt struct {
			StatusURL string `json:"status_url"`
		}
		if err := json.Unmarshal(accepted.Body.Bytes(), &receipt); err != nil || receipt.StatusURL == "" {
			t.Fatalf("invalid acceptance: %s (%v)", accepted.Body.String(), err)
		}
		statusURL = receipt.StatusURL
	} else {
		releasePrepare := make(chan struct{})
		allowPrepare := sync.OnceFunc(func() { close(releasePrepare) })
		defer allowPrepare()
		holdPrepare := func(w http.ResponseWriter, r *http.Request, cluster *Cluster, action partition.Action) bool {
			if action.Operation == "prepare" {
				select {
				case <-releasePrepare:
				case <-r.Context().Done():
					return true
				}
			}
			return false
		}
		for _, node := range b.nodes {
			node.graphAction.Store(&holdPrepare)
			defer node.graphAction.Store(nil)
		}
		attempt := make(chan error, 1)
		engine := a.nodes[homeIndex].cluster.partitionEngine("big", base.Generation, false)
		go func() {
			_, err := engine.Submit(ctx, direct)
			attempt <- err
		}()
		for protected.Txn == "" {
			if g, err := describe(); err == nil && g.Active != nil {
				protected = g.Active.Prepared[healthyBucket]
			}
			select {
			case <-ctx.Done():
				t.Fatal("healthy participant did not prepare")
			case <-time.After(20 * time.Millisecond):
			}
		}
		allowPrepare()
		select {
		case err := <-attempt:
			if !errors.Is(err, partition.ErrPrepareSnapshotBudget) {
				t.Fatalf("missing participant did not hit snapshot gate: %v", err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	select {
	case <-collect:
	case <-ctx.Done():
		t.Fatal("snapshot pressure did not start recovery GC")
	}
	paused, err := describe()
	if err != nil || paused.Version != base.Version || paused.Active == nil || paused.GC == nil ||
		paused.Active.State != "preparing" || paused.GC.RecoverySequence != paused.Active.Sequence {
		t.Fatalf("recovery GC did not retain the active transaction: version=%d, active=%+v, gc=%+v (%v)", paused.Version, paused.Active, paused.GC, err)
	}
	commit := partition.Action{Operation: "commit", Tenant: "big", Generation: base.Generation,
		Txn: paused.Active.ID, Sequence: paused.Active.Sequence, Result: &storage.CommitResult{Manifest: storage.Manifest{Version: base.Version + 1}}}
	if err := remote.Action(ctx, home, commit, nil); !errors.Is(err, partition.ErrGraphBusy) {
		t.Fatalf("in-flight publication crossed the recovery fence: %v", err)
	}
	readReceipt := func() storage.IngestBatchStatus {
		t.Helper()
		r := call(http.MethodGet, statusURL, "big", nil, http.StatusOK)
		var status storage.IngestBatchStatus
		if err := json.Unmarshal(r.Body.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		return status
	}
	if wal {
		if receipt := readReceipt(); receipt.State != "accepted" || receipt.Result != nil {
			t.Fatalf("WAL acceptance changed during recovery: %+v", receipt)
		}
	}
	a.stop(homeIndex)
	a.leader(homeIndex)
	afterFailover, err := describe()
	if err != nil || afterFailover.Active == nil || afterFailover.Active.ID != paused.Active.ID || afterFailover.GC == nil || afterFailover.GC.ID != paused.GC.ID {
		t.Fatalf("failover lost recovery state: active=%+v gc=%+v (%v)", afterFailover.Active, afterFailover.GC, err)
	}
	startGC()
	var published partition.Graph
	for {
		published, err = describe()
		if err == nil && published.Active == nil && published.GC == nil && published.Version == base.Version+1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("original transaction did not resume: version=%d gc=%+v (%v)", published.Version, published.GC, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	if published.LastRecoveryGCSequence != paused.Active.Sequence || data.cluster.Node.Status()["snapshot_error"] != nil {
		t.Fatal("a different transaction completed or snapshot pressure persisted")
	}
	if _, err := data.files.Get(ctx, oldPage); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("obsolete graph page survived GC: %v", err)
	}
	if !wal && published.Roots[healthyBucket] != protected.Root {
		t.Fatal("recovery GC lost the prepared root")
	}
	readPin, err := remote.Pin(ctx, home, "big", base.Generation, pinned.Graph.Version, pinned.Pin.Query, pinned.Pin.ID)
	if err != nil || readPin.Graph.Roots[healthyBucket] != pinned.Graph.Roots[healthyBucket] {
		t.Fatalf("recovery GC lost the pinned view: %v", err)
	}
	var historical graph.Entity
	view := partition.View{Graph: readPin.Graph, Remote: remote}
	if found, err := view.Get(ctx, healthyBucket, partition.EntityKey(healthyID), &historical); err != nil || !found || historical.Fields["payload"] != "pinned" {
		t.Fatalf("recovery GC removed the pinned row: %+v (%v)", historical, err)
	}
	for _, update := range updates {
		r := call(http.MethodGet, "/v1/entities/"+update.ID, "big", nil, http.StatusOK)
		if !strings.Contains(r.Body.String(), update.Fields["payload"].(string)) {
			t.Fatalf("recovery did not publish %s: %s", update.ID, r.Body.String())
		}
	}
	if wal {
		if receipt := readReceipt(); receipt.State != "committed" || receipt.Result == nil || receipt.Result.Version != published.Version || receipt.Result.Applied != len(updates) {
			t.Fatalf("wrong terminal WAL receipt: %+v", receipt)
		}
	} else {
		r := call(http.MethodPost, "/v1/commits", "big", direct, http.StatusOK)
		var replay storage.CommitResult
		if err := json.Unmarshal(r.Body.Bytes(), &replay); err != nil || !replay.IdempotentReplay || replay.Version != published.Version || replay.DataHash != published.DataHash {
			t.Fatalf("wrong idempotent result: %+v (%v)", replay, err)
		}
	}
}
