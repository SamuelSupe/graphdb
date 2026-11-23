package ha

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestEnsurePendingFallbackOnCorruptHintCount(t *testing.T) {
	ctx := context.Background()
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	app := &Application{Files: files, ShardID: "home", Store: storage.NewTenantStore(files, "graphdb")}
	key := app.ingestKey("tenant-a", "src", "col", "b1", 1)
	rec := acceptedRequest{
		Partitioned: true,
		Tenant:      "tenant-a",
		Generation:  1,
		Index:       1,
		State:       "accepted",
		Request:     storage.IngestRequest{Source: "src", CollectorID: "col", BatchID: "b1"},
	}
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := files.ApplyReplicated(ctx, 1, "seed", time.Unix(1, 0).UTC(), func(tx context.Context) ([]byte, error) {
		if err := files.Put(tx, key, data); err != nil {
			return nil, err
		}
		// Persist a corrupt spilled hint whose Count disagrees with canonical receipts.
		return nil, files.SetReplicationIngestQueue(tx, &storage.ReplicationIngestQueue{
			Count:   99,
			Spilled: true,
		})
	}); err != nil {
		t.Fatal(err)
	}

	pending, err := app.pendingSnapshot(ctx)
	if err != nil {
		t.Fatalf("ensurePending fallback failed on corrupt hint count: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("expected 1 recovered pending receipt, got %d", len(pending))
	}
	if _, ok := pending[key]; !ok {
		t.Fatalf("expected recovered key %q in pending map", key)
	}
}

func TestShardingMutatesReceiptsClassification(t *testing.T) {
	for _, op := range []string{"install", "retire", "discard"} {
		body, _ := json.Marshal(map[string]string{"operation": op})
		if !shardingMutatesReceipts(body) {
			t.Errorf("expected operation %q to mutate receipts", op)
		}
	}
	for _, op := range []string{"own", "reserve", "freeze", "activate", "thaw", "stage"} {
		body, _ := json.Marshal(map[string]string{"operation": op})
		if shardingMutatesReceipts(body) {
			t.Errorf("expected operation %q NOT to mutate receipts", op)
		}
	}
}

func TestClusterReadRouteClassifiesReadOnlyPostEndpoints(t *testing.T) {
	readPaths := []string{
		"/cluster/graph/read",
		"/cluster/graph/read-batch",
		"/cluster/graph/input",
		"/cluster/graph/pages",
		"/cluster/export",
		"/cluster/export/manifest",
		"/cluster/export/chunk",
	}
	for _, p := range readPaths {
		req := httptest.NewRequest(http.MethodPost, p, nil)
		if !clusterReadRoute(req) {
			t.Errorf("expected POST %s to be classified as read-only", p)
		}
	}
	for _, p := range []string{"/cluster/action", "/cluster/graph/action", "/cluster/graph/enable"} {
		req := httptest.NewRequest(http.MethodPost, p, nil)
		if clusterReadRoute(req) {
			t.Errorf("expected POST %s to be classified as mutation", p)
		}
	}
}
