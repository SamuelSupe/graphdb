package ha

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestApplicationBatchRecovery(t *testing.T) {
	root := os.Getenv("GRAPHDB_TEST_BATCH_CRASH_ROOT")
	crashing := root != ""
	if !crashing {
		root = t.TempDir()
	}
	open := func() *Application {
		files, err := storage.OpenFileStore(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := files.RequireReplicatedWrites(); err != nil {
			t.Fatal(err)
		}
		return &Application{Files: files}
	}
	entry := func(index uint64, value string) replication.ApplyEntry {
		data, err := json.Marshal(command{ID: value, At: time.Unix(int64(index), 0), Kind: "http", Method: "POST", URI: "/v1/commits", Body: []byte(value)})
		if err != nil {
			t.Fatal(err)
		}
		return replication.ApplyEntry{Index: index, Data: data}
	}
	app := open()
	fail := false
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value, _ := io.ReadAll(r.Body)
		if err := app.Files.Put(r.Context(), "graphdb/manifest", value); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		if string(value) == "second" {
			if err := app.Files.Put(r.Context(), "graphdb/new", value); err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			if crashing {
				os.Exit(0)
			}
			if fail {
				w.WriteHeader(500)
				return
			}
		}
		w.Write(value)
	})
	app.Handler = handler
	batch := []replication.ApplyEntry{entry(2, "first"), entry(3, "second")}
	if crashing {
		_, err := app.ApplyBatch(context.Background(), batch)
		t.Fatalf("crash helper returned: %v", err)
	}
	if _, err := app.Apply(context.Background(), 1, entry(1, "old").Data); err != nil {
		t.Fatal(err)
	}
	verifyRollback := func() {
		t.Helper()
		checkpoint, err := app.Files.ReplicationCheckpoint()
		if err != nil || checkpoint.Index != 1 {
			t.Fatalf("partial batch advanced checkpoint: %+v, %v", checkpoint, err)
		}
		data, err := app.Files.Get(context.Background(), "graphdb/manifest")
		if err != nil || string(data) != "old" {
			t.Fatalf("earlier command survived interrupted batch: %q, %v", data, err)
		}
		if _, err := app.Files.Get(context.Background(), "graphdb/new"); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("new object survived interrupted batch: %v", err)
		}
	}
	fail = true
	if _, err := app.ApplyBatch(context.Background(), batch); err == nil {
		t.Fatal("failed command acknowledged the batch")
	}
	verifyRollback()
	if err := app.Files.Close(); err != nil {
		t.Fatal(err)
	}
	process := exec.Command(os.Args[0], "-test.run=^TestApplicationBatchRecovery$")
	process.Env = append(os.Environ(), "GRAPHDB_TEST_BATCH_CRASH_ROOT="+root)
	if output, err := process.CombinedOutput(); err != nil {
		t.Fatalf("crash helper: %v: %s", err, output)
	}
	app = open()
	app.Handler = handler
	verifyRollback()
	fail = false
	results, err := app.ApplyBatch(context.Background(), batch)
	if err != nil || len(results) != 2 {
		t.Fatalf("batch replay: %d results, %v", len(results), err)
	}
	for i, want := range []string{"first", "second"} {
		var result httpResult
		if err := json.Unmarshal(results[i], &result); err != nil || result.Status != 200 || string(result.Body) != want {
			t.Fatalf("command result lost: %+v, %v", result, err)
		}
	}
	if err := app.Files.Close(); err != nil {
		t.Fatal(err)
	}
	app = open()
	defer app.Files.Close()
	checkpoint, err := app.Files.ReplicationCheckpoint()
	if err != nil || checkpoint.Index != 3 || string(checkpoint.Response) != string(results[1]) {
		t.Fatalf("committed batch lost after reopen: %+v, %v", checkpoint, err)
	}
	for _, key := range []string{"graphdb/manifest", "graphdb/new"} {
		data, err := app.Files.Get(context.Background(), key)
		if err != nil || string(data) != "second" {
			t.Fatalf("committed batch object lost: %s: %q, %v", key, data, err)
		}
	}
}

func TestApplicationBatchAcceptanceIdentity(t *testing.T) {
	files, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.RequireReplicatedWrites(); err != nil {
		t.Fatal(err)
	}
	store := storage.NewTenantStoreWithOptions(files, "graphdb", storage.TenantStoreOptions{InstanceID: "batch-test"})
	app := &Application{Files: files, Store: store}
	_, err = files.ApplyReplicated(context.Background(), 1, "create", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		_, err := store.CreateTenant(ctx, "tenant-a", storage.TenantCreateOptions{})
		return nil, err
	})
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]replication.ApplyEntry, 2)
	for i := range entries {
		body, _ := json.Marshal(storage.IngestRequest{Source: "test", CollectorID: "test", BatchID: string(rune('a' + i))})
		data, _ := json.Marshal(command{ID: string(rune('a' + i)), At: time.Unix(int64(i+2), 0).UTC(), Kind: "accept", Tenant: "tenant-a", Body: body})
		entries[i] = replication.ApplyEntry{Index: uint64(i + 2), Data: data}
	}
	responses, err := app.ApplyBatch(context.Background(), entries)
	if err != nil || len(responses) != 2 {
		t.Fatalf("accept batch: %d results, %v", len(responses), err)
	}
	for i, response := range responses {
		var result httpResult
		var accepted struct {
			Index uint64    `json:"accepted_lsn"`
			At    time.Time `json:"accepted_at"`
		}
		if err := json.Unmarshal(response, &result); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(result.Body, &accepted); err != nil || result.Status != http.StatusAccepted || accepted.Index != entries[i].Index || !accepted.At.Equal(time.Unix(int64(i+2), 0)) {
			t.Fatalf("acceptance lost its own log identity: %+v, %v", accepted, err)
		}
		record, err := app.accepted(context.Background(), app.ingestKey("tenant-a", "test", "test", string(rune('a'+i)), 1))
		if err != nil || record.Index != accepted.Index || !record.AcceptedAt.Equal(accepted.At) {
			t.Fatalf("durable acceptance differs from response: %+v, %v", record, err)
		}
	}
	app.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) })
	barriers := []command{
		{ID: "control", Kind: "http", Method: "POST", URI: "/v1/tenants"},
		{ID: "flush", Kind: "flush", Tenant: "tenant-a", IDs: []string{"invalid-key"}},
	}
	for i, barrier := range barriers {
		index := uint64(4 + i*2)
		id := string(rune('c' + i))
		body, _ := json.Marshal(storage.IngestRequest{Source: "test", CollectorID: "test", BatchID: id})
		data, _ := json.Marshal(command{ID: id, At: time.Unix(int64(index), 0), Kind: "accept", Tenant: "tenant-a", Body: body})
		barrier.At = time.Unix(int64(index+1), 0)
		control, _ := json.Marshal(barrier)
		boundary := []replication.ApplyEntry{{Index: index, Data: data}, {Index: index + 1, Data: control}}
		results, err := app.ApplyBatch(context.Background(), boundary)
		if err != nil || len(results) != 1 {
			t.Fatalf("acceptance waited for a later %s operation: %d results, %v", barrier.Kind, len(results), err)
		}
		if _, err := app.ApplyBatch(context.Background(), boundary[1:]); err == nil {
			t.Fatalf("failed %s operation was acknowledged", barrier.Kind)
		}
		checkpoint, err := files.ReplicationCheckpoint()
		if err != nil || checkpoint.Index != index {
			t.Fatalf("later %s failure rolled back acceptance: %+v, %v", barrier.Kind, checkpoint, err)
		}
		if record, err := app.accepted(context.Background(), app.ingestKey("tenant-a", "test", "test", id, 1)); err != nil || record.Index != index {
			t.Fatalf("accepted publication lost across %s failure: %+v, %v", barrier.Kind, record, err)
		}
	}
}
