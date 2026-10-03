package ha

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/replication"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestApplicationSnapshotRejectsCorruptPublishedGraph(t *testing.T) {
	for _, failure := range []string{"missing-head", "missing-commit", "corrupt-commit"} {
		t.Run(failure, func(t *testing.T) {
			ctx := context.Background()
			root := t.TempDir()
			files, err := storage.OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			store := storage.NewTenantStore(files, "graphdb")
			var manifest storage.Manifest
			if _, err := files.ApplyReplicated(ctx, 1, "seed", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
				var err error
				manifest, err = store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:seed", Kind: "host"}}}, storage.CommitOptions{})
				return nil, err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := files.ApplyReplicated(ctx, 2, "next-position", time.Unix(2, 0), func(context.Context) ([]byte, error) { return nil, nil }); err != nil {
				t.Fatal(err)
			}
			if _, _, err := store.Load(ctx, "tenant-a"); err != nil {
				t.Fatal(err)
			}
			app := &Application{Store: store, Files: files, MaxSnapshotBytes: 8 << 20}
			var captured replication.SnapshotSource
			if failure != "corrupt-commit" {
				captured, err = app.CaptureSnapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer captured.Close()
			}
			object := manifest.CommitKeys[0]
			if failure == "missing-head" {
				object = "graphdb/tenants/tenant-a/manifest.parquet"
			}
			filename := filepath.Join(root, object)
			if failure == "corrupt-commit" {
				err = os.WriteFile(filename, []byte("corrupt committed graph"), 0600)
			} else {
				err = os.Remove(filename)
			}
			if err != nil {
				t.Fatal(err)
			}
			if captured != nil {
				output, err := os.CreateTemp(t.TempDir(), "captured-")
				if err != nil {
					t.Fatal(err)
				}
				defer output.Close()
				if err := captured.WriteTo(ctx, output); err != nil {
					t.Fatalf("valid captured view read later damaged live files: %v", err)
				}
				if _, err := output.Seek(0, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				target := storage.NewFileStore(t.TempDir())
				if err := target.InstallReplicationSnapshotReader(ctx, 2, output, app.MaxSnapshotBytes); err != nil {
					t.Fatal(err)
				}
				g, manifest, err := storage.NewTenantStore(target, "graphdb").Load(ctx, "tenant-a")
				if err != nil || manifest.Version != 1 {
					t.Fatalf("captured graph restore: version=%d err=%v", manifest.Version, err)
				}
				if _, ok := g.GetEntity("host:seed"); !ok {
					t.Fatal("captured graph lost its entity")
				}
			}
			if data, err := app.Snapshot(ctx); err == nil {
				t.Errorf("legacy snapshot accepted %s: %d encoded bytes", failure, len(data))
			}
			source, err := app.CaptureSnapshot(ctx)
			if err != nil {
				return
			}
			defer source.Close()
			output, err := os.CreateTemp(t.TempDir(), "snapshot-")
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close()
			if err := source.WriteTo(ctx, output); err == nil {
				t.Errorf("streaming snapshot accepted %s", failure)
			}
			legacyInput, err := files.ReplicationSnapshot(ctx, app.MaxSnapshotBytes)
			if err != nil {
				t.Fatal(err)
			}
			for _, format := range []string{"legacy", "streaming"} {
				target, err := storage.OpenFileStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				defer target.Close()
				targetStore := storage.NewTenantStore(target, "graphdb")
				if _, err := target.ApplyReplicated(ctx, 1, "healthy", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
					_, err := targetStore.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:healthy", Kind: "host"}}}, storage.CommitOptions{})
					return nil, err
				}); err != nil {
					t.Fatal(err)
				}
				targetApp := &Application{Files: target, Store: targetStore, MaxSnapshotBytes: app.MaxSnapshotBytes}
				if format == "legacy" {
					err = targetApp.Restore(ctx, 2, legacyInput)
				} else {
					err = targetApp.RestoreSnapshot(ctx, 2, bytes.NewReader(legacyInput))
				}
				if err == nil {
					t.Errorf("%s receiver accepted a checksummed %s snapshot", format, failure)
				}
				g, _, loadErr := storage.NewTenantStore(target, "graphdb").Load(ctx, "tenant-a")
				checkpoint, checkpointErr := target.ReplicationCheckpoint()
				if loadErr != nil || checkpointErr != nil || checkpoint.Index != 1 {
					t.Errorf("%s receiver changed healthy state: checkpoint=%d errors=%v/%v", format, checkpoint.Index, loadErr, checkpointErr)
				} else if _, ok := g.GetEntity("host:healthy"); !ok {
					t.Errorf("%s receiver lost its healthy graph", format)
				}
			}
		})
	}
}

func TestApplicationRejectsForeignDataPrefix(t *testing.T) {
	ctx := context.Background()
	source := storage.NewFileStore(t.TempDir())
	if err := source.Put(ctx, "other/control/identity", []byte("foreign namespace")); err != nil {
		t.Fatal(err)
	}
	if _, err := source.ApplyReplicated(ctx, 2, "source", time.Unix(2, 0), func(context.Context) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	input, err := source.ReplicationSnapshot(ctx, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"legacy", "streaming", "command"} {
		t.Run(format, func(t *testing.T) {
			files := storage.NewFileStore(t.TempDir())
			store := storage.NewTenantStore(files, "graphdb")
			if _, err := files.ApplyReplicated(ctx, 1, "healthy", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
				_, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:healthy", Kind: "host"}}}, storage.CommitOptions{})
				return nil, err
			}); err != nil {
				t.Fatal(err)
			}
			app := &Application{Files: files, Store: store, MaxSnapshotBytes: 8 << 20}
			switch format {
			case "legacy":
				err = app.Restore(ctx, 2, input)
			case "streaming":
				err = app.RestoreSnapshot(ctx, 2, bytes.NewReader(input))
			case "command":
				data := []byte(`{"id":"foreign","at":"2026-10-03T00:00:00Z","kind":"http","prefix":"other","method":"POST","uri":"/v1/commits"}`)
				app.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, commitErr := store.Commit(r.Context(), "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:foreign", Kind: "host"}}}, storage.CommitOptions{})
					if commitErr != nil {
						t.Error(commitErr)
					}
				})
				_, err = app.Apply(ctx, 2, data)
			}
			if err == nil {
				t.Errorf("%s accepted a foreign data prefix", format)
			}
			checkpoint, err := files.ReplicationCheckpoint()
			if err != nil || checkpoint.Index != 1 {
				t.Errorf("%s changed checkpoint to %d, err=%v", format, checkpoint.Index, err)
			}
			g, _, err := storage.NewTenantStore(files, "graphdb").Load(ctx, "tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := g.GetEntity("host:healthy"); !ok {
				t.Error("lost the healthy graph")
			}
			if _, ok := g.GetEntity("host:foreign"); ok {
				t.Error("applied the foreign command")
			}
		})
	}
}

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
	var cancelHandler context.CancelFunc
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value, _ := io.ReadAll(r.Body)
		if string(value) == "second" && cancelHandler != nil {
			cancelHandler()
			w.WriteHeader(http.StatusRequestTimeout)
			return
		}
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
	fail = false
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelHandler = cancel
	if _, err := app.ApplyBatch(ctx, batch); !errors.Is(err, context.Canceled) {
		t.Fatalf("translated cancellation acknowledged the batch: %v", err)
	}
	cancelHandler = nil
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

func TestStagedRestoreIntegrityAndCancellation(t *testing.T) {
	for _, scenario := range []string{"missing", "corrupt", "canceled", "purged", "schema_named_purge"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := context.Background()
			files, err := storage.OpenFileStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			if err := files.RequireReplicatedWrites(); err != nil {
				t.Fatal(err)
			}
			store := storage.NewTenantStore(files, "graphdb")
			store.ReplicationMode = true
			app := &Application{Files: files, Store: store, MaxSnapshotBytes: 64 << 20}
			app.Handler = (&httpapi.Server{Store: store, Mode: "all"}).Handler()
			index := uint64(0)
			apply := func(cmd command, success bool) []byte {
				t.Helper()
				cmd.ID = fmt.Sprint(index + 1)
				cmd.At = time.Unix(int64(index+1), 0)
				if cmd.Tenant != "" {
					cmd.Header = make(http.Header)
					cmd.Header.Set("X-Tenant-ID", cmd.Tenant)
				}
				data, err := json.Marshal(cmd)
				if err != nil {
					t.Fatal(err)
				}
				response, err := app.Apply(ctx, index+1, data)
				if (err == nil) != success {
					t.Fatalf("%s: error=%v, success=%v", cmd.Kind, err, success)
				}
				if success {
					index++
				}
				return response
			}
			apply(command{Kind: "http", Method: "POST", URI: "/v1/tenants", Body: []byte(`{"tenant_id":"tenant-a"}`)}, true)
			wantVersion := int64(0)
			if scenario == "schema_named_purge" {
				seed := apply(command{Kind: "http", Method: "POST", URI: "/v1/commits", Tenant: "tenant-a",
					Body: []byte(`{"mutations":{"upsert_relation_types":[{"name":"purge","from_kind":"host","to_kind":"host"}]}}`)}, true)
				var response httpResult
				if json.Unmarshal(seed, &response) != nil || response.Status != http.StatusOK {
					t.Fatalf("relation type not created: %s", seed)
				}
				wantVersion = 1
			}
			result := apply(command{Kind: "http", Method: "POST", URI: "/v1/tenants/tenant-a/restore", Tenant: "tenant-a", Body: []byte(`{"backup_key":"graphdb/missing-backup","overwrite":true}`)}, true)
			var response httpResult
			var task storage.Task
			if json.Unmarshal(result, &response) != nil || response.Status != 202 || json.Unmarshal(response.Body, &task) != nil || task.ID == "" {
				t.Fatalf("restore not queued: %s", result)
			}
			input := bytes.Repeat([]byte("x"), restoreChunkBytes+1)
			digest := sha256.Sum256(input)
			manifest := restoreManifest{Bytes: int64(len(input)), SHA256: hex.EncodeToString(digest[:])}
			body, _ := json.Marshal(restorePart{restoreManifest: manifest})
			part := input[:restoreChunkBytes]
			if scenario == "corrupt" {
				part = bytes.Repeat([]byte("y"), restoreChunkBytes)
			}
			apply(command{Kind: "restore_part", Tenant: "tenant-a", IDs: []string{task.ID}, Body: body, Restore: part}, true)
			if scenario == "corrupt" {
				body, _ = json.Marshal(restorePart{restoreManifest: manifest, Part: 1})
				apply(command{Kind: "restore_part", Tenant: "tenant-a", IDs: []string{task.ID}, Body: body, Restore: input[restoreChunkBytes:]}, true)
			}
			if scenario == "schema_named_purge" {
				result := apply(command{Kind: "http", Method: "PUT", Tenant: "tenant-a",
					URI: "/v1/relation-schemas/purge", Body: []byte(`{}`)}, true)
				var response httpResult
				staging, err := files.List(ctx, app.restorePrefix("tenant-a", task.ID))
				current, taskErr := store.GetTask(ctx, "tenant-a", task.ID)
				if json.Unmarshal(result, &response) != nil || response.Status != http.StatusOK || err != nil || taskErr != nil || len(staging) != 2 || current.Status != storage.TaskStatusQueued {
					t.Fatalf("schema update discarded pending restore: response=%s staging=%d task=%+v errors=%v/%v", result, len(staging), current, err, taskErr)
				}
			}
			if scenario == "canceled" {
				result := apply(command{Kind: "http", Method: "POST", Tenant: "tenant-a", URI: "/v1/tasks/" + task.ID + "/cancel?reason=test"}, true)
				var canceled httpResult
				current, err := store.GetTask(ctx, "tenant-a", task.ID)
				if json.Unmarshal(result, &canceled) != nil || canceled.Status != 200 || err != nil || current.Status != storage.TaskStatusCanceled {
					t.Fatalf("cancel failed: response=%s task=%+v error=%v", result, current, err)
				}
			}
			if scenario == "purged" {
				result := apply(command{Kind: "http", Method: "POST", Tenant: "tenant-a",
					URI: "/v1/tenants/tenant-a/purge?force=true"}, true)
				var response httpResult
				if json.Unmarshal(result, &response) != nil || response.Status != http.StatusOK {
					t.Fatalf("purge failed: %s", result)
				}
				staging, err := files.List(ctx, app.restorePrefix("tenant-a", task.ID))
				if err != nil || len(staging) != 0 {
					t.Fatalf("purge retained orphaned restore input: %d objects, %v", len(staging), err)
				}
				body, _ = json.Marshal(manifest)
				result = apply(command{Kind: "task", Tenant: "tenant-a", IDs: []string{task.ID}, Body: body, ExpectedGeneration: 1}, true)
				if json.Unmarshal(result, &response) != nil || response.Status != http.StatusConflict {
					t.Fatalf("stale restore was not fenced: %s", result)
				}
				generation, err := store.ReplicationTenantGeneration(ctx, "tenant-a")
				objects, listErr := files.List(ctx, "graphdb/tenants/tenant-a/")
				staging, stagingErr := files.List(ctx, app.restorePrefix("tenant-a", task.ID))
				if err != nil || listErr != nil || stagingErr != nil || generation != 2 || len(objects) != 0 || len(staging) != 0 {
					t.Fatalf("stale restore recreated purged tenant: generation=%d objects=%d staging=%d errors=%v/%v/%v", generation, len(objects), len(staging), err, listErr, stagingErr)
				}
				return
			}
			body, _ = json.Marshal(manifest)
			apply(command{Kind: "task", Tenant: "tenant-a", IDs: []string{task.ID}, Body: body}, scenario == "canceled")
			checkpoint, err := files.ReplicationCheckpoint()
			generation, generationErr := store.ReplicationTenantGeneration(ctx, "tenant-a")
			current, manifestErr := store.CurrentManifest(ctx, "tenant-a")
			if err != nil || generationErr != nil || manifestErr != nil || checkpoint.Index != index || generation != 1 || current.Version != wantVersion {
				t.Fatalf("invalid transfer changed publication: checkpoint=%+v generation=%d version=%d errors=%v/%v/%v", checkpoint, generation, current.Version, err, generationErr, manifestErr)
			}
			if scenario == "canceled" {
				objects, err := files.List(ctx, app.restorePrefix("tenant-a", task.ID))
				if err != nil || len(objects) != 0 {
					t.Fatalf("canceled transfer retained staging: %d objects, %v", len(objects), err)
				}
			}
		})
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
