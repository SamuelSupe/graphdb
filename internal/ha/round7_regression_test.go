package ha

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestRound7StaleRestorePartAndStagedTaskReturnConflictNotFatal(t *testing.T) {
	ctx := context.Background()
	fs, err := storage.OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	store := storage.NewTenantStore(fs, "r7-ha-restore")
	defer store.ShutdownTasks(ctx)
	store.ReplicationMode = true

	tenant := "t-restore-conflict"
	repCtx := storage.ReplicatedContext(ctx, "seed-1", time.Now().UTC())
	if _, err := store.CreateTenant(repCtx, tenant, storage.TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(repCtx, tenant, graph.Mutations{
		UpsertEntities: []graph.Entity{{ID: "h1", Kind: "host"}},
	}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	queued, err := store.StartTask(repCtx, tenant, storage.TaskTypeIndexRebuild, nil)
	if err != nil {
		t.Fatalf("StartTask failed: %v", err)
	}

	app := &Application{Files: fs, Store: store}

	// 1. Stale restore_part with Part > 0 when manifest.json is missing must return 409 conflict, NOT fatal Go error!
	m1 := restoreManifest{Bytes: restoreChunkBytes + 4, SHA256: strings.Repeat("a", 64), Generation: 1, Maintenance: true}
	bodyPart1, _ := json.Marshal(restorePart{restoreManifest: m1, Part: 1})
	resp, err := app.stageRestore(repCtx, command{
		ID:      "p1",
		Kind:    "restore_part",
		Tenant:  tenant,
		IDs:     []string{queued.ID},
		Body:    bodyPart1,
		Restore: []byte("abcd"),
	}).toCommandResult()
	if err != nil {
		t.Fatalf("stale restore_part (missing manifest) returned fatal Go error: %v", err)
	}
	if !errors.Is(checkProposalConflict(resp), storage.ErrConflict) {
		t.Fatalf("expected 409 conflict response for missing manifest, got %s", string(resp))
	}

	// 2. Stage part 0 with manifest M1, then send part 0 with conflicting manifest M2 -> 409 conflict, NOT fatal Go error!
	mSmall1 := restoreManifest{Bytes: 4, SHA256: strings.Repeat("a", 64), Generation: 1, Maintenance: true}
	bodyM1, _ := json.Marshal(restorePart{restoreManifest: mSmall1, Part: 0})
	if _, err := app.stageRestore(repCtx, command{
		ID:      "p0-m1",
		Kind:    "restore_part",
		Tenant:  tenant,
		IDs:     []string{queued.ID},
		Body:    bodyM1,
		Restore: []byte("1234"),
	}).toCommandResult(); err != nil {
		t.Fatalf("initial stageRestore failed: %v", err)
	}
	mSmall2 := restoreManifest{Bytes: 4, SHA256: strings.Repeat("b", 64), Generation: 1, Maintenance: true}
	bodyM2, _ := json.Marshal(restorePart{restoreManifest: mSmall2, Part: 0})
	resp, err = app.stageRestore(repCtx, command{
		ID:      "p0-m2",
		Kind:    "restore_part",
		Tenant:  tenant,
		IDs:     []string{queued.ID},
		Body:    bodyM2,
		Restore: []byte("5678"),
	}).toCommandResult()
	if err != nil {
		t.Fatalf("conflicting manifest stageRestore returned fatal Go error: %v", err)
	}
	if !errors.Is(checkProposalConflict(resp), storage.ErrConflict) {
		t.Fatalf("expected 409 conflict response for mismatched manifest, got %s", string(resp))
	}

	// 3. Stale runStagedRestore with mismatched manifest M2 -> typed conflict outcome -> applyCommand returns 409!
	rawM2, _ := json.Marshal(mSmall2)
	outcome := app.runStagedRestore(repCtx, command{
		ID:     "pub-m2",
		Kind:   "task",
		Tenant: tenant,
		IDs:    []string{queued.ID},
		Body:   rawM2,
	})
	if outcome.fatalErr != nil || outcome.conflictMsg == "" {
		t.Fatalf("expected non-fatal conflict outcome from mismatched runStagedRestore, got %+v", outcome)
	}
	respM2, err := outcome.toCommandResult()
	if err != nil || !errors.Is(checkProposalConflict(respM2), storage.ErrConflict) {
		t.Fatalf("expected 409 conflict response from mismatched runStagedRestore, got resp=%s err=%v", string(respM2), err)
	}
	// Verify checkProposalConflict parses 409 httpResult cleanly and ignores Task JSON.
	conflictBytes, _ := resultJSON(http.StatusConflict, map[string]any{"code": "restore_stage_conflict"})
	if !errors.Is(checkProposalConflict(conflictBytes), storage.ErrConflict) {
		t.Fatalf("expected checkProposalConflict to detect 409")
	}
	taskBytes, _ := json.Marshal(queued)
	if err := checkProposalConflict(taskBytes); err != nil {
		t.Fatalf("expected checkProposalConflict to accept Task JSON, got %v", err)
	}
}

func TestRound7LoadAcceptedUnderRLockDecodesWithoutWriteLock(t *testing.T) {
	ctx := context.Background()
	mem := storage.NewMemoryStore()
	store := storage.NewTenantStore(mem, "r7-rlock")
	defer store.ShutdownTasks(ctx)
	app := &Application{Store: store}

	expected := acceptedRequest{
		Tenant: "t1",
		State:  "accepted",
		Index:  7,
	}
	raw, _ := json.Marshal(expected)
	if err := mem.Put(ctx, "ingest/r1.json", raw); err != nil {
		t.Fatal(err)
	}

	// Hold another RLock simultaneously — loadAcceptedUnderRLock must not block!
	app.mu.RLock()
	defer app.mu.RUnlock()
	done := make(chan acceptedRequest, 1)
	errCh := make(chan error, 1)
	go func() {
		rec, err := app.loadAcceptedUnderRLock(ctx, "ingest/r1.json")
		if err != nil {
			errCh <- err
			return
		}
		done <- rec
	}()
	select {
	case err := <-errCh:
		t.Fatalf("loadAcceptedUnderRLock failed: %v", err)
	case rec := <-done:
		if rec.Tenant != "t1" || rec.Index != 7 || rec.State != "accepted" {
			t.Fatalf("unexpected decoded record: %+v", rec)
		}
	case <-time.After(time.Second):
		t.Fatalf("loadAcceptedUnderRLock blocked while concurrent RLock was held")
	}
}
