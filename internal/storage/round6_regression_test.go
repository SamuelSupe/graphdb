package storage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/backupstore"
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func TestRound6AutomaticBackupRestoreDrillPersistsIsolatedRestoreTask(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	if err := fs.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(fs, "graphdb")
	defer store.ShutdownTasks(ctx)
	s3 := newRound5FakeS3Server(t)
	store.Backups, err = backupstore.New(ctx, backupstore.Config{
		Bucket:          "test-backups",
		Prefix:          "backups",
		Endpoint:        s3.URL,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		PathStyle:       true,
	})
	if err != nil {
		t.Fatal(err)
	}

	tenant := "r6-drill-backup"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "calls", FromKind: "service", ToKind: "service", Directed: true},
		},
		UpsertEntities: []graph.Entity{
			{ID: "svc:1", Kind: "service", Fields: graph.Fields{"name": "payments"}},
			{ID: "svc:2", Kind: "service", Fields: graph.Fields{"name": "ledger"}},
		},
		UpsertEdges: []graph.Edge{
			{ID: "e1", Type: "calls", From: "svc:1", To: "svc:2"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}

	taskID := "auto-drill-1"
	task := Task{
		ID:       taskID,
		TenantID: tenant,
		Type:     TaskTypeTenantBackup,
		Status:   TaskStatusQueued,
		Params: map[string]any{
			"destination":   "object",
			"automatic":     true,
			"restore_drill": true,
			"keep_count":    2,
		},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err := store.CaptureReplicatedObjectBackup(ctx, tenant, taskID); err != nil {
		t.Fatalf("CaptureReplicatedObjectBackup failed: %v", err)
	}

	prepared, err := store.PrepareReplicatedTask(ctx, task)
	if err != nil {
		t.Fatalf("PrepareReplicatedTask with restore_drill=true failed: %v", err)
	}
	var payload replicatedBackupPayload
	if err := json.Unmarshal(prepared, &payload); err != nil {
		t.Fatalf("unmarshal prepared payload: %v", err)
	}
	if payload.Automation == nil || payload.Automation["restore_drill"] == nil {
		t.Fatalf("expected restore_drill report in automation payload, got %+v", payload.Automation)
	}
	done, err := store.RunReplicatedTask(ctx, tenant, taskID, prepared)
	if err != nil {
		t.Fatalf("RunReplicatedTask failed: %v", err)
	}
	if done.Status != TaskStatusSucceeded {
		t.Fatalf("task status = %q, err = %q", done.Status, done.Error)
	}
}

func TestRound6ObjectBackupIDUsesCheckpointRemoteBackupID(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	store := NewTenantStore(fs, "graphdb")
	defer store.ShutdownTasks(ctx)
	s3 := newRound5FakeS3Server(t)
	store.Backups, err = backupstore.New(ctx, backupstore.Config{
		Bucket:          "test-backups",
		Prefix:          "backups",
		Endpoint:        s3.URL,
		Region:          "us-east-1",
		AccessKeyID:     "test",
		SecretAccessKey: "test",
		PathStyle:       true,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenant := "r6-remote-id"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "calls", FromKind: "service", ToKind: "service", Directed: true},
		},
		UpsertEntities: []graph.Entity{{ID: "h1", Kind: "host"}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}

	customBackupID := "scheduled-prior-crash-id"
	task := Task{
		ID:         "retry-task-2",
		TenantID:   tenant,
		Type:       TaskTypeTenantBackup,
		Status:     TaskStatusQueued,
		Params:     map[string]any{"destination": "object", "automatic": true},
		Checkpoint: map[string]any{"remote_backup_id": customBackupID},
		StartedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	if err := store.saveTask(ctx, task); err != nil {
		t.Fatal(err)
	}

	captured, err := store.ReplicatedObjectBackupCaptured(ctx, task)
	if err != nil || captured {
		t.Fatalf("expected captured=false before capture, got captured=%v err=%v", captured, err)
	}
	if err := store.CaptureReplicatedObjectBackup(ctx, tenant, task.ID); err != nil {
		t.Fatalf("CaptureReplicatedObjectBackup failed: %v", err)
	}
	captured, err = store.ReplicatedObjectBackupCaptured(ctx, task)
	if err != nil || !captured {
		t.Fatalf("expected captured=true after capture under checkpoint remote_backup_id, got captured=%v err=%v", captured, err)
	}
	if _, err := store.loadTenantBackupRecord(ctx, store.taskResultKey(tenant, customBackupID)); err != nil {
		t.Fatalf("expected backup record under custom checkpoint ID %q: %v", customBackupID, err)
	}

	prepared, err := store.PrepareReplicatedTask(ctx, task)
	if err != nil {
		t.Fatalf("PrepareReplicatedTask failed: %v", err)
	}
	var payload replicatedBackupPayload
	if err := json.Unmarshal(prepared, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload.Entry.BackupID != customBackupID {
		t.Fatalf("published BackupID = %q, want %q", payload.Entry.BackupID, customBackupID)
	}
	done, err := store.RunReplicatedTask(ctx, tenant, task.ID, prepared)
	if err != nil || done.Status != TaskStatusSucceeded {
		t.Fatalf("RunReplicatedTask failed: status=%q err=%q runErr=%v", done.Status, done.Error, err)
	}
}

func TestRound6TaskHeartbeatSurvivesOverwriteRestoreAndRebindsFence(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "r6-hb")
	defer store.ShutdownTasks(ctx)
	store.InstanceID = "writer-1"

	tenant := "t-hb"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	origInterval := sharedStoreTaskHeartbeatInterval
	sharedStoreTaskHeartbeatInterval = 15 * time.Millisecond
	defer func() { sharedStoreTaskHeartbeatInterval = origInterval }()

	boundCtx, err := store.acquireAndBindWriterFence(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	task := Task{
		ID:        "restore-ow-1",
		TenantID:  tenant,
		Type:      TaskTypeTenantRestore,
		Status:    TaskStatusRunning,
		OwnerID:   store.InstanceID,
		Params:    map[string]any{"overwrite": true},
		StartedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC().Add(-time.Minute),
	}
	if err := store.saveTask(boundCtx, task); err != nil {
		t.Fatal(err)
	}

	stop := store.startTaskHeartbeat(boundCtx, task)
	defer stop()

	// Simulate overwrite purge temporarily deleting the task object and advancing the lease epoch.
	if err := store.Objects.Delete(ctx, store.taskKey(tenant, task.ID)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(45 * time.Millisecond)

	// Re-create the task object with a newer writer lease epoch owned by the same instance.
	if err := store.prepareTenantCreateLease(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	newBoundCtx, err := store.rebindCurrentWriterFence(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	staleTime := time.Now().UTC().Add(-10 * time.Minute)
	task.UpdatedAt = staleTime
	if err := store.saveTask(newBoundCtx, task); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, getErr := store.GetTask(ctx, tenant, task.ID)
		if getErr == nil && current.UpdatedAt.After(staleTime) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected overwrite restore heartbeat to survive transient ErrNotFound and rebind fence epoch")
}

func TestRound6IngestWALSyncCloseOnFailedWALAndMissingSegmentsAfterCheckpoint(t *testing.T) {
	// 1. Failed async WAL must not advance .durable on syncAndClose.
	dir := t.TempDir()
	cfg := DefaultIngestWALConfig(dir)
	cfg.Durability = IngestWALDurabilityOS
	wal, err := OpenIngestWAL(cfg, func(IngestWALRecord) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	state := &ingestWALWriterState{
		wal:        wal,
		writtenLSN: 5,
		durableLSN: 2,
		segments:   []ingestWALSegment{{path: filepath.Join(dir, ingestWALSegmentName(1)), startLSN: 1, maxLSN: 5}},
		current:    0,
	}
	wal.recordError(errors.New("injected io failure"))
	if err := state.syncAndClose(); err != nil {
		t.Fatalf("syncAndClose returned unexpected error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".durable")); !os.IsNotExist(err) {
		t.Fatalf("expected failed async WAL syncAndClose not to persist .durable, stat err=%v", err)
	}
	_ = wal.Close()

	// 2. scanIngestWALWithDurability must reject checkpoint with 0 segments.
	cpDir := t.TempDir()
	if _, err := writeIngestWALCheckpoint(cpDir, ingestWALCheckpoint{Version: 1, Through: 10}, 1<<20); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = scanIngestWALWithDurability(cpDir, IngestWALDurabilitySync, nil)
	if !errors.Is(err, ErrIngestWALCorrupt) {
		t.Fatalf("expected ErrIngestWALCorrupt when checkpoint exists with no WAL segments, got %v", err)
	}
}

func TestRound6FileStoreReceiptDBConditionalCASUsesSnapshotETag(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	if err := fs.EnableLocalReceiptPacking(ctx, "graphdb"); err != nil {
		t.Fatal(err)
	}
	store := NewTenantStore(fs, "graphdb")
	defer store.ShutdownTasks(ctx)
	tenant := "r6-cas-db"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Ingest(ctx, tenant, IngestRequest{
		Source:      "s1",
		CollectorID: "c1",
		BatchID:     "b1",
		Items: []IngestItem{
			{Entity: &graph.Entity{ID: "h1", Kind: "host"}},
		},
	}); err != nil {
		t.Fatal(err)
	}

	dbKey := fs.receiptPrefix + tenant + "/" + receiptDatabase
	snapshotData, meta, err := fs.GetWithMeta(ctx, dbKey)
	if err != nil {
		t.Fatalf("GetWithMeta(%s) failed: %v", dbKey, err)
	}
	if !meta.Exists || meta.ETag == "" {
		t.Fatalf("expected existing receipt DB with non-empty ETag, got %+v", meta)
	}

	// Wrong IfMatch must fail with ErrConflict.
	if _, err := fs.PutConditional(ctx, dbKey, snapshotData, PutCondition{IfMatch: "deadbeef"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for wrong PutConditional IfMatch, got %v", err)
	}
	if err := fs.DeleteConditional(ctx, dbKey, PutCondition{IfMatch: "deadbeef"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected ErrConflict for wrong DeleteConditional IfMatch, got %v", err)
	}

	// Matching snapshot ETag must succeed for PutConditional without deadlocking.
	nextMeta, err := fs.PutConditional(ctx, dbKey, snapshotData, PutCondition{IfMatch: meta.ETag})
	if err != nil {
		t.Fatalf("PutConditional with matching receipt DB snapshot ETag failed: %v", err)
	}
	if nextMeta.ETag == "" {
		t.Fatalf("expected non-empty ETag after PutConditional")
	}

	// Matching snapshot ETag must succeed for DeleteConditional.
	if err := fs.DeleteConditional(ctx, dbKey, PutCondition{IfMatch: nextMeta.ETag}); err != nil {
		t.Fatalf("DeleteConditional with matching receipt DB snapshot ETag failed: %v", err)
	}
}

func TestRound6RepairManifestReconstructsSpanningCommitSegment(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := graph.New()
	c1 := graph.Commit{ID: "c1", Version: 1, CreatedAt: now, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "h1", Kind: "host"}}}}
	c2 := graph.Commit{ID: "c2", Version: 2, CreatedAt: now.Add(time.Second), Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "h2", Kind: "host"}}}}
	c3 := graph.Commit{ID: "c3", Version: 3, CreatedAt: now.Add(2 * time.Second), Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "h3", Kind: "host"}}}}
	c4 := graph.Commit{ID: "c4", Version: 4, CreatedAt: now.Add(3 * time.Second), Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "h4", Kind: "host"}}}}

	// Snapshot at version 2 (already includes c1 and c2).
	if err := base.ApplyCommit(c1); err != nil {
		t.Fatal(err)
	}
	if err := base.ApplyCommit(c2); err != nil {
		t.Fatal(err)
	}

	// Segment spans [1..4], overlapping snapshot version 2.
	seg := commitSegmentObject{
		Ref: CommitSegmentRef{Key: "seg-1-4.parquet", FirstVersion: 1, LastVersion: 4},
		Items: []commitSegmentItem{
			{Commit: c1},
			{Commit: c2},
			{Commit: c3},
			{Commit: c4},
		},
	}
	manifest := Manifest{Version: 2}
	reconstructed := applyReconstructedCommitTail(base, &manifest, []commitSegmentObject{seg}, nil)
	if reconstructed.Version != 4 || manifest.Version != 4 {
		t.Fatalf("expected reconstructed version 4, got graph=%d manifest=%d", reconstructed.Version, manifest.Version)
	}
	if _, ok := reconstructed.Entities.Get("h3"); !ok {
		t.Fatalf("expected h3 from spanning commit segment to be recovered")
	}
	if _, ok := reconstructed.Entities.Get("h4"); !ok {
		t.Fatalf("expected h4 from spanning commit segment to be recovered")
	}
}

func TestRound6SavedQueryParquetRoundTripsAllRequestFields(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)
	original := SavedQuery{
		TenantID:    "t1",
		Name:        "complex-template",
		Description: "covers all Request and PathFilter fields",
		CreatedAt:   now,
		UpdatedAt:   now.Add(time.Minute),
		Request: query.Request{
			Op:                "traverse",
			Kind:              "service",
			ID:                "svc:1",
			Direction:         "out",
			DirectionStrategy: "impact",
			Depth:             3,
			Limit:             50,
			MinVersion:        17,
			AllowStale:        true,
			WhereExpr: &query.FilterExpr{
				Op: "and",
				Children: []query.FilterExpr{
					{Field: "env", Op: "eq", Value: "prod"},
					{
						Op: "or",
						Children: []query.FilterExpr{
							{Field: "tier", Op: "eq", Value: float64(1)},
							{Field: "owner", Op: "exists", Value: true},
						},
					},
				},
			},
			EdgeWhere: []query.Filter{
				{Field: "weight", Op: "gte", Value: float64(2)},
			},
			EdgeWhereExpr: &query.FilterExpr{
				Op: "not",
				Children: []query.FilterExpr{
					{Field: "disabled", Op: "eq", Value: true},
				},
			},
			GroupBy: []string{"env", "tier"},
			Having: []query.Filter{
				{Field: "cnt", Op: "gt", Value: float64(1)},
			},
			HavingExpr: &query.FilterExpr{
				Field: "cnt",
				Op:    "lte",
				Value: float64(100),
			},
			Aggregate: []query.Aggregation{
				{Name: "cnt", Op: "count"},
			},
			Path: query.PathFilter{
				EndKind:  "database",
				MaxPaths: 10,
				EndWhereExpr: &query.FilterExpr{
					Field: "engine",
					Op:    "eq",
					Value: "postgres",
				},
				Steps: []query.PathStep{
					{
						Direction:     "out",
						RelationTypes: []string{"calls"},
						NodeKinds:     []string{"service"},
						Where:         []query.Filter{{Field: "active", Op: "eq", Value: true}},
						WhereExpr:     &query.FilterExpr{Field: "region", Op: "eq", Value: "us-east"},
						EdgeWhere:     []query.Filter{{Field: "protocol", Op: "eq", Value: "grpc"}},
						EdgeWhereExpr: &query.FilterExpr{Field: "tls", Op: "eq", Value: true},
					},
					{
						Direction:     "in",
						RelationTypes: []string{"backed_by"},
						NodeKinds:     []string{"database"},
					},
				},
			},
		},
	}

	raw, err := marshalParquetSavedQuery(ctx, original)
	if err != nil {
		t.Fatalf("marshalParquetSavedQuery failed: %v", err)
	}
	decoded, err := decodeParquetSavedQuery(ctx, raw)
	if err != nil {
		t.Fatalf("decodeParquetSavedQuery failed: %v", err)
	}
	if !reflect.DeepEqual(decoded, original) {
		t.Fatalf("round-trip mismatch:\ngot:  %+v\nwant: %+v", decoded, original)
	}
}

func TestRound6IngestRecordAndDeadLetterParquetRoundTripSkipReasonAndDetailsTruncated(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 2, 3, 4, 5, 6, 0, time.UTC)

	rec := IngestBatchRecord{
		TenantID: "t1",
		Request: IngestRequest{
			Source:      "src",
			CollectorID: "col",
			BatchID:     "b1",
			Items:       []IngestItem{},
		},
		Result: IngestResult{
			BatchID:          "b1",
			Version:          2,
			Applied:          1,
			Failed:           5,
			DetailsTruncated: true,
			Failures:         []IngestFailure{{Index: 0, Error: "err"}},
		},
		StartedAt:  now,
		FinishedAt: now.Add(time.Second),
	}
	recBytes, err := marshalParquetIngestRecord(ctx, rec)
	if err != nil {
		t.Fatalf("marshalParquetIngestRecord failed: %v", err)
	}
	gotRec, err := decodeParquetIngestRecord(ctx, recBytes)
	if err != nil {
		t.Fatalf("decodeParquetIngestRecord failed: %v", err)
	}
	if !gotRec.Result.DetailsTruncated {
		t.Fatalf("expected DetailsTruncated=true after ingest record round-trip")
	}

	letter := DeadLetter{
		ID:        "dl-1",
		TenantID:  "t1",
		Source:    "src",
		BatchID:   "b1",
		Status:    "pending",
		Attempts:  1,
		Error:     "failed",
		CreatedAt: now,
		UpdatedAt: now.Add(time.Second),
		Request: IngestRequest{
			Source:      "src",
			CollectorID: "col",
			BatchID:     "b1",
			Items:       []IngestItem{},
		},
		LastResult: IngestResult{
			BatchID:          "b1",
			Version:          2,
			Skipped:          true,
			SkipReason:       IngestSkipReasonLogicalNoop,
			DetailsTruncated: true,
		},
	}
	dlBytes, err := marshalParquetDeadLetter(ctx, letter)
	if err != nil {
		t.Fatalf("marshalParquetDeadLetter failed: %v", err)
	}
	gotLetter, err := decodeParquetDeadLetter(ctx, dlBytes)
	if err != nil {
		t.Fatalf("decodeParquetDeadLetter failed: %v", err)
	}
	if gotLetter.LastResult.SkipReason != IngestSkipReasonLogicalNoop || !gotLetter.LastResult.DetailsTruncated {
		t.Fatalf("deadletter LastResult mismatch: %+v", gotLetter.LastResult)
	}
}

func TestRound6AdjacencyPreservedAcrossSchemaRebuild(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	store := NewTenantStore(fs, "r6-adj")
	defer store.ShutdownTasks(ctx)
	store.WriteIndexRangePages = true
	store.WriteIndexAdjacencyPages = true

	tenant := "t-adj"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "links", FromKind: "host", ToKind: "host", Directed: true},
		},
		UpsertEntities: []graph.Entity{
			{ID: "n1", Kind: "host"},
			{ID: "n2", Kind: "host"},
		},
		UpsertEdges: []graph.Edge{
			{ID: "e1", Type: "links", From: "n1", To: "n2"},
		},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, tenant); err != nil {
		t.Fatalf("RebuildIndexes failed: %v", err)
	}
	cat1, err := store.GetIndexCatalog(ctx, tenant)
	if err != nil || !indexCatalogHasAdjacency(cat1) {
		t.Fatalf("expected initial catalog to have AdjacencyRoot, err=%v catalog=%+v", err, cat1.EdgeShards)
	}

	// Trigger a schema-change commit which forces a full rebuild without explicit adjacencyBuildKey.
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertCITypes: []graph.CIType{
			{Name: "service", Fields: map[string]graph.FieldSpec{"name": {Type: "string"}}},
		},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	// Run rebuildIndexesFromGraph directly (simulating catch-up/schema-change rebuild path).
	loaded, err := store.loadWithMeta(ctx, tenant)
	if err != nil {
		t.Fatal(err)
	}
	cat2, err := store.rebuildIndexesFromGraph(ctx, tenant, loaded.Graph, loaded.Manifest)
	if err != nil {
		t.Fatalf("rebuildIndexesFromGraph failed: %v", err)
	}
	if !indexCatalogHasAdjacency(cat2) {
		t.Fatalf("expected AdjacencyRoot to be preserved across rebuildIndexesFromGraph, got %+v", cat2.EdgeShards)
	}
	rev, err := store.GetReverseIndexCatalog(ctx, tenant, cat2.Version)
	if err != nil {
		t.Fatalf("GetReverseIndexCatalog failed: %v", err)
	}
	hasRevAdj := false
	for _, spec := range rev.EdgeShards {
		if spec.AdjacencyRoot != nil {
			hasRevAdj = true
		}
	}
	if !hasRevAdj {
		t.Fatalf("expected reverse catalog AdjacencyRoot to be preserved, got %+v", rev.EdgeShards)
	}
}

func TestRound6RangeCompactionPreservesOtherEntityRecords(t *testing.T) {
	ctx := context.Background()
	fs, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	store := NewTenantStore(fs, "r6-compact")
	defer store.ShutdownTasks(ctx)
	store.WriteEntityRecords = true
	store.WriteIndexRangePages = true
	store.WriteIndexAdjacencyPages = true

	tenant := "t-compact"
	if _, err := store.CreateTenant(ctx, tenant, TenantCreateOptions{}); err != nil {
		t.Fatal(err)
	}

	// Create entities across multiple shards so compacting one shard has unrelated entities in other shards.
	entities := make([]graph.Entity, 0, 20)
	for i := 0; i < 20; i++ {
		id := string(rune('a'+i)) + "-entity"
		entities = append(entities, graph.Entity{ID: id, Kind: "host", Fields: graph.Fields{"idx": i}})
	}
	if _, err := store.Commit(ctx, tenant, graph.Mutations{UpsertEntities: entities}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RebuildIndexes(ctx, tenant); err != nil {
		t.Fatal(err)
	}
	// Add a second commit touching one entity so its shard splits into two small range pages.
	if _, err := store.Commit(ctx, tenant, graph.Mutations{
		UpsertRelationTypes: []graph.RelationType{
			{Name: "calls", FromKind: "service", ToKind: "service", Directed: true},
		},
		UpsertEntities: []graph.Entity{{ID: "a-entity-extra", Kind: "host", Fields: graph.Fields{"idx": 99}}},
	}, CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	// Run range compaction.
	skipped := map[string]bool{}
	_, _, _, err = store.compactRangeSlice(ctx, tenant, skipped)
	if err != nil {
		t.Fatalf("compactRangeSlice failed: %v", err)
	}

	// Verify EVERY entity's EntityRecord is still alive (not tombstoned!).
	for _, e := range entities {
		rec, err := store.loadEntityRecord(ctx, tenant, e.ID)
		if err != nil {
			t.Fatalf("GetEntityRecord(%q) failed after range compaction: %v", e.ID, err)
		}
		if rec.Deleted {
			t.Fatalf("entity %q was erroneously tombstoned by range compaction", e.ID)
		}
	}
}
