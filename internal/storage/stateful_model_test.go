package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// sharedNoNotifyBackend wraps a MemoryStore without exposing OnChange so the
// stateful model test exercises shared object-store lease takeover & cache
// eviction semantics identically to remote object backends.
type sharedNoNotifyBackend struct {
	ObjectStore
}

// TestStatefulStorageModelWithFaultInjection runs randomized interleaved
// sequences of Commit, Ingest, Schema Change, Compact + Range Compaction,
// checkpointed GC (with random cursor pauses/resumes), Backup + RestoreDrill,
// Manifest corruption + Repair, and shared-store WriterLease takeover. After
// every step it verifies exact equivalence with a pure in-memory reference
// model and deep integrity audit cleanliness.
func TestStatefulStorageModelWithFaultInjection(t *testing.T) {
	modes := []struct {
		name      string
		newObject func(t *testing.T) ObjectStore
	}{
		{
			name: "shared_no_notify_memory",
			newObject: func(t *testing.T) ObjectStore {
				return &sharedNoNotifyBackend{ObjectStore: NewMemoryStore()}
			},
		},
		{
			name: "local_file_store",
			newObject: func(t *testing.T) ObjectStore {
				return NewFileStore(t.TempDir())
			},
		},
	}

	for _, mode := range modes {
		for _, seed := range []int64{1, 19} {
			t.Run(fmt.Sprintf("%s/seed_%d", mode.name, seed), func(t *testing.T) {
				runStatefulStorageModelSeed(t, mode.newObject(t), seed)
			})
		}
	}
}

func runStatefulStorageModelSeed(t *testing.T, objects ObjectStore, seed int64) {
	t.Helper()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(seed))
	const tenant = "tenant-model"

	newStore := func(instanceID string) *TenantStore {
		s := NewTenantStore(objects, "model")
		s.InstanceID = instanceID
		s.WriteIndexRangePages = true
		s.WriteIndexAdjacencyPages = true
		s.WriteEntityRecords = true
		s.UseEntityRecordsForRead = true
		return s
	}

	storeA := newStore("writer-a")
	defer storeA.ShutdownTasks(ctx)
	storeB := newStore("writer-b")
	defer storeB.ShutdownTasks(ctx)

	if _, err := storeA.CreateTenant(ctx, tenant, TenantCreateOptions{Name: "Stateful Model"}); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}

	// Reference model tracking live entities and edges.
	refEntities := map[string]graph.Entity{}
	refEdges := map[string]graph.Edge{}
	nextEntitySeq := 0
	nextEdgeSeq := 0
	schemaVersion := 0
	hasSnapshot := false

	// Initial schema + seed entities + initial compact + explicit index rebuild
	// so range pages, adjacency roots, entity records, and snapshot catalog exist.
	seedMutations := graph.Mutations{
		UpsertCITypes: []graph.CIType{
			{
				Name: "service",
				Fields: map[string]graph.FieldSpec{
					"env":   {Type: "string", Indexed: true},
					"score": {Type: "number", Indexed: true},
				},
			},
		},
		UpsertRelationTypes: []graph.RelationType{
			{Name: "calls", Directed: true, FromKind: "service", ToKind: "service", ImpactDirection: "forward"},
		},
		UpsertEntities: []graph.Entity{
			{ID: "service:0", Kind: "service", Fields: graph.Fields{"env": "prod", "score": 10}},
			{ID: "service:1", Kind: "service", Fields: graph.Fields{"env": "stage", "score": 20}},
		},
	}
	if _, err := storeA.Commit(ctx, tenant, seedMutations, CommitOptions{}); err != nil {
		t.Fatalf("seed Commit: %v", err)
	}
	for _, e := range seedMutations.UpsertEntities {
		refEntities[e.ID] = e
	}
	nextEntitySeq = 2
	if _, err := storeA.Compact(ctx, tenant); err != nil {
		t.Fatalf("seed Compact: %v", err)
	}
	hasSnapshot = true
	if _, err := storeA.RebuildIndexes(ctx, tenant); err != nil {
		t.Fatalf("seed RebuildIndexes: %v", err)
	}

	activeStore := storeA

	assertModelAndAudit := func(step int, op string, requireDeepAudit bool) {
		t.Helper()
		g, _, err := activeStore.Load(ctx, tenant)
		if err != nil {
			t.Fatalf("step %d (%s) Load: %v", step, op, err)
		}
		if g.Entities.Len() != len(refEntities) {
			t.Fatalf("step %d (%s) entity count = %d, want %d", step, op, g.Entities.Len(), len(refEntities))
		}
		for id, want := range refEntities {
			got, ok := g.GetEntity(id)
			if !ok {
				t.Fatalf("step %d (%s) missing entity %q", step, op, id)
			}
			if got.Kind != want.Kind || fmt.Sprint(got.Fields["env"]) != fmt.Sprint(want.Fields["env"]) || fmt.Sprint(got.Fields["score"]) != fmt.Sprint(want.Fields["score"]) {
				t.Fatalf("step %d (%s) entity %q mismatch: got=%+v want=%+v", step, op, id, got.Fields, want.Fields)
			}
		}
		if g.Edges.Len() != len(refEdges) {
			t.Fatalf("step %d (%s) edge count = %d, want %d", step, op, g.Edges.Len(), len(refEdges))
		}
		for _, want := range refEdges {
			matched := false
			for _, got := range g.Edges.All() {
				if got.Type == want.Type && got.From == want.From && got.To == want.To {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("step %d (%s) missing edge %s -[%s]-> %s", step, op, want.From, want.Type, want.To)
			}
		}
		if requireDeepAudit && hasSnapshot {
			report, err := activeStore.AuditIntegrity(ctx, tenant, IntegrityAuditOptions{Deep: true})
			if err != nil {
				t.Fatalf("step %d (%s) AuditIntegrity error: %v", step, op, err)
			}
			if report.Status != "ok" || len(report.Issues) > 0 {
				t.Fatalf("step %d (%s) AuditIntegrity failed: status=%s issues=%+v", step, op, report.Status, report.Issues)
			}
		}
	}

	assertModelAndAudit(0, "seed", true)

	envs := []string{"prod", "stage", "dev"}
	const totalSteps = 22
	for step := 1; step <= totalSteps; step++ {
		// Ensure every operation class is exercised deterministically as well as randomly.
		action := rng.Intn(8)
		switch step {
		case 3:
			action = 2 // Schema change
		case 6:
			action = 3 // Compact + Range compaction
		case 9:
			action = 4 // Checkpointed GC
		case 12:
			action = 5 // Backup + RestoreDrill
		case 15:
			action = 6 // Manifest corruption + Repair
		case 18:
			action = 7 // Writer lease takeover
		}

		switch action {
		case 0: // Commit upsert/delete entities & edges
			id := fmt.Sprintf("service:%d", nextEntitySeq)
			nextEntitySeq++
			ent := graph.Entity{
				ID:     id,
				Kind:   "service",
				Fields: graph.Fields{"env": envs[rng.Intn(len(envs))], "score": rng.Intn(100)},
			}
			muts := graph.Mutations{UpsertEntities: []graph.Entity{ent}}
			existingIDs := sortedKeys(refEntities)
			var addedEdge *graph.Edge
			if len(existingIDs) > 0 {
				target := existingIDs[rng.Intn(len(existingIDs))]
				edge := graph.Edge{
					ID:   fmt.Sprintf("e:%d", nextEdgeSeq),
					Type: "calls",
					From: id,
					To:   target,
				}
				nextEdgeSeq++
				muts.UpsertEdges = []graph.Edge{edge}
				addedEdge = &edge
			}
			if _, err := activeStore.Commit(ctx, tenant, muts, CommitOptions{}); err != nil {
				t.Fatalf("step %d Commit: %v", step, err)
			}
			refEntities[ent.ID] = ent
			if addedEdge != nil {
				refEdges[addedEdge.From+"->"+addedEdge.To] = *addedEdge
			}
			assertModelAndAudit(step, "commit", true)

		case 1: // IngestBatch + idempotent replay
			id := fmt.Sprintf("service:%d", nextEntitySeq)
			nextEntitySeq++
			ent := graph.Entity{
				ID:     id,
				Kind:   "service",
				Fields: graph.Fields{"env": envs[rng.Intn(len(envs))], "score": rng.Intn(100)},
			}
			req := IngestRequest{
				Source:         "model-collector",
				CollectorID:    "c1",
				BatchID:        fmt.Sprintf("batch-%d", step),
				IdempotencyKey: fmt.Sprintf("idem-%d", step),
				Items: []IngestItem{
					{ExternalID: id, Entity: &ent},
				},
			}
			res, err := activeStore.Ingest(ctx, tenant, req)
			if err != nil || res.Failed > 0 {
				t.Fatalf("step %d Ingest: res=%+v err=%v", step, res, err)
			}
			// Replay with identical idempotency key must be a no-op replay.
			replay, err := activeStore.Ingest(ctx, tenant, req)
			if err != nil || !replay.Skipped {
				t.Fatalf("step %d Ingest replay: res=%+v err=%v", step, replay, err)
			}
			refEntities[ent.ID] = ent
			assertModelAndAudit(step, "ingest", true)

		case 2: // Schema change (triggers index rebuild and must retain adjacency)
			schemaVersion++
			kindName := fmt.Sprintf("extra_kind_%d", schemaVersion)
			muts := graph.Mutations{
				UpsertCITypes: []graph.CIType{
					{
						Name: kindName,
						Fields: map[string]graph.FieldSpec{
							"env":   {Type: "string", Indexed: true},
							"score": {Type: "number"},
						},
					},
				},
			}
			if _, err := activeStore.Commit(ctx, tenant, muts, CommitOptions{}); err != nil {
				t.Fatalf("step %d schema Commit: %v", step, err)
			}
			if _, err := activeStore.RebuildIndexes(ctx, tenant); err != nil {
				t.Fatalf("step %d RebuildIndexes after schema change: %v", step, err)
			}
			assertModelAndAudit(step, "schema_change", true)

		case 3: // Compact + explicit range compaction slice
			if _, err := activeStore.Compact(ctx, tenant); err != nil {
				t.Fatalf("step %d Compact: %v", step, err)
			}
			hasSnapshot = true
			if _, _, _, err := activeStore.compactRangeSlice(ctx, tenant, map[string]bool{}); err != nil && !errors.Is(err, ErrConflict) {
				t.Fatalf("step %d compactRangeSlice: %v", step, err)
			}
			assertModelAndAudit(step, "compact_and_range_slice", true)

		case 4: // Checkpointed GC with random MaxKeys pauses & resumes
			cursor := ""
			for pass := 0; pass < 16; pass++ {
				gcReport, err := activeStore.RunGC(ctx, tenant, GCOptions{
					KeepSnapshots:       1,
					CleanupIndexOrphans: true,
					MaxDeletes:          1 + rng.Intn(4),
					CheckpointCursor:    cursor,
				})
				if err != nil {
					t.Fatalf("step %d GC pass %d: %v", step, pass, err)
				}
				if gcReport.Checkpoint.Completed || gcReport.Checkpoint.NextCursor == "" {
					break
				}
				cursor = gcReport.Checkpoint.NextCursor
			}
			assertModelAndAudit(step, "gc_checkpointed", true)

		case 5: // Backup + RestoreDrill verification
			backupTask, err := activeStore.StartTask(ctx, tenant, TaskTypeTenantBackup, nil)
			if err != nil {
				t.Fatalf("step %d StartTask backup: %v", step, err)
			}
			doneBackup, err := waitTaskTerminal(ctx, activeStore, tenant, backupTask.ID)
			if err != nil || doneBackup.Status != TaskStatusSucceeded {
				t.Fatalf("step %d backup task failed: task=%+v err=%v", step, doneBackup, err)
			}
			backupKey, _ := doneBackup.Result["backup_manifest_key"].(string)
			if backupKey == "" {
				backupKey = doneBackup.ResultKey
			}
			drillTask, err := activeStore.StartTask(ctx, tenant, TaskTypeTenantRestoreDrill, map[string]any{
				"backup_key": backupKey,
				"cleanup":    true,
			})
			if err != nil {
				t.Fatalf("step %d StartTask restore_drill: %v", step, err)
			}
			doneDrill, err := waitTaskTerminal(ctx, activeStore, tenant, drillTask.ID)
			if err != nil || doneDrill.Status != TaskStatusSucceeded {
				t.Fatalf("step %d restore_drill task failed: task=%+v err=%v", step, doneDrill, err)
			}
			if recoverable, _ := doneDrill.Result["recoverable"].(bool); !recoverable {
				t.Fatalf("step %d restore_drill not recoverable: %+v", step, doneDrill.Result)
			}
			assertModelAndAudit(step, "backup_restore_drill", true)

		case 6: // Corrupt manifest object -> Repair -> RebuildIndexes -> Deep Audit
			manifestKey := activeStore.manifestKey(tenant)
			if err := objects.Put(ctx, manifestKey, []byte("corrupted-manifest-bytes")); err != nil {
				t.Fatalf("step %d corrupt manifest: %v", step, err)
			}
			activeStore.InvalidateTenantCaches(tenant)
			if _, err := activeStore.repairManifest(ctx, tenant); err != nil {
				t.Fatalf("step %d repairManifest: %v", step, err)
			}
			if _, err := activeStore.RebuildIndexes(ctx, tenant); err != nil {
				t.Fatalf("step %d RebuildIndexes after repair: %v", step, err)
			}
			assertModelAndAudit(step, "manifest_corrupt_repair", true)

		case 7: // Shared-store WriterLease takeover between storeA and storeB
			other := storeB
			if activeStore == storeB {
				other = storeA
			}
			// `other` takes over the tenant lease (advancing FenceEpoch) and commits.
			id := fmt.Sprintf("service:%d", nextEntitySeq)
			nextEntitySeq++
			ent := graph.Entity{
				ID:     id,
				Kind:   "service",
				Fields: graph.Fields{"env": "prod", "score": 99},
			}
			if _, err := other.Commit(ctx, tenant, graph.Mutations{UpsertEntities: []graph.Entity{ent}}, CommitOptions{}); err != nil {
				t.Fatalf("step %d takeover Commit by %s: %v", step, other.InstanceID, err)
			}
			refEntities[ent.ID] = ent

			// If `activeStore` still had a stale cached lease (on shared no-notify backend),
			// attempting a commit with its bound old fence must fail and atomically evict
			// the stale cache entry so a subsequent takeover cleanly observes latest state.
			if cachedLease, _, hadCached := activeStore.getCachedWriterLease(tenant); hadCached {
				err := activeStore.writerFenceStillCurrent(ctx, tenant, writerFenceRef{
					ownerID: cachedLease.OwnerID,
					token:   cachedLease.FenceToken,
					epoch:   cachedLease.FenceEpoch,
				})
				if !errors.Is(err, ErrLeaseHeld) {
					t.Fatalf("step %d expected ErrLeaseHeld for stale owner %s, got %v", step, activeStore.InstanceID, err)
				}
				if _, _, stillCached := activeStore.getCachedWriterLease(tenant); stillCached {
					t.Fatalf("step %d stale cached lease was not atomically evicted for %s", step, activeStore.InstanceID)
				}
			}
			activeStore = other
			assertModelAndAudit(step, "lease_takeover", true)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func waitTaskTerminal(ctx context.Context, s *TenantStore, tenantID, taskID string) (Task, error) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		task, err := s.GetTask(ctx, tenantID, taskID)
		if err != nil {
			return Task{}, err
		}
		if taskTerminal(task.Status) {
			return task, nil
		}
		time.Sleep(15 * time.Millisecond)
	}
	return Task{}, fmt.Errorf("task %s timed out waiting for completion", taskID)
}
