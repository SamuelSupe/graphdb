package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

type casStressBatch struct {
	start int
	end   int
}

func TestPostgresCoordinatorS3CASStress(t *testing.T) {
	rawDuration := os.Getenv("GRAPHDB_TEST_CAS_STRESS_DURATION")
	if rawDuration == "" {
		t.Skip("set GRAPHDB_TEST_CAS_STRESS_DURATION=30m to run the multi-writer soak")
	}
	duration, err := time.ParseDuration(rawDuration)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid GRAPHDB_TEST_CAS_STRESS_DURATION %q", rawDuration)
	}
	qps := stressPositiveInt(t, "GRAPHDB_TEST_CAS_STRESS_QPS", 20)
	writerCount := stressPositiveInt(t, "GRAPHDB_TEST_CAS_STRESS_WRITERS", 2)
	batchSize := stressPositiveInt(t, "GRAPHDB_TEST_CAS_STRESS_BATCH_SIZE", 10)
	compactEvery := stressPositiveInt(t, "GRAPHDB_TEST_CAS_STRESS_COMPACT_EVERY", 1000)
	clientRetries := stressPositiveInt(t, "GRAPHDB_TEST_CAS_STRESS_CLIENT_RETRIES", 64)
	targetCommits := int(duration * time.Duration(qps) / time.Second)
	if targetCommits < 1 {
		targetCommits = 1
	}
	targetBatches := (targetCommits + batchSize - 1) / batchSize

	fixtureTimeout := duration + 10*time.Minute
	fixture := newPostgresS3Fixture(t, "s3-stress", fixtureTimeout)
	t.Logf(
		"PostgreSQL/S3 CAS stress start: writers=%d target_commits=%d target_batches=%d batch_size=%d qps=%d duration=%s fixture_timeout=%s",
		writerCount, targetCommits, targetBatches, batchSize, qps, duration, fixtureTimeout,
	)
	writers := make([]*TenantStore, writerCount)
	for writerID := range writers {
		writers[writerID] = fixture.newWriter(t, writerID)
	}
	maintenanceCtx, stopMaintenance := context.WithCancel(fixture.ctx)
	defer stopMaintenance()
	for _, writer := range writers {
		writer.StartCoordinatorMaintenance(maintenanceCtx, 250*time.Millisecond)
	}
	jobs := make(chan casStressBatch, targetBatches)
	errs := make(chan error, targetCommits)
	committedVersions := make([]int64, targetCommits)
	var completed atomic.Int64
	var publishes atomic.Int64
	var writeConflicts atomic.Int64
	var retryConflicts atomic.Int64
	var taskLeaseRetries atomic.Int64
	var casConflicts atomic.Int64
	var compactRetries atomic.Int64
	var compactions atomic.Int64
	writerCommitted := make([]atomic.Int64, writerCount)
	reportWritten := false
	defer func() {
		if reportWritten || os.Getenv("GRAPHDB_TEST_CAS_STRESS_REPORT") == "" {
			return
		}
		writerCommittedReport := make([]int64, len(writerCommitted))
		for writerID := range writerCommitted {
			writerCommittedReport[writerID] = writerCommitted[writerID].Load()
		}
		writeCASStressReport(t, casStressReport{
			SchemaVersion:    2,
			Success:          false,
			Writers:          writerCount,
			TargetQPS:        qps,
			Duration:         duration.String(),
			TargetCommits:    targetCommits,
			BatchSize:        batchSize,
			TargetBatches:    targetBatches,
			Committed:        int(completed.Load()),
			WriterCommitted:  writerCommittedReport,
			Publishes:        publishes.Load(),
			Compactions:      compactions.Load(),
			WriteConflicts:   writeConflicts.Load(),
			RetryConflicts:   retryConflicts.Load(),
			TaskLeaseRetries: taskLeaseRetries.Load(),
			CASConflicts:     casConflicts.Load(),
			CompactRetries:   compactRetries.Load(),
		})
	}()
	var wg sync.WaitGroup
	compactSignals := make(chan struct{}, 1)
	compactErr := make(chan error, 1)
	compactor := fixture.newWriter(t, writerCount)
	var compactWG sync.WaitGroup
	compactWG.Add(1)
	go func() {
		defer compactWG.Done()
		var lastSnapshot int64
		for range compactSignals {
			for {
				var manifest Manifest
				for compactAttempt := 0; ; compactAttempt++ {
					var err error
					manifest, err = compactor.Compact(fixture.ctx, "tenant-a")
					if err == nil {
						break
					}
					if !stressRetryableCASConflict(err) || compactAttempt >= clientRetries {
						compactErr <- fmt.Errorf("compact through writer %d: %w", writerCount, err)
						return
					}
					compactRetries.Add(1)
					if err := stressClientRetryDelay(fixture.ctx, compactAttempt); err != nil {
						compactErr <- fmt.Errorf("retry compact through writer %d: %w", writerCount, err)
						return
					}
				}
				if manifest.SnapshotVersion > lastSnapshot {
					lastSnapshot = manifest.SnapshotVersion
					compactions.Add(1)
				}
				if completed.Load()-manifest.SnapshotVersion < int64(compactEvery) {
					break
				}
			}
		}
	}()
	for writerID := range writers {
		writerID := writerID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				entries := make([]IngestBatchEntry, batch.end-batch.start)
				for index, sequence := 0, batch.start; sequence < batch.end; index, sequence = index+1, sequence+1 {
					entries[index] = IngestBatchEntry{Request: IngestRequest{
						Source:         "cas-stress",
						CollectorID:    fmt.Sprintf("collector-%d", writerID),
						BatchID:        fmt.Sprintf("stress-%d", sequence),
						IdempotencyKey: fmt.Sprintf("stress-%d", sequence),
						Cursor:         fmt.Sprintf("cursor-%d", sequence),
						Items: []IngestItem{{
							ExternalID: fmt.Sprintf("sample:%d", sequence),
							Entity: &graph.Entity{
								ID:     fmt.Sprintf("sample:%d", sequence),
								Kind:   "sample",
								Fields: graph.Fields{"writer": writerID, "sequence": sequence},
							},
						}},
					}}
				}
				for clientAttempt := 0; ; clientAttempt++ {
					attemptWriterID := writerID
					results, err := writers[attemptWriterID].IngestDurableBatchWithHooks(
						fixture.ctx,
						"tenant-a",
						entries,
						IngestBatchHooks{Published: func() { publishes.Add(1) }},
					)
					if err == nil {
						if len(results) != len(entries) {
							errs <- fmt.Errorf("batch [%d,%d) through writer %d returned %d results, want %d", batch.start, batch.end, attemptWriterID, len(results), len(entries))
							break
						}
						for index, result := range results {
							committedVersions[batch.start+index] = result.Version
						}
						writerCommitted[writerID].Add(int64(len(entries)))
						count := completed.Add(int64(len(entries)))
						previous := count - int64(len(entries))
						if count/int64(compactEvery) > previous/int64(compactEvery) {
							select {
							case compactSignals <- struct{}{}:
							default:
							}
						}
						break
					}
					if !stressRetryableCASConflict(err) || clientAttempt >= clientRetries {
						errs <- fmt.Errorf("batch [%d,%d) through writer %d: %w", batch.start, batch.end, attemptWriterID, err)
						break
					}
					retryConflicts.Add(1)
					switch {
					case errors.Is(err, ErrTaskLeaseHeld):
						taskLeaseRetries.Add(1)
					case errors.Is(err, ErrWriteConflict):
						writeConflicts.Add(1)
					case errors.Is(err, ErrConflict):
						casConflicts.Add(1)
					}
					if err := stressClientRetryDelay(fixture.ctx, clientAttempt); err != nil {
						errs <- fmt.Errorf("retry batch [%d,%d) through writer %d: %w", batch.start, batch.end, attemptWriterID, err)
						break
					}
				}
			}
		}()
	}

	started := time.Now()
	interval := time.Second * time.Duration(batchSize) / time.Duration(qps)
	if interval <= 0 {
		interval = time.Nanosecond
	}
	progressEvery := qps * 5 * 60
	ticker := time.NewTicker(interval)
	for batchStart := 0; batchStart < targetCommits; batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > targetCommits {
			batchEnd = targetCommits
		}
		select {
		case jobs <- casStressBatch{start: batchStart, end: batchEnd}:
		case <-fixture.ctx.Done():
			t.Fatalf(
				"schedule stress batches: scheduled=%d completed=%d queued=%d: %v",
				batchStart/batchSize, completed.Load(), len(jobs), fixture.ctx.Err(),
			)
		}
		scheduled := batchEnd
		if progressEvery > 0 && scheduled%progressEvery == 0 {
			t.Logf(
				"PostgreSQL/S3 CAS stress progress: scheduled=%d completed=%d queued=%d elapsed=%s",
				scheduled, completed.Load(), len(jobs), time.Since(started).Round(time.Second),
			)
		}
		if batchEnd < targetCommits {
			select {
			case <-ticker.C:
			case <-fixture.ctx.Done():
				t.Fatalf(
					"pace stress batches: scheduled=%d completed=%d queued=%d: %v",
					batchEnd, completed.Load(), len(jobs), fixture.ctx.Err(),
				)
			}
		}
	}
	ticker.Stop()
	close(jobs)
	wg.Wait()
	commitElapsed := time.Since(started)
	close(compactSignals)
	compactWG.Wait()
	close(errs)
	terminalErrors := 0
	for err := range errs {
		if terminalErrors < 10 {
			t.Error(err)
		}
		terminalErrors++
	}
	if terminalErrors > 10 {
		t.Errorf("%d additional terminal batch errors omitted", terminalErrors-10)
	}
	select {
	case err := <-compactErr:
		t.Errorf("background compact: %v", err)
	default:
	}
	if t.Failed() {
		return
	}

	finalManifest, err := compactor.Compact(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("final compact: %v", err)
	}
	if finalManifest.SnapshotVersion != int64(targetCommits) ||
		manifestCommitTailLength(finalManifest) != 0 {
		t.Fatalf(
			"final compact snapshot/tail = %d/%d, want %d/0",
			finalManifest.SnapshotVersion,
			manifestCommitTailLength(finalManifest),
			targetCommits,
		)
	}
	committed := int(completed.Load())
	if committed != targetCommits {
		t.Fatalf("completed commits = %d, want %d", committed, targetCommits)
	}
	writerCommittedReport := make([]int64, len(writerCommitted))
	for writerID := range writerCommitted {
		writerCommittedReport[writerID] = writerCommitted[writerID].Load()
	}
	if targetBatches >= writerCount {
		for writerID, count := range writerCommittedReport {
			if count == 0 {
				t.Fatalf("writer %d completed no logical requests despite %d scheduled batches", writerID, targetBatches)
			}
		}
	}
	if publishes.Load() != int64(targetBatches) {
		t.Fatalf("successful batch publishes = %d, want %d", publishes.Load(), targetBatches)
	}
	seenVersions := make([]bool, targetCommits+1)
	for sequence, version := range committedVersions {
		if version < 1 || version > int64(targetCommits) {
			t.Fatalf("commit %d returned graph version %d outside [1,%d]", sequence, version, targetCommits)
		}
		if seenVersions[version] {
			t.Fatalf("duplicate graph version %d returned by successful commits", version)
		}
		seenVersions[version] = true
	}
	head, exists, err := fixture.coordinator.Head(fixture.ctx, "tenant-a")
	if err != nil || !exists {
		t.Fatalf("load stress head exists=%v err=%v", exists, err)
	}
	if head.GraphVersion != int64(targetCommits) || head.Revision < int64(targetBatches) {
		t.Fatalf("stress head version/revision = %d/%d, want version %d and revision >= %d batch publishes",
			head.GraphVersion, head.Revision, targetCommits, targetBatches)
	}
	graphData, manifest, err := writers[0].Load(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load stress graph: %v", err)
	}
	if manifest.Version != int64(targetCommits) || len(graphData.Entities) != targetCommits {
		t.Fatalf("stress graph version/entities = %d/%d, want %d/%d",
			manifest.Version, len(graphData.Entities), targetCommits, targetCommits)
	}
	maintenanceStarted := time.Now()
	maintenanceStatus := waitForCASMaintenance(t, fixture, 5*time.Minute)
	maintenanceElapsed := time.Since(maintenanceStarted)
	legacy := NewTenantStore(fixture.objects, fixture.prefix)
	legacyGraph, legacyManifest, err := legacy.Load(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load stress graph through 1.0-compatible manifest: %v", err)
	}
	if legacyManifest.Version != int64(targetCommits) ||
		len(legacyGraph.Entities) != targetCommits {
		t.Fatalf(
			"legacy graph version/entities = %d/%d, want %d/%d",
			legacyManifest.Version, len(legacyGraph.Entities), targetCommits, targetCommits,
		)
	}
	indexCatalog, err := writers[0].GetIndexCatalog(fixture.ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load derived index catalog: %v", err)
	}
	if indexCatalog.Version != int64(targetCommits) {
		t.Fatalf("derived index catalog version = %d, want %d", indexCatalog.Version, targetCommits)
	}
	legacyV1Validated := verifyCASStressWithV1Binary(
		t, fixture, targetCommits,
	)
	throughput := float64(committed) / commitElapsed.Seconds()
	minimumThroughput := float64(qps) * 0.9
	reportWritten = true
	writeCASStressReport(t, casStressReport{
		SchemaVersion:       2,
		Success:             throughput >= minimumThroughput,
		Writers:             writerCount,
		TargetQPS:           qps,
		Duration:            duration.String(),
		TargetCommits:       targetCommits,
		BatchSize:           batchSize,
		TargetBatches:       targetBatches,
		Committed:           committed,
		WriterCommitted:     writerCommittedReport,
		Publishes:           publishes.Load(),
		Compactions:         compactions.Load(),
		WriteConflicts:      writeConflicts.Load(),
		RetryConflicts:      retryConflicts.Load(),
		TaskLeaseRetries:    taskLeaseRetries.Load(),
		CASConflicts:        casConflicts.Load(),
		CompactRetries:      compactRetries.Load(),
		ElapsedMS:           commitElapsed.Milliseconds(),
		Throughput:          throughput,
		GraphVersion:        head.GraphVersion,
		HeadRevision:        head.Revision,
		Entities:            len(graphData.Entities),
		SnapshotVersion:     finalManifest.SnapshotVersion,
		CommitTailLength:    manifestCommitTailLength(finalManifest),
		MaintenanceDrainMS:  maintenanceElapsed.Milliseconds(),
		LegacyManifest:      legacyManifest.Version,
		LegacyEntities:      len(legacyGraph.Entities),
		IndexCatalogVersion: indexCatalog.Version,
		LegacyMirrorLag:     maintenanceStatus.MaxMirrorLag,
		LegacyOutboxBacklog: maintenanceStatus.OutboxBacklog,
		DerivedTaskBacklog:  maintenanceStatus.DerivedBacklog,
		LegacyV1Tag:         os.Getenv("GRAPHDB_TEST_V1_TAG"),
		LegacyV1Validated:   legacyV1Validated,
	})
	t.Logf(
		"PostgreSQL/S3 CAS stress: writers=%d commits=%d batches=%d batch_size=%d writer_committed=%v compactions=%d compact_retries=%d retry_conflicts=%d task_lease_retries=%d write_conflicts=%d cas_conflicts=%d client_retry_limit=%d elapsed=%s throughput=%.2f commits/s",
		writerCount, committed, publishes.Load(), batchSize, writerCommittedReport, compactions.Load(), compactRetries.Load(), retryConflicts.Load(), taskLeaseRetries.Load(), writeConflicts.Load(), casConflicts.Load(), clientRetries, commitElapsed.Round(time.Millisecond), throughput,
	)
	if throughput < minimumThroughput {
		t.Fatalf("commit throughput %.2f/s is below 90%% of target %d/s", throughput, qps)
	}
}

func verifyCASStressWithV1Binary(
	t *testing.T,
	fixture *postgresS3Fixture,
	version int,
) bool {
	t.Helper()
	binary := os.Getenv("GRAPHDB_TEST_V1_BINARY")
	if binary == "" {
		return false
	}
	queryPath := filepath.Join(t.TempDir(), "v1-query.json")
	query := `{"op":"match","kind":"sample","limit":1}`
	if err := os.WriteFile(queryPath, []byte(query), 0o600); err != nil {
		t.Fatalf("write v1 compatibility query: %v", err)
	}
	command := exec.CommandContext(
		fixture.ctx, binary, "query", "tenant-a", queryPath,
	)
	command.Env = append(os.Environ(),
		"GRAPHDB_STORAGE=s3",
		"GRAPHDB_MODE=reader",
		"GRAPHDB_COORDINATION=local",
		"GRAPHDB_PREFIX="+fixture.prefix,
		"GRAPHDB_INSTANCE_ID=cas-stress-v1-reader",
		"S3_PROVIDER="+ObjectProviderGenericS3,
		"GRAPHDB_WRITER_TOPOLOGY="+WriterTopologyCAS,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("v1.0 binary read of mirrored manifest: %v\n%s", err, output)
	}
	var response struct {
		Version int64 `json:"version"`
		Results []struct {
			Entity *graph.Entity `json:"entity"`
		} `json:"results"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		t.Fatalf("decode v1.0 binary query response: %v\n%s", err, output)
	}
	if response.Version != int64(version) ||
		len(response.Results) != 1 ||
		response.Results[0].Entity == nil ||
		response.Results[0].Entity.Kind != "sample" {
		t.Fatalf(
			"v1.0 binary response version/results = %d/%#v, want version %d and one sample",
			response.Version, response.Results, version,
		)
	}
	return true
}

func waitForCASMaintenance(
	t *testing.T,
	fixture *postgresS3Fixture,
	timeout time.Duration,
) CoordinatorStatus {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last CoordinatorStatus
	for time.Now().Before(deadline) {
		status, err := fixture.coordinator.Status(fixture.ctx)
		if err != nil {
			t.Fatalf("load coordinator maintenance status: %v", err)
		}
		last = status
		if status.MaxMirrorLag == 0 &&
			status.OutboxBacklog == 0 &&
			status.DerivedBacklog == 0 {
			return status
		}
		select {
		case <-fixture.ctx.Done():
			t.Fatalf("wait for coordinator maintenance: %v", fixture.ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Fatalf(
		"coordinator maintenance did not drain within %s: mirror_lag=%d outbox=%d derived=%d",
		timeout, last.MaxMirrorLag, last.OutboxBacklog, last.DerivedBacklog,
	)
	return last
}

func stressClientRetryDelay(ctx context.Context, attempt int) error {
	ceiling := 100 * time.Millisecond
	for i := 0; i < attempt && ceiling < 2*time.Second; i++ {
		ceiling *= 2
		if ceiling > 2*time.Second {
			ceiling = 2 * time.Second
		}
	}
	floor := ceiling / 2
	delay := floor + time.Duration(rand.Int64N(int64(ceiling-floor)+1))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func stressRetryableCASConflict(err error) bool {
	return errors.Is(err, ErrTaskLeaseHeld) ||
		errors.Is(err, ErrWriteConflict) ||
		errors.Is(err, ErrConflict)
}

func stressPositiveInt(t *testing.T, key string, fallback int) int {
	t.Helper()
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		t.Fatalf("%s must be a positive integer", key)
	}
	return value
}
