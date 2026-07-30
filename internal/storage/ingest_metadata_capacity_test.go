package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestIngestMetadataCapacityRustFS(t *testing.T) {
	if os.Getenv("GRAPHDB_INGEST_METADATA_CAPACITY") != "1" {
		t.Skip("set GRAPHDB_INGEST_METADATA_CAPACITY=1 to run the RustFS WAL metadata capacity matrix")
	}
	pathStyle, err := envBool("S3_PATH_STYLE")
	if err != nil {
		t.Fatal(err)
	}
	s3, err := NewS3StoreWithOptions(
		os.Getenv("S3_ENDPOINT"),
		os.Getenv("S3_BUCKET"),
		envOr("S3_REGION", "us-east-1"),
		envOr("S3_ACCESS_KEY_ID", os.Getenv("AWS_ACCESS_KEY_ID")),
		envOr("S3_SECRET_ACCESS_KEY", os.Getenv("AWS_SECRET_ACCESS_KEY")),
		S3Options{PathStyle: pathStyle},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := s3.Probe(context.Background()); err != nil {
		t.Fatalf("probe RustFS: %v", err)
	}
	objects := &capacityCountingStore{ObjectStore: s3}
	runID := time.Now().UnixNano()
	cases := []struct {
		name       string
		tenants    int
		perTenant  int
		maxPutRate float64
	}{
		{name: "dense", tenants: 100, perTenant: 4, maxPutRate: 0.55},
		{name: "sparse", tenants: 1000, perTenant: 1},
		{name: "burst", tenants: 1, perTenant: 256, maxPutRate: 0.02},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			runtime.GC()
			var before runtime.MemStats
			runtime.ReadMemStats(&before)

			prefix := fmt.Sprintf("graphdb-ingest-metadata-capacity-%d-%s", runID, test.name)
			store := NewTenantStore(objects, prefix)
			store.IngestMetadataMode = IngestMetadataModeSegment
			walConfig := DefaultIngestWALConfig(t.TempDir())
			wal, recoveredAtOpen, err := OpenIngestWAL(walConfig)
			if err != nil {
				t.Fatal(err)
			}
			if len(recoveredAtOpen) != 0 {
				t.Fatalf("new capacity WAL recovered %d records", len(recoveredAtOpen))
			}
			walClosed := false
			defer func() {
				if !walClosed {
					_ = wal.Close()
				}
			}()

			stopWALSampler, walHighWater := sampleDirectoryHighWater(walConfig.Dir)
			defer stopWALSampler()
			beforePuts := objects.metadataPuts(prefix)
			records, durableDurations := capacityAppendAccepted(t, wal, test.name, test.tenants, test.perTenant)
			committedDurations := capacityPublishMetadata(t, wal, store, records)
			stopWALSampler()
			if err := wal.Close(); err != nil {
				t.Fatal(err)
			}
			walClosed = true

			recoveryStarted := time.Now()
			recoveredWAL, replayRecords, err := OpenIngestWAL(walConfig)
			recoveryDuration := time.Since(recoveryStarted)
			if err != nil {
				t.Fatalf("recover capacity WAL: %v", err)
			}
			expectedWALRecords := test.tenants*2 + test.tenants*test.perTenant
			if len(replayRecords) != expectedWALRecords {
				_ = recoveredWAL.Close()
				t.Fatalf("recovered WAL records = %d, want %d", len(replayRecords), expectedWALRecords)
			}
			var replayBytes int64
			var highestLSN uint64
			for _, record := range replayRecords {
				replayBytes += int64(len(record.Payload))
				highestLSN = max(highestLSN, record.LSN)
			}
			if err := recoveredWAL.Prune(context.Background(), highestLSN+1); err != nil {
				_ = recoveredWAL.Close()
				t.Fatal(err)
			}
			if err := recoveredWAL.Close(); err != nil {
				t.Fatal(err)
			}

			reader := NewTenantStore(objects, prefix)
			reader.IngestMetadataMode = IngestMetadataModeSegment
			capacityVerifyMetadata(t, reader, test.name, test.tenants, test.perTenant)

			totalRequests := test.tenants * test.perTenant
			metadataPuts := objects.metadataPuts(prefix) - beforePuts
			putRate := float64(metadataPuts) / float64(totalRequests)
			if test.maxPutRate > 0 && putRate > test.maxPutRate {
				t.Fatalf("metadata PUT/request = %.4f, want <= %.4f", putRate, test.maxPutRate)
			}
			var after runtime.MemStats
			runtime.ReadMemStats(&after)
			report := map[string]any{
				"scenario":             test.name,
				"storage_profile":      "local_wal_rustfs_metadata",
				"tenants":              test.tenants,
				"requests":             totalRequests,
				"metadata_puts":        metadataPuts,
				"metadata_put_per_req": putRate,
				"durable_p50_ms":       durationMillis(capacityPercentileDuration(durableDurations, 0.50)),
				"durable_p95_ms":       durationMillis(capacityPercentileDuration(durableDurations, 0.95)),
				"durable_p99_ms":       durationMillis(capacityPercentileDuration(durableDurations, 0.99)),
				"committed_p50_ms":     durationMillis(capacityPercentileDuration(committedDurations, 0.50)),
				"committed_p95_ms":     durationMillis(capacityPercentileDuration(committedDurations, 0.95)),
				"committed_p99_ms":     durationMillis(capacityPercentileDuration(committedDurations, 0.99)),
				"wal_high_water_bytes": walHighWater.Load(),
				"replay_bytes":         replayBytes,
				"recovery_ms":          durationMillis(recoveryDuration),
				"heap_alloc_bytes":     after.HeapAlloc,
				"rss_bytes":            processRSSBytes(),
				"gc_cycles":            after.NumGC - before.NumGC,
			}
			data, _ := json.Marshal(report)
			t.Log(string(data))
		})
	}
}

type capacityWALRecord struct {
	envelope walIngestEnvelope
	metadata ingestMetadataRecord
	started  time.Time
}

func capacityAppendAccepted(
	t *testing.T,
	wal *IngestWAL,
	scenario string,
	tenants int,
	perTenant int,
) ([][]capacityWALRecord, []time.Duration) {
	t.Helper()
	records := make([][]capacityWALRecord, tenants)
	durable := make([][]time.Duration, tenants)
	sem := make(chan struct{}, min(64, tenants))
	var wait sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for tenantIndex := range tenants {
		wait.Add(1)
		go func() {
			defer wait.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			tenantID := capacityTenantID(scenario, tenantIndex)
			tenantRecords := make([]capacityWALRecord, perTenant)
			tenantDurable := make([]time.Duration, perTenant)
			for requestIndex := range perTenant {
				started := time.Now()
				request, err := PrepareIngestRequest(tenantID, capacityIngestRequest(tenantID, requestIndex))
				if err != nil {
					recordCapacityError(&errMu, &firstErr, err)
					return
				}
				requestJSON, err := json.Marshal(request)
				if err != nil {
					recordCapacityError(&errMu, &firstErr, err)
					return
				}
				envelope := walIngestEnvelope{
					RecordID:   ingestRecordID(ingestRequestIdentity(tenantID, request)),
					TenantID:   tenantID,
					Request:    request,
					Digest:     sha256Hex(requestJSON),
					AcceptedAt: started.UTC(),
					State:      IngestStateAccepted,
				}
				payload, err := json.Marshal(envelope)
				if err != nil {
					recordCapacityError(&errMu, &firstErr, err)
					return
				}
				appendResult, err := wal.Append(context.Background(), IngestWALAccepted, payload)
				if err != nil {
					recordCapacityError(&errMu, &firstErr, err)
					return
				}
				tenantDurable[requestIndex] = time.Since(started)
				envelope.AcceptedLSN = appendResult.LSN
				finishedAt := time.Now().UTC()
				result := IngestResult{
					BatchID: request.BatchID,
					Version: int64(requestIndex + 1),
					Applied: 1,
				}
				envelope.Result = &result
				envelope.FinishedAt = finishedAt
				tenantRecords[requestIndex] = capacityWALRecord{
					envelope: envelope,
					metadata: ingestMetadataRecord{
						AcceptedLSN: appendResult.LSN,
						Digest:      envelope.Digest,
						Batch: IngestBatchRecord{
							TenantID:   tenantID,
							Request:    request,
							Result:     result,
							StartedAt:  started.UTC(),
							FinishedAt: finishedAt,
						},
					},
					started: started,
				}
			}
			records[tenantIndex] = tenantRecords
			durable[tenantIndex] = tenantDurable
		}()
	}
	wait.Wait()
	if firstErr != nil {
		t.Fatal(firstErr)
	}
	flatDurable := make([]time.Duration, 0, tenants*perTenant)
	for _, values := range durable {
		flatDurable = append(flatDurable, values...)
	}
	return records, flatDurable
}

func capacityPublishMetadata(
	t *testing.T,
	wal *IngestWAL,
	store *TenantStore,
	records [][]capacityWALRecord,
) []time.Duration {
	t.Helper()
	total := 0
	for _, tenantRecords := range records {
		total += len(tenantRecords)
	}
	committed := make([]time.Duration, 0, total)
	for tenantIndex, tenantRecords := range records {
		tenantID := tenantRecords[0].envelope.TenantID
		groupID := fmt.Sprintf("capacity-%04d", tenantIndex)
		published := make([]walIngestEnvelope, len(tenantRecords))
		metadata := make([]ingestMetadataRecord, len(tenantRecords))
		for index, record := range tenantRecords {
			envelope := record.envelope
			envelope.State = IngestStatePublished
			envelope.MetadataFlushID = groupID
			published[index] = envelope
			metadata[index] = record.metadata
		}
		capacityAppendWALBatch(t, wal, IngestWALPublished, published)
		if _, err := store.publishIngestMetadataSegment(context.Background(), tenantID, metadata); err != nil {
			t.Fatalf("publish metadata for %s: %v", tenantID, err)
		}
		finalized := make([]walIngestEnvelope, len(published))
		for index, envelope := range published {
			envelope.State = IngestStateCommitted
			finalized[index] = envelope
		}
		capacityAppendWALBatch(t, wal, IngestWALFinalized, finalized)
		finished := time.Now()
		for _, record := range tenantRecords {
			committed = append(committed, finished.Sub(record.started))
		}
	}
	return committed
}

func capacityAppendWALBatch(t *testing.T, wal *IngestWAL, kind IngestWALRecordType, envelopes []walIngestEnvelope) {
	t.Helper()
	payload, err := json.Marshal(walPreparedBatchEnvelope{Items: envelopes})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wal.Append(context.Background(), kind, payload); err != nil {
		t.Fatal(err)
	}
}

func capacityVerifyMetadata(
	t *testing.T,
	reader *TenantStore,
	scenario string,
	tenants int,
	perTenant int,
) {
	t.Helper()
	for tenantIndex := range tenants {
		tenantID := capacityTenantID(scenario, tenantIndex)
		for requestIndex := range perTenant {
			batchID := fmt.Sprintf("batch-%04d", requestIndex)
			record, err := reader.GetIngestBatch(context.Background(), tenantID, "capacity", "collector", batchID)
			if err != nil {
				t.Fatalf("lookup %s/%s: %v", tenantID, batchID, err)
			}
			if record.Result.BatchID != batchID || record.Result.Version != int64(requestIndex+1) {
				t.Fatalf("lookup %s/%s returned %#v", tenantID, batchID, record.Result)
			}
		}
		status, err := reader.GetCollectorStatus(context.Background(), tenantID, "capacity", "collector")
		if err != nil {
			t.Fatal(err)
		}
		if status.AppliedTotal != perTenant || status.LastVersion != int64(perTenant) {
			t.Fatalf("collector status for %s = %#v, want applied=%d version=%d", tenantID, status, perTenant, perTenant)
		}
	}
}

func recordCapacityError(mu *sync.Mutex, target *error, err error) {
	mu.Lock()
	defer mu.Unlock()
	if *target == nil {
		*target = err
	}
}

func capacityTenantID(scenario string, tenantIndex int) string {
	return fmt.Sprintf("%s-%04d", scenario, tenantIndex)
}

func capacityIngestRequest(tenantID string, requestIndex int) IngestRequest {
	batchID := fmt.Sprintf("batch-%04d", requestIndex)
	return IngestRequest{
		Source:         "capacity",
		CollectorID:    "collector",
		BatchID:        batchID,
		IdempotencyKey: "idem-" + batchID,
		Items: []IngestItem{{
			ExternalID: fmt.Sprintf("host-%04d", requestIndex),
			Entity: &graph.Entity{
				ID:     fmt.Sprintf("host:%s:%04d", tenantID, requestIndex),
				Kind:   "host",
				Fields: graph.Fields{"name": fmt.Sprintf("host-%04d", requestIndex)},
			},
		}},
	}
}

type capacityCountingStore struct {
	ObjectStore
	mu   sync.Mutex
	puts []string
}

func (s *capacityCountingStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, condition)
	if err == nil {
		s.mu.Lock()
		s.puts = append(s.puts, key)
		s.mu.Unlock()
	}
	return meta, err
}

func (s *capacityCountingStore) metadataPuts(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, key := range s.puts {
		if strings.HasPrefix(key, prefix+"/tenants/") && strings.Contains(key, "/ingest/metadata/") {
			count++
		}
	}
	return count
}

func sampleDirectoryHighWater(dir string) (func(), *atomic.Int64) {
	var highWater atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			updateDirectoryHighWater(dir, &highWater)
			select {
			case <-ticker.C:
			case <-stop:
				updateDirectoryHighWater(dir, &highWater)
				return
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			close(stop)
			<-done
		})
	}, &highWater
}

func updateDirectoryHighWater(dir string, highWater *atomic.Int64) {
	var total int64
	_ = filepath.WalkDir(dir, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err == nil {
			total += info.Size()
		}
		return nil
	})
	for current := highWater.Load(); total > current && !highWater.CompareAndSwap(current, total); current = highWater.Load() {
	}
}

func capacityPercentileDuration(values []time.Duration, quantile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(quantile * float64(len(sorted)-1))
	return sorted[index]
}

func durationMillis(value time.Duration) float64 {
	return float64(value.Microseconds()) / 1000
}

func processRSSBytes() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * int64(os.Getpagesize())
}
