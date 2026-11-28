package observability

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestUnregisterTenantDropsMetricsEvenWhenCheckIsInFlight(t *testing.T) {
	o := New(nil, time.Second)
	o.RegisterTenant("tenant-a")
	o.Metrics.RecordIndexHealth("tenant-a", "ok", 0)
	started, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		o.checkKnownTenantIndexes(t.Context(), func(context.Context, string) (string, int, error) {
			close(started)
			<-resume
			return "ok", 0, nil
		})
	}()
	<-started
	o.UnregisterTenant("tenant-a")
	close(resume)
	<-done
	if out := string(o.Metrics.SnapshotPrometheus()); strings.Contains(out, `tenant="tenant-a"`) {
		t.Fatalf("unregistered tenant still appears in metrics: %s", out)
	}
}

func TestMetricsCardinalityCapsAndLockFreeSnapshot(t *testing.T) {
	m := NewMetrics()
	for i := 0; i < 270; i++ {
		m.RecordHTTPRequest(fmt.Sprintf("CUSTOM_%d", i), "/v1/query", 200, time.Millisecond)
		m.RecordQuery("tenant-a", fmt.Sprintf("custom_op_%d", i), "ok", time.Millisecond, false)
		m.RecordIngestSkipped("tenant-a", fmt.Sprintf("source-%d", i), "duplicate")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			m.RecordHTTPRequest("GET", "/v1/health", 200, time.Millisecond)
			m.RecordQuery("tenant-a", "match", "ok", time.Millisecond, false)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = m.SnapshotPrometheus()
		}
	}()
	wg.Wait()

	out := string(m.SnapshotPrometheus())
	if !strings.Contains(out, `method="OTHER"`) {
		t.Fatalf("expected unknown HTTP methods to collapse to OTHER, got:\n%s", out)
	}
	if !strings.Contains(out, `op="invalid"`) {
		t.Fatalf("expected unknown query ops to collapse to invalid, got:\n%s", out)
	}
	if !strings.Contains(out, `source="__other__"`) {
		t.Fatalf("expected excess ingest sources to collapse to __other__, got:\n%s", out)
	}
}

func TestMetricsSplitLocksConcurrentRace(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	wg.Add(5)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			m.RecordQuery("tenant-a", "match", "ok", time.Millisecond, i%10 == 0)
			m.RecordReadAdmissionQueue("tenant-a", "ok", time.Millisecond)
			m.RecordReaderCache("tenant-a", "hit")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			m.RecordIngestWALAppend("batch", "ok", 128, time.Millisecond)
			m.RecordIngestFlush("ok", time.Millisecond, 2, 1, 1, 1, 0, false)
			m.RecordIngestSkipped("tenant-a", "cmdb", "duplicate")
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			m.RecordHTTPRequest("POST", "/v1/query", 200, time.Millisecond)
			m.RecordObjectStoreOperation("get", "ok", time.Millisecond)
			m.RecordWriteAdmissionQueue("tenant-a", "ok", time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = m.SnapshotPrometheus()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			tid := fmt.Sprintf("ephemeral-%d", i%5)
			m.RecordQuery(tid, "match", "ok", time.Millisecond, false)
			m.forgetTenant(tid)
		}
	}()
	wg.Wait()
	out := string(m.SnapshotPrometheus())
	if !strings.Contains(out, `graphdb_queries_total{tenant="tenant-a",op="match",status="ok"} 300`) {
		t.Fatalf("missing expected query counter:\n%s", out)
	}
	if !strings.Contains(out, `graphdb_ingest_flush_total{status="ok"} 300`) {
		t.Fatalf("missing expected ingest flush counter:\n%s", out)
	}
}
