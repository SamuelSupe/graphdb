package observability

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

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
