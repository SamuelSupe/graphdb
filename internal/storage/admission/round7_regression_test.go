package admission

import (
	"context"
	"testing"
	"time"
)

func TestRound7ReasonsWithConfigPreservesLargerWindowAndNilRecentBytes(t *testing.T) {
	var nilPressure *WritePressure
	if bytes, ok := nilPressure.RecentTenantBytes("t1"); ok || bytes != 0 {
		t.Fatalf("nil RecentTenantBytes = (%d, %v), want (0, false)", bytes, ok)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p := NewWritePressure(BackpressureConfig{
		ObjectErrorWindow:      10 * time.Second,
		ObjectErrorThreshold:   1,
		ObjectLatencyThreshold: 100 * time.Millisecond,
	})
	p.clock = func() time.Time { return now }

	p.RecordObjectOperation(500*time.Millisecond, context.DeadlineExceeded)

	// Advance 20s: outside default 10s window, but inside custom 60s window.
	now = now.Add(20 * time.Second)
	custom := BackpressureConfig{
		ObjectErrorWindow:      60 * time.Second,
		ObjectErrorThreshold:   1,
		ObjectLatencyThreshold: 100 * time.Millisecond,
	}
	reasons := p.ReasonsWithConfig("t1", custom)
	if len(reasons) != 2 {
		t.Fatalf("expected latency and error reasons under larger custom window, got %+v", reasons)
	}
}
