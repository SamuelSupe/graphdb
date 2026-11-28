package admission

import (
	"testing"
	"time"
)

func TestRound6CASConflictHistoryBoundedAndCompacted(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg := BackpressureConfig{
		CASConflictWindow:    10 * time.Second,
		CASConflictThreshold: 5,
	}
	p := NewWritePressure(cfg)
	p.clock = func() time.Time { return now }

	for i := 0; i < 5000; i++ {
		now = now.Add(time.Millisecond)
		p.RecordManifestCASConflict("tenant-a")
	}

	p.mu.Lock()
	gotLen := len(p.conflicts["tenant-a"].times)
	p.mu.Unlock()
	if gotLen > pressureMaxSamples {
		t.Fatalf("conflict history len = %d, want <= %d", gotLen, pressureMaxSamples)
	}

	reasons := p.Reasons("tenant-a")
	if len(reasons) == 0 || reasons[0].Code != "manifest_cas_conflicts_high" {
		t.Fatalf("expected manifest_cas_conflicts_high reason, got %+v", reasons)
	}

	// Advance past window and verify expired entries are compacted away.
	now = now.Add(11 * time.Second)
	if reasons := p.Reasons("tenant-a"); len(reasons) != 0 {
		t.Fatalf("expected no reasons after window expiry, got %+v", reasons)
	}
	p.mu.Lock()
	_, exists := p.conflicts["tenant-a"]
	p.mu.Unlock()
	if exists {
		t.Fatalf("expected expired conflict history entry to be removed")
	}
}
