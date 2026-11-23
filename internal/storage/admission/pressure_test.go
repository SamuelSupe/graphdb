package admission

import (
	"testing"
	"time"
)

func TestLatencyBackpressureExpiresWithoutSuccessfulWrites(t *testing.T) {
	now := time.Now()
	pressure := NewWritePressure(BackpressureConfig{ObjectLatencyThreshold: time.Millisecond, ObjectErrorWindow: time.Second})
	pressure.clock = func() time.Time { return now }
	pressure.RecordObjectLatency(time.Second)
	if len(pressure.Reasons("tenant-a")) == 0 {
		t.Fatal("missing initial pressure")
	}
	now = now.Add(2 * time.Second)
	if reasons := pressure.Reasons("tenant-a"); len(reasons) > 0 {
		t.Fatalf("stale samples permanently block new writes: %+v", reasons)
	}
	pressure.RecordObjectLatency(time.Microsecond)
	if reasons := pressure.Reasons("tenant-a"); len(reasons) > 0 {
		t.Fatalf("expired EWMA revived: %+v", reasons)
	}
}

func TestTenantCASConflictWindowSurvivesDefaultPressureCheck(t *testing.T) {
	now := time.Unix(100, 0)
	config := BackpressureConfig{CASConflictThreshold: 2, CASConflictWindow: 30 * time.Second}
	pressure := NewWritePressure(config)
	pressure.clock = func() time.Time { return now }
	config.CASConflictWindow = 120 * time.Second
	pressure.RecordManifestCASConflictWithConfig("a", config)
	now = now.Add(40 * time.Second)
	pressure.Reasons("a")
	pressure.RecordManifestCASConflictWithConfig("a", config)
	if reasons := pressure.ReasonsWithConfig("a", config); len(reasons) != 1 {
		t.Fatalf("tenant pressure: %v", reasons)
	}
	now = now.Add(121 * time.Second)
	if reasons := pressure.ReasonsWithConfig("a", config); len(reasons) != 0 {
		t.Fatalf("expired tenant pressure: %v", reasons)
	}
}
