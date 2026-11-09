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
