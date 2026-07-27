package httpapi

import (
	"context"
	"testing"
	"time"
)

func TestAdmissionsRejectAlreadyCanceledContext(t *testing.T) {
	t.Run("query", func(t *testing.T) {
		admission := NewQueryAdmission(1, 1, time.Second)
		for attempt := 0; attempt < 100; attempt++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			release, err := admission.Acquire(ctx, "tenant-a")
			if err == nil {
				release()
				t.Fatalf("attempt %d admitted a canceled query", attempt)
			}
		}
	})

	t.Run("write", func(t *testing.T) {
		admission := NewWriteAdmission(1, 1, time.Second)
		for attempt := 0; attempt < 100; attempt++ {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			release, _, err := admission.Acquire(ctx, "tenant-a")
			if err == nil {
				release()
				t.Fatalf("attempt %d admitted a canceled write", attempt)
			}
		}
	})
}
