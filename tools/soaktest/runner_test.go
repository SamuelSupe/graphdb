package main

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMaintenancePreservesFailuresAndIgnoresRunCancellation(t *testing.T) {
	for _, test := range []struct {
		name        string
		cancel      bool
		err         error
		wantFailure bool
	}{
		{"run_cancel", true, context.Canceled, false},
		{"request_deadline", false, context.DeadlineExceeded, true},
		{"failure_at_shutdown", true, errors.New("invalid catalog"), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var output bytes.Buffer
			runner := &soakRunner{events: newEventWriter(&output)}
			var wg sync.WaitGroup
			wg.Add(1)
			calls := 0
			runner.periodic(ctx, &wg, "gc", time.Millisecond, func(context.Context, *registry) error {
				calls++
				if test.cancel || calls > 1 {
					cancel()
				}
				if calls > 1 {
					return context.Canceled
				}
				return test.err
			})
			wg.Wait()
			if calls == 0 || bytes.Contains(output.Bytes(), []byte(`"event":"gc_error"`)) != test.wantFailure {
				t.Fatalf("maintenance failure classification: calls=%d output=%s", calls, output.String())
			}
		})
	}
}

func TestSnapshotExportThrottle(t *testing.T) {
	runner := newSoakRunner(config{snapshotExportInterval: 5 * time.Minute}, nil, nil, nil, nil, nil, 0)
	now := time.Unix(100, 0)
	if !runner.tryStartSnapshotExport(now) {
		t.Fatal("first export was throttled")
	}
	if runner.tryStartSnapshotExport(now.Add(time.Second)) {
		t.Fatal("concurrent export was allowed")
	}
	runner.snapshotExportRunning.Store(false)
	if runner.tryStartSnapshotExport(now.Add(time.Minute)) {
		t.Fatal("export inside cooldown was allowed")
	}
	if !runner.tryStartSnapshotExport(now.Add(5 * time.Minute)) {
		t.Fatal("export after cooldown was throttled")
	}
}
