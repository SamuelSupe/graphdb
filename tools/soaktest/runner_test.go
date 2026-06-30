package main

import (
	"testing"
	"time"
)

func TestSnapshotExportThrottle(t *testing.T) {
	runner := newSoakRunner(config{snapshotExportInterval: 5 * time.Minute}, nil, nil, nil, nil, 0)
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
