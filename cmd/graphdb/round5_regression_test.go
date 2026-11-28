package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRound5CleanupOrphanRuntimeRestoreStages(t *testing.T) {
	dataDir := t.TempDir()
	walDir := t.TempDir()
	roots := map[string]string{"data": dataDir, "wal": walDir}

	orphanData := filepath.Join(dataDir, ".runtime-restore-deadbeef12345678")
	orphanWal := filepath.Join(walDir, ".runtime-restore-deadbeef12345678")
	if err := os.MkdirAll(orphanData, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(orphanData, "partial.bin"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(orphanWal, 0700); err != nil {
		t.Fatal(err)
	}

	if err := cleanupOrphanRuntimeRestoreStages(roots); err != nil {
		t.Fatalf("cleanupOrphanRuntimeRestoreStages failed: %v", err)
	}
	if _, err := os.Lstat(orphanData); !os.IsNotExist(err) {
		t.Fatalf("orphan data stage still exists: %v", err)
	}
	if _, err := os.Lstat(orphanWal); !os.IsNotExist(err) {
		t.Fatalf("orphan wal stage still exists: %v", err)
	}
	journal := filepath.Join(dataDir, runtimeRestoreJournal)
	if err := checkRuntimeTargets(roots, journal, nil, nil); err != nil {
		t.Fatalf("checkRuntimeTargets still failed after cleanup: %v", err)
	}
}
