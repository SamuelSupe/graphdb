package storage

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

var diskSyncCalls atomic.Int64
var diskSyncFailures atomic.Int64
var diskSyncNanos atomic.Int64

func syncStorageFile(file interface{ Sync() error }) error {
	start := time.Now()
	err := file.Sync()
	diskSyncCalls.Add(1)
	diskSyncNanos.Add(int64(time.Since(start)))
	if err != nil {
		diskSyncFailures.Add(1)
	}
	return err
}

// WriteDiskMetrics reports this process's data, directory, and ingest WAL syncs.
// It does not include syncs performed by other processes or external services.
func WriteDiskMetrics(w io.Writer) {
	fmt.Fprintf(w, "# HELP graphdb_disk_sync_total Storage file and directory sync attempts in this process, excluding the Raft database.\n# TYPE graphdb_disk_sync_total counter\ngraphdb_disk_sync_total %d\n", diskSyncCalls.Load())
	fmt.Fprintf(w, "# HELP graphdb_disk_sync_failures_total Failed storage file and directory sync attempts in this process.\n# TYPE graphdb_disk_sync_failures_total counter\ngraphdb_disk_sync_failures_total %d\n", diskSyncFailures.Load())
	fmt.Fprintf(w, "# HELP graphdb_disk_sync_seconds_total Cumulative storage file and directory sync latency in this process.\n# TYPE graphdb_disk_sync_seconds_total counter\ngraphdb_disk_sync_seconds_total %g\n", float64(diskSyncNanos.Load())/float64(time.Second))
}
