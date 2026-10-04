package replication

import (
	"errors"
	"os"
	"path/filepath"
	"time"
)

func (n *Node) scheduleSnapshotCleanup(index uint64) {
	if index == 0 {
		return
	}
	// A single producer replaces obsolete cleanup work without retaining graph
	// snapshots or waiting for filesystem cleanup on the Raft control path.
	select {
	case n.snapshotCleanupRequests <- index:
	default:
		select {
		case <-n.snapshotCleanupRequests:
		default:
		}
		n.snapshotCleanupRequests <- index
	}
}

func (n *Node) snapshotCleanupLoop(orphans []os.FileInfo) {
	defer n.workers.Done()
	retry := time.NewTicker(5 * time.Second)
	defer retry.Stop()
	var pending uint64
	for {
		select {
		case <-n.ctx.Done():
			return
		case request := <-n.snapshotCleanupRequests:
			pending = request
		case <-retry.C:
			if pending == 0 && len(orphans) == 0 {
				continue
			}
		}
		finish := n.metrics.Start("snapshot_cleanup")
		var err error
		remaining := orphans[:0]
		for _, orphan := range orphans {
			path := filepath.Join(n.cfg.Dir, "snapshots", orphan.Name())
			current, removeErr := os.Lstat(path)
			// An incoming snapshot can reuse a startup orphan's filename. Its
			// atomic rename installs a different file, which cleanup must keep.
			if os.IsNotExist(removeErr) || (removeErr == nil && !os.SameFile(orphan, current)) {
				continue
			}
			if removeErr == nil {
				removeErr = os.Remove(path)
			}
			if removeErr != nil && !os.IsNotExist(removeErr) {
				remaining = append(remaining, orphan)
				err = errors.Join(err, removeErr)
			}
		}
		orphans = remaining
		// The application worker may still be restoring an older delivered snapshot.
		// Keep its file until durable application has passed that position.
		applied := n.applied.Load()
		if pending > 0 {
			_, cleanupErr := pruneSnapshotFilesBefore(n.cfg.Dir, min(pending, applied), "", false)
			err = errors.Join(err, cleanupErr)
			if cleanupErr == nil && applied >= pending {
				pending = 0
			}
		}
		finish(err)
		n.mu.Lock()
		n.snapshotCleanupFailure = err
		n.mu.Unlock()
	}
}
