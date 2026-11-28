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
	for {
		current := n.snapshotCleanupTarget.Load()
		if index <= current {
			break
		}
		if n.snapshotCleanupTarget.CompareAndSwap(current, index) {
			break
		}
	}
	if n.snapshotCleanupWake == nil {
		return
	}
	select {
	case n.snapshotCleanupWake <- struct{}{}:
	default:
	}
}

func (n *Node) snapshotCleanupLoop(orphans []os.FileInfo) {
	defer n.workers.Done()
	retry := time.NewTicker(5 * time.Second)
	defer retry.Stop()
	var cleaned uint64
	for {
		woke := false
		select {
		case <-n.ctx.Done():
			return
		case <-n.snapshotCleanupWake:
			woke = true
		case <-retry.C:
			if n.snapshotCleanupTarget.Load() <= cleaned && len(orphans) == 0 {
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
		target := n.snapshotCleanupTarget.Load()
		if target > cleaned || (woke && target > 0) {
			_, cleanupErr := pruneSnapshotFilesBefore(n.cfg.Dir, min(target, applied), "", false)
			err = errors.Join(err, cleanupErr)
			if cleanupErr == nil && applied >= target && target > cleaned {
				cleaned = target
			}
		}
		finish(err)
		n.mu.Lock()
		n.snapshotCleanupFailure = err
		n.mu.Unlock()
	}
}
