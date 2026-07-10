package storage

import (
	"context"
	"errors"
	"strings"
)

var errGCPaused = errors.New("gc checkpoint paused")

type gcCheckpointRunner struct {
	options    GCOptions
	checkpoint GCCheckpoint
	prefixSeen map[string]struct{}
}

func newGCCheckpointRunner(options GCOptions) *gcCheckpointRunner {
	return &gcCheckpointRunner{
		options: options,
		checkpoint: GCCheckpoint{
			Cursor:     options.CheckpointCursor,
			MaxDeletes: options.MaxDeletes,
			DryRun:     options.DryRun,
		},
		prefixSeen: map[string]struct{}{},
	}
}

func (r *gcCheckpointRunner) addPrefix(prefix string) {
	if prefix == "" {
		return
	}
	if _, ok := r.prefixSeen[prefix]; ok {
		return
	}
	r.prefixSeen[prefix] = struct{}{}
	r.checkpoint.ScannedPrefixes = append(r.checkpoint.ScannedPrefixes, prefix)
}

func (r *gcCheckpointRunner) deleteKey(ctx context.Context, objects ObjectStore, key string) (bool, error) {
	return r.deleteKeyWithCursor(ctx, objects, key, true)
}

func (r *gcCheckpointRunner) deleteKeyIgnoringCursor(ctx context.Context, objects ObjectStore, key string) (bool, error) {
	return r.deleteKeyWithCursor(ctx, objects, key, false)
}

func (r *gcCheckpointRunner) deleteKeyWithCursor(ctx context.Context, objects ObjectStore, key string, honorCursor bool) (bool, error) {
	if key == "" {
		return false, nil
	}
	if r.checkpoint.Paused {
		return false, errGCPaused
	}
	if honorCursor && r.checkpoint.Cursor != "" && key <= r.checkpoint.Cursor {
		r.checkpoint.SkippedByCursor++
		return false, nil
	}
	if r.limitReached() {
		r.pause()
		return false, errGCPaused
	}
	r.checkpoint.ScannedKeys++
	r.checkpoint.LastKey = key
	if r.options.DryRun {
		r.checkpoint.Planned++
		r.checkpoint.PlannedKeys = append(r.checkpoint.PlannedKeys, key)
		if r.limitReached() {
			r.pause()
		}
		return false, objectContextErr(ctx)
	}
	if err := objects.Delete(ctx, key); err != nil {
		r.checkpoint.FailedKeys = append(r.checkpoint.FailedKeys, key)
		return false, err
	}
	r.checkpoint.Deleted++
	r.checkpoint.DeletedKeys = append(r.checkpoint.DeletedKeys, key)
	if r.limitReached() {
		r.pause()
	}
	return true, objectContextErr(ctx)
}

func (r *gcCheckpointRunner) limitReached() bool {
	if r.options.MaxDeletes <= 0 {
		return false
	}
	return r.checkpoint.Deleted+r.checkpoint.Planned >= r.options.MaxDeletes
}

func (r *gcCheckpointRunner) pause() {
	r.checkpoint.Paused = true
	r.checkpoint.NextCursor = r.checkpoint.LastKey
}

func (r *gcCheckpointRunner) pauseAt(key string) {
	if key == "" {
		return
	}
	r.checkpoint.LastKey = key
	r.checkpoint.NextCursor = key
	r.checkpoint.Paused = true
}

func (r *gcCheckpointRunner) scanPageLimit() int {
	if r.options.MaxDeletes <= 0 {
		return 0
	}
	return max(64, min(512, r.options.MaxDeletes*2))
}

func (r *gcCheckpointRunner) pageCursor(prefix string) (string, bool) {
	cursor := r.options.CheckpointCursor
	if cursor == "" {
		return "", false
	}
	if strings.HasPrefix(cursor, prefix) {
		return cursor, false
	}
	return "", cursor > prefix
}

func (r *gcCheckpointRunner) listPage(ctx context.Context, objects ObjectStore, prefix string) ([]ObjectInfo, string, bool, error) {
	r.addPrefix(prefix)
	cursor, skip := r.pageCursor(prefix)
	if skip {
		return nil, "", true, nil
	}
	items, next, err := listObjectPage(ctx, objects, prefix, cursor, r.scanPageLimit())
	return items, next, false, err
}

func (r *gcCheckpointRunner) pauseAfterPage(next string) error {
	if r.checkpoint.Paused {
		return errGCPaused
	}
	if next == "" {
		return nil
	}
	r.pauseAt(next)
	return errGCPaused
}

func (r *gcCheckpointRunner) result() GCCheckpoint {
	out := r.checkpoint
	out.Completed = !out.Paused
	return out
}

func gcPaused(err error) bool {
	return errors.Is(err, errGCPaused)
}
