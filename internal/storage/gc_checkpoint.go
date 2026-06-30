package storage

import (
	"context"
	"errors"
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
	if key == "" {
		return false, nil
	}
	if r.checkpoint.Paused {
		return false, errGCPaused
	}
	if r.checkpoint.Cursor != "" && key <= r.checkpoint.Cursor {
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

func (r *gcCheckpointRunner) result() GCCheckpoint {
	out := r.checkpoint
	out.Completed = !out.Paused
	return out
}

func gcPaused(err error) bool {
	return errors.Is(err, errGCPaused)
}
