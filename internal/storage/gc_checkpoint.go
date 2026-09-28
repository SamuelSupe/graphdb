package storage

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"
)

var errGCPaused = errors.New("gc checkpoint paused")

type gcCheckpointRunner struct {
	options    GCOptions
	checkpoint GCCheckpoint
	prefixSeen map[string]struct{}
	started    time.Time
	visited    int
	bytes      int64
}

type gcListing struct {
	after, next string
	items       []ObjectInfo
}

const (
	gcBatchDuration = 50 * time.Millisecond
	gcBatchBytes    = 16 << 20
)

func newGCCheckpointRunner(options GCOptions) *gcCheckpointRunner {
	return &gcCheckpointRunner{
		options: options,
		started: time.Now(),
		checkpoint: GCCheckpoint{
			Cursor:     options.CheckpointCursor,
			MaxDeletes: options.MaxDeletes,
			DryRun:     options.DryRun,
		},
		prefixSeen: map[string]struct{}{},
	}
}

// Yield between objects, including ones that are retained rather than deleted.
// A single object is always allowed so a large object cannot stall the cursor.
func (r *gcCheckpointRunner) visit(object ObjectInfo) error {
	if r.options.listings == nil {
		return nil
	}
	if r.visited > 0 && (r.visited >= 512 || r.bytes >= gcBatchBytes || time.Since(r.started) >= gcBatchDuration) {
		r.pauseBeforeObject(object.Key)
		return errGCPaused
	}
	r.visited++
	r.bytes += object.Size
	return nil
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

func (r *gcCheckpointRunner) deleteExistingKeyIgnoringCursor(
	ctx context.Context,
	objects ObjectStore,
	key string,
) (bool, error) {
	if _, err := objectMeta(ctx, objects, key); errors.Is(err, ErrNotFound) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	return r.deleteKeyIgnoringCursor(ctx, objects, key)
}

func (r *gcCheckpointRunner) planExistingKeyIgnoringLimit(
	ctx context.Context,
	objects ObjectStore,
	key string,
) error {
	if _, err := objectMeta(ctx, objects, key); errors.Is(err, ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	r.checkpoint.ScannedKeys++
	r.checkpoint.LastKey = key
	r.checkpoint.Planned++
	r.checkpoint.PlannedKeys = append(r.checkpoint.PlannedKeys, key)
	return objectContextErr(ctx)
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
		if !r.options.view.canDelete(key) {
			r.checkpoint.DeferredFiles++
			return false, objectContextErr(ctx)
		}
		r.checkpoint.Planned++
		r.checkpoint.PlannedKeys = append(r.checkpoint.PlannedKeys, key)
		if r.limitReached() {
			r.pause()
		}
		return false, objectContextErr(ctx)
	}
	if r.options.view != nil {
		ctx = context.WithValue(ctx, gcViewKey{}, r.options.view)
	}
	if err := objects.Delete(ctx, key); err != nil {
		if errors.Is(err, errGCViewPinned) {
			r.checkpoint.DeferredFiles++
			return false, nil
		}
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

func (r *gcCheckpointRunner) pauseBeforeObject(key string) {
	if key == "" {
		return
	}
	// Listing cursors are exclusive. A strict prefix of the current key makes
	// that task visible again without rescanning previously completed tasks.
	cursor := key[:len(key)-1]
	r.checkpoint.NextCursor = cursor
	r.checkpoint.Paused = true
}

func (r *gcCheckpointRunner) scanPageLimit() int {
	if r.options.MaxDeletes <= 0 {
		return 0
	}
	limit := 512
	return max(64, min(limit, r.options.MaxDeletes*2))
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
		delete(r.options.listings, prefix)
		return nil, "", true, nil
	}
	if r.options.listings != nil {
		page, exists := r.options.listings[prefix]
		if !exists || cursor < page.after || (page.next != "" && cursor >= page.next) {
			var err error
			ctx = context.WithValue(ctx, fileListBeforeKey{}, r.options.listBefore)
			page = gcListing{after: cursor}
			page.items, page.next, err = listObjectPage(ctx, objects, prefix, cursor, r.scanPageLimit())
			if err != nil {
				return nil, "", false, err
			}
			r.options.listings[prefix] = page
		}
		start := sort.Search(len(page.items), func(i int) bool { return page.items[i].Key > cursor })
		return page.items[start:], page.next, false, objectContextErr(ctx)
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
