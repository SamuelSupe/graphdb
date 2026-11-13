package ha

import (
	"context"
	"errors"
	"sort"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
	"golang.org/x/sync/errgroup"
)

const maxBufferedFragmentBytes = 1 << 20

var errBufferedFragmentTooLarge = errors.New("fragment exceeds buffered write budget")

// Each batch action owns one partition. Buffer its changes so before-images
// across the group can be recorded together before changing any live files.
type bufferedFragment struct {
	*storage.FileStore
	writes  map[string][]byte
	reclaim []string
	bytes   int
}

func (b *bufferedFragment) Get(ctx context.Context, key string) ([]byte, error) {
	if data, ok := b.writes[key]; ok {
		if data == nil {
			return nil, storage.ErrNotFound
		}
		return data, ctx.Err()
	}
	return b.FileStore.Get(ctx, key)
}

func (b *bufferedFragment) Put(ctx context.Context, key string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	next := b.bytes + len(key) + len(data)
	if next > maxBufferedFragmentBytes {
		return errBufferedFragmentTooLarge
	}
	b.writes[key], b.bytes = data, next
	return nil
}

func (b *bufferedFragment) Delete(ctx context.Context, key string) error {
	return b.Put(ctx, key, nil)
}

func (b *bufferedFragment) DeferPageReclaim(ctx context.Context, keys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	next := b.bytes
	for _, key := range keys {
		next += len(key)
	}
	if next > maxBufferedFragmentBytes {
		return errBufferedFragmentTooLarge
	}
	b.bytes = next
	b.reclaim = append(b.reclaim, keys...)
	return nil
}

func (b *bufferedFragment) PutConditional(ctx context.Context, key string, data []byte, condition storage.PutCondition) (storage.ObjectMeta, error) {
	if !condition.IfNoneMatch || condition.IfMatch != "" {
		return storage.ObjectMeta{}, storage.ErrConditionalWriteUnsupported
	}
	if _, err := b.Get(ctx, key); err == nil {
		return storage.ObjectMeta{}, storage.ErrConflict
	} else if !errors.Is(err, storage.ErrNotFound) {
		return storage.ObjectMeta{}, err
	}
	return storage.ObjectMeta{}, b.Put(ctx, key, data)
}

func (a *Application) flushBufferedFragments(ctx context.Context, writes []map[string][]byte, operation string) error {
	var keys []string
	for _, objects := range writes {
		for key := range objects {
			keys = append(keys, key)
		}
	}
	if len(keys) == 0 {
		return nil
	}
	sort.Strings(keys)
	if err := a.Files.JournalReplicatedObjects(ctx, keys); err != nil {
		return err
	}
	workers, workCtx := errgroup.WithContext(ctx)
	limit := 8
	if operation == "prepare" {
		// Prepare publishes the new tree pages as well as partition state.
		limit = 16
	}
	workers.SetLimit(limit)
	if operation == "prepare" || operation == "finalize" {
		// A partition can produce many more pages than its peers. Schedule
		// the independent fsyncs by object so one partition cannot leave
		// the other writers idle at the end of the batch.
		for _, objects := range writes {
			for key, data := range objects {
				workers.Go(func() error {
					if data == nil {
						return a.Files.Delete(workCtx, key)
					}
					return a.Files.PutReplicatedObject(workCtx, key, data)
				})
			}
		}
		return workers.Wait()
	}
	for _, objects := range writes {
		if len(objects) == 0 {
			continue
		}
		workers.Go(func() error {
			for key, data := range objects {
				var err error
				if data == nil {
					err = a.Files.Delete(workCtx, key)
				} else {
					err = a.Files.Put(workCtx, key, data)
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	return workers.Wait()
}
