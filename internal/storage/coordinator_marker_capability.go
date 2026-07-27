package storage

import (
	"context"
	"fmt"
)

func requireS3ConditionalDelete(
	ctx context.Context,
	objects ObjectStore,
	probeKey string,
) error {
	store := findS3Store(objects)
	if store == nil {
		return nil
	}
	supported, err := store.supportsConditionalDelete(ctx, probeKey)
	if err != nil {
		return fmt.Errorf("probe S3 conditional delete: %w", err)
	}
	if !supported {
		return fmt.Errorf(
			"%w: PostgreSQL coordination requires atomic If-Match delete",
			ErrConditionalDeleteUnsupported,
		)
	}
	return nil
}

func findS3Store(objects ObjectStore) *S3Store {
	for depth := 0; objects != nil && depth < 16; depth++ {
		switch store := objects.(type) {
		case *S3Store:
			return store
		case *ReadProtectedObjectStore:
			objects = store.Inner
		case *DelayedReadObjectStore:
			objects = store.Inner
		case *SingleWriterObjectStore:
			objects = store.Inner
		default:
			unwrapper, ok := objects.(objectStoreUnwrapper)
			if !ok {
				return nil
			}
			next := unwrapper.UnwrapObjectStore()
			if next == nil || next == objects {
				return nil
			}
			objects = next
		}
	}
	return nil
}
