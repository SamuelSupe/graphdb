package storage

import (
	"path/filepath"
	"reflect"
)

func sameTenantMigrationLocation(
	source ObjectStore,
	sourcePrefix string,
	target ObjectStore,
	targetPrefix string,
) bool {
	if sourcePrefix != targetPrefix {
		return false
	}
	source = unwrapTenantMigrationStore(source)
	target = unwrapTenantMigrationStore(target)
	switch sourceStore := source.(type) {
	case *MemoryStore:
		targetStore, ok := target.(*MemoryStore)
		return ok && sourceStore == targetStore
	case *FileStore:
		targetStore, ok := target.(*FileStore)
		return ok && sameFileStoreRoot(sourceStore.root, targetStore.root)
	case *S3Store:
		targetStore, ok := target.(*S3Store)
		return ok &&
			sourceStore.endpoint.String() == targetStore.endpoint.String() &&
			sourceStore.bucket == targetStore.bucket
	}
	sourceValue := reflect.ValueOf(source)
	targetValue := reflect.ValueOf(target)
	return sourceValue.IsValid() &&
		targetValue.IsValid() &&
		sourceValue.Type() == targetValue.Type() &&
		sourceValue.Kind() == reflect.Pointer &&
		sourceValue.Pointer() == targetValue.Pointer()
}

func unwrapTenantMigrationStore(store ObjectStore) ObjectStore {
	for store != nil {
		switch current := store.(type) {
		case *ReadProtectedObjectStore:
			store = current.Inner
		case *DelayedReadObjectStore:
			store = current.Inner
		case *SingleWriterObjectStore:
			store = current.Inner
		default:
			unwrapper, ok := store.(objectStoreUnwrapper)
			if !ok {
				return store
			}
			next := unwrapper.UnwrapObjectStore()
			if next == nil || next == store {
				return store
			}
			store = next
		}
	}
	return nil
}

func sameFileStoreRoot(left string, right string) bool {
	leftPath, leftErr := filepath.Abs(left)
	rightPath, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return filepath.Clean(left) == filepath.Clean(right)
	}
	return filepath.Clean(leftPath) == filepath.Clean(rightPath)
}
