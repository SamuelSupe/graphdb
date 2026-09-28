package storage

import (
	"context"
	"fmt"
)

const objectPrefixProbePageSize = 128
const objectPrefixScanPageSize = 1000

func objectPrefixMatches(
	ctx context.Context,
	objects ObjectStore,
	prefix string,
	match func(ObjectInfo) bool,
) (bool, error) {
	if !objectStoreSupportsPaging(objects) {
		items, err := objects.List(ctx, prefix)
		if err != nil {
			return false, err
		}
		return anyObjectMatches(items, match), nil
	}
	cursor := ""
	for {
		items, next, err := listObjectPage(
			ctx, objects, prefix, cursor, objectPrefixProbePageSize,
		)
		if err != nil {
			return false, err
		}
		if anyObjectMatches(items, match) {
			return true, nil
		}
		if next == "" {
			return false, nil
		}
		if next <= cursor {
			return false, fmt.Errorf(
				"object list cursor did not advance for prefix %q", prefix,
			)
		}
		cursor = next
	}
}

func anyObjectMatches(items []ObjectInfo, match func(ObjectInfo) bool) bool {
	for _, item := range items {
		if match(item) {
			return true
		}
	}
	return false
}

func objectStoreSupportsPaging(objects ObjectStore) bool {
	for objects != nil {
		if _, ok := objects.(objectPageLister); ok {
			return true
		}
		unwrapper, ok := objects.(objectStoreUnwrapper)
		if !ok {
			return false
		}
		next := unwrapper.UnwrapObjectStore()
		if next == objects {
			return false
		}
		objects = next
	}
	return false
}

func scanObjectPrefix(
	ctx context.Context,
	objects ObjectStore,
	prefix string,
	visit func([]ObjectInfo) error,
) error {
	if !objectStoreSupportsPaging(objects) {
		items, err := objects.List(ctx, prefix)
		if err != nil {
			return err
		}
		return visit(items)
	}
	cursor := ""
	for {
		items, next, err := listObjectPage(
			ctx, objects, prefix, cursor, objectPrefixScanPageSize,
		)
		if err != nil {
			return err
		}
		if err := visit(items); err != nil {
			return err
		}
		if next == "" {
			return nil
		}
		if next <= cursor {
			return fmt.Errorf(
				"object list cursor did not advance for prefix %q", prefix,
			)
		}
		cursor = next
	}
}

func scanObjectPrefixFresh(
	ctx context.Context,
	objects ObjectStore,
	prefix string,
	visit func([]ObjectInfo) error,
) error {
	if !objectStoreSupportsPaging(objects) {
		items, _, err := listObjectPage(ctx, objects, prefix, "", 0)
		if err != nil {
			return err
		}
		return visit(items)
	}
	cursor := ""
	for {
		items, next, err := listObjectPage(
			ctx, objects, prefix, cursor, objectPrefixScanPageSize,
		)
		if err != nil {
			return err
		}
		if err := visit(items); err != nil {
			return err
		}
		if next == "" {
			return nil
		}
		if next <= cursor {
			return fmt.Errorf(
				"object list cursor did not advance for prefix %q", prefix,
			)
		}
		cursor = next
	}
}
