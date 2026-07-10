package storage

import (
	"context"
	"sort"
)

type objectPageLister interface {
	ListPage(ctx context.Context, prefix string, after string, limit int) ([]ObjectInfo, string, error)
}

// listObjectPage preserves the behavior of store wrappers while allowing S3
// callers to stop after one bounded page. Stores without native paging use a
// sorted compatibility fallback.
func listObjectPage(ctx context.Context, objects ObjectStore, prefix string, after string, limit int) ([]ObjectInfo, string, error) {
	if limit <= 0 {
		items, err := objects.List(ctx, prefix)
		if err != nil {
			return nil, "", err
		}
		sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
		start := sort.Search(len(items), func(i int) bool { return items[i].Key > after })
		return items[start:], "", nil
	}
	switch store := objects.(type) {
	case *WriterObjectCache:
		return listObjectPage(ctx, store.Inner, prefix, after, limit)
	case *MeteredObjectStore:
		done := store.start("list_page")
		items, next, err := listObjectPage(ctx, store.Inner, prefix, after, limit)
		done(err)
		return items, next, err
	case *ReadProtectedObjectStore:
		release, err := store.acquireRead(ctx)
		if err != nil {
			return nil, "", err
		}
		defer release()
		return listObjectPage(ctx, store.Inner, prefix, after, limit)
	case *DelayedReadObjectStore:
		if err := store.wait(ctx); err != nil {
			return nil, "", err
		}
		return listObjectPage(ctx, store.Inner, prefix, after, limit)
	}
	if paged, ok := objects.(objectPageLister); ok {
		return paged.ListPage(ctx, prefix, after, limit)
	}
	items, err := objects.List(ctx, prefix)
	if err != nil {
		return nil, "", err
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	start := sort.Search(len(items), func(i int) bool { return items[i].Key > after })
	items = items[start:]
	if limit <= 0 || len(items) <= limit {
		return items, "", nil
	}
	page := append([]ObjectInfo(nil), items[:limit]...)
	return page, page[len(page)-1].Key, nil
}
