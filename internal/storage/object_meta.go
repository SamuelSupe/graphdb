package storage

import "context"

type objectHeadStore interface {
	Head(ctx context.Context, key string) (ObjectMeta, error)
}

func objectMeta(ctx context.Context, objects ObjectStore, key string) (ObjectMeta, error) {
	if head, ok := objects.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err := objects.GetWithMeta(ctx, key)
	return meta, err
}
