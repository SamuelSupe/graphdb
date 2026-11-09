package ha

import (
	"context"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (a *Application) validatePartitionSnapshot(ctx context.Context, objects storage.ObjectStore) error {
	if a.ShardID == "" {
		return nil
	}
	return (partition.Store{Objects: objects, Prefix: a.Store.Prefix}).ValidateSnapshot(ctx, a.ShardID)
}
