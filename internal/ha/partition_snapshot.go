package ha

import (
	"context"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func (a *Application) validatePartitionSnapshot(ctx context.Context, objects storage.ObjectStore) error {
	if a.ShardID == "" {
		return nil
	}
	return a.partitionStoreFor(objects).ValidateSnapshot(ctx, a.ShardID)
}
