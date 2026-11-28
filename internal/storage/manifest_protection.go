package storage

import "github.com/SamuelSupe/graphdb/v2/internal/storage/catalog"

type (
	ProtectedSnapshotSelector = catalog.ProtectedSnapshotSelector
	ProtectedObjectSet        = catalog.ProtectedObjectSet
)

// CollectReachableKeys is the single pure-function reachability truth layer for
// tenant objects. GC, Repair, and Integrity Audit derive protected keys through
// this function instead of duplicating ad-hoc field checks.
func CollectReachableKeys(
	manifest Manifest,
	snapshotCatalog *ShardedSnapshotCatalog,
	indexCatalog *IndexCatalog,
	reverseCatalog *ReverseIndexCatalog,
	extraKeys ...string,
) ProtectedObjectSet {
	var collected []string
	if snapshotCatalog != nil {
		collected = append(collected, snapshotCatalog.Key, snapshotCatalog.Schema.Key)
		for _, page := range snapshotCatalog.EntityPages {
			collected = append(collected, page.Key)
		}
		for _, shard := range snapshotCatalog.EdgeShards {
			collected = append(collected, shard.Key)
		}
	}
	if indexCatalog != nil {
		for _, spec := range indexCatalog.Indexes {
			for _, obj := range spec.Objects {
				collected = append(collected, obj.Key)
			}
		}
		for _, shard := range indexCatalog.EdgeShards {
			for _, obj := range shard.Objects {
				collected = append(collected, obj.Key)
			}
			if shard.AdjacencyRoot != nil {
				collected = append(collected, shard.AdjacencyRoot.Key)
			}
		}
		for _, page := range indexCatalog.EntityPages {
			for _, obj := range page.Objects {
				collected = append(collected, obj.Key)
			}
		}
	}
	if reverseCatalog != nil {
		for _, shard := range reverseCatalog.EdgeShards {
			for _, obj := range shard.Objects {
				collected = append(collected, obj.Key)
			}
			if shard.AdjacencyRoot != nil {
				collected = append(collected, shard.AdjacencyRoot.Key)
			}
		}
	}
	collected = append(collected, extraKeys...)
	return catalog.NewProtectedObjectSet(manifest, collected...)
}
