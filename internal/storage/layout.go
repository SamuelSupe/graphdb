package storage

import (
	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
)

func normalizeObjectAfterRead(value any, kind string) error {
	switch typed := value.(type) {
	case *Manifest:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *snapshotRecord:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *ShardedSnapshotCatalog:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *snapshotSchemaData:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *graph.Commit:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *IndexCatalog:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *IndexDefinitionRecord:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *SecondaryIndex:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *EdgeShardData:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *EntityPageData:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *EntityRecord:
		version, err := codec.ReadableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	}
	return nil
}
