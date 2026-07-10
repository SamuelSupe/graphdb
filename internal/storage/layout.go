package storage

import (
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const (
	LegacyObjectLayoutVersion  = 1
	CurrentObjectLayoutVersion = 2
)

func readableLayoutVersion(kind string, version int) (int, error) {
	if version == 0 {
		return LegacyObjectLayoutVersion, nil
	}
	if version > CurrentObjectLayoutVersion {
		return version, fmt.Errorf("%s layout version %d is newer than supported version %d", kind, version, CurrentObjectLayoutVersion)
	}
	if version < LegacyObjectLayoutVersion {
		return version, fmt.Errorf("%s layout version %d is invalid", kind, version)
	}
	return version, nil
}

func normalizeObjectAfterRead(value any, kind string) error {
	switch typed := value.(type) {
	case *Manifest:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *snapshotRecord:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *ShardedSnapshotCatalog:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *snapshotSchemaData:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *graph.Commit:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *IndexCatalog:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *IndexDefinitionRecord:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *SecondaryIndex:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *EdgeShardData:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *EntityPageData:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	case *EntityRecord:
		version, err := readableLayoutVersion(kind, typed.LayoutVersion)
		if err != nil {
			return err
		}
		typed.LayoutVersion = version
	}
	return nil
}
