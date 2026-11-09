package codec

import "fmt"

const (
	LegacyObjectLayoutVersion  = 1
	CurrentObjectLayoutVersion = 2
)

func ReadableLayoutVersion(kind string, version int) (int, error) {
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
