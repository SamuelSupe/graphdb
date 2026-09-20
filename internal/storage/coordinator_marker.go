package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"time"
)

const coordinationMarkerLayoutVersion = 1

type coordinationMarker struct {
	LayoutVersion int       `json:"layout_version"`
	Backend       string    `json:"backend"`
	Namespace     string    `json:"namespace"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func decodeCoordinationMarker(data []byte) (coordinationMarker, error) {
	var marker coordinationMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return coordinationMarker{}, fmt.Errorf("decode coordination marker: %w", err)
	}
	if marker.LayoutVersion != coordinationMarkerLayoutVersion {
		return coordinationMarker{}, fmt.Errorf(
			"unsupported coordination marker layout version %d",
			marker.LayoutVersion,
		)
	}
	return marker, nil
}

func (s *TenantStore) coordinationMarkerKey() string {
	return path.Join(s.Prefix, "coordination", "mode.json")
}

func (s *TenantStore) EnsureLocalWriterAllowed(ctx context.Context) error {
	data, err := s.Objects.Get(ctx, s.coordinationMarkerKey())
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	marker, err := decodeCoordinationMarker(data)
	if err != nil {
		return err
	}
	if marker.Backend == "postgres" {
		return fmt.Errorf("local writer is disabled: object prefix is managed by PostgreSQL coordinator namespace %q", marker.Namespace)
	}
	return nil
}
