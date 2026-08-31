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

func (s *TenantStore) PutCoordinationMarker(ctx context.Context, backend, namespace string) error {
	if backend == CoordinationPostgres {
		if err := requireS3ConditionalDelete(
			ctx, s.Objects, s.coordinationMarkerKey(),
		); err != nil {
			return err
		}
	}
	marker := coordinationMarker{
		LayoutVersion: coordinationMarkerLayoutVersion,
		Backend:       backend,
		Namespace:     namespace,
		UpdatedAt:     time.Now().UTC(),
	}
	data, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	key := s.coordinationMarkerKey()
	for attempt := 0; attempt < 4; attempt++ {
		current, _, err := s.Objects.GetWithMeta(ctx, key)
		if errors.Is(err, ErrNotFound) {
			_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
		} else if err == nil {
			existing, decodeErr := decodeCoordinationMarker(current)
			if decodeErr != nil {
				return decodeErr
			}
			if existing.Backend == backend && existing.Namespace == namespace {
				return nil
			}
			return fmt.Errorf(
				"%w: coordination marker already belongs to backend %q namespace %q",
				ErrConflict, existing.Backend, existing.Namespace,
			)
		}
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
		if err := retryDelay(ctx, attempt); err != nil {
			return err
		}
	}
	return fmt.Errorf("%w: coordination marker changed while publishing", ErrConflict)
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
	if marker.Backend == CoordinationPostgres {
		return fmt.Errorf("local writer is disabled: object prefix is managed by PostgreSQL coordinator namespace %q", marker.Namespace)
	}
	return nil
}

func (s *TenantStore) EnsurePostgresMarker(ctx context.Context) error {
	if !s.coordinated() {
		return nil
	}
	data, err := s.Objects.Get(ctx, s.coordinationMarkerKey())
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("PostgreSQL coordination marker is missing; run coordinator bootstrap --apply")
	}
	if err != nil {
		return err
	}
	marker, err := decodeCoordinationMarker(data)
	if err != nil {
		return err
	}
	if marker.Backend != CoordinationPostgres || marker.Namespace != s.Coordinator.Namespace() {
		return fmt.Errorf("coordination marker mismatch: backend=%q namespace=%q", marker.Backend, marker.Namespace)
	}
	s.coordinationMarkerVerified.Store(true)
	return nil
}
