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
const packedReceiptMarkerLayoutVersion = 2

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
	if marker.LayoutVersion != coordinationMarkerLayoutVersion &&
		(marker.LayoutVersion != packedReceiptMarkerLayoutVersion || marker.Backend != "local") {
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
	return s.ensureLocalWriterAllowed(ctx, s.LocalReceiptPackingEnabled())
}

// An offline migration may copy an intact packed database as a physical file.
func (s *TenantStore) EnsureLocalMigrationAllowed(ctx context.Context) error {
	if files := s.localFileStore(); files != nil {
		if err := files.CheckStandaloneDirectory(); err != nil {
			return err
		}
	}
	return s.ensureLocalWriterAllowed(ctx, true)
}

func (s *TenantStore) ensureLocalWriterAllowed(ctx context.Context, allowPacked bool) error {
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
	if marker.LayoutVersion == packedReceiptMarkerLayoutVersion && !allowPacked {
		return fmt.Errorf("packed receipts require standalone storage with receipt packing enabled")
	}
	return nil
}

// The existing marker is checked by older binaries before they serve requests.
// Publish it before opening a pack or removing any legacy receipt.
func (s *FileStore) EnableLocalReceiptPacking(ctx context.Context, prefix string) error {
	if !s.Exclusive() {
		return fmt.Errorf("packed receipts require exclusive data-directory ownership")
	}
	if err := s.ensurePackedReceiptMarker(ctx, prefix); err != nil {
		return err
	}
	s.receiptPrefix = path.Join(cleanPrefix(prefix), "tenants") + "/"
	return nil
}

func (s *FileStore) ensurePackedReceiptMarker(ctx context.Context, prefix string) error {
	key := path.Join(cleanPrefix(prefix), "coordination", "mode.json")
	data, meta, err := s.GetWithMeta(ctx, key)
	if err == nil {
		marker, err := decodeCoordinationMarker(data)
		if err != nil {
			return err
		}
		if marker.Backend != "local" {
			return fmt.Errorf("cannot enable packed receipts for %q coordination", marker.Backend)
		}
		if marker.LayoutVersion == packedReceiptMarkerLayoutVersion {
			return nil
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	marker := coordinationMarker{LayoutVersion: packedReceiptMarkerLayoutVersion, Backend: "local", UpdatedAt: time.Now().UTC()}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return err
	}
	if _, err := s.PutConditional(ctx, key, encoded, PutCondition{IfMatch: meta.ETag, IfNoneMatch: !meta.Exists}); err != nil {
		return fmt.Errorf("protect packed receipts from older binaries: %w", err)
	}
	return nil
}
