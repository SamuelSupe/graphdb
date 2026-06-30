package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"
)

type ReaderHeartbeat struct {
	ReaderID        string    `json:"reader_id"`
	InstanceID      string    `json:"instance_id,omitempty"`
	TenantID        string    `json:"tenant_id"`
	Mode            string    `json:"mode,omitempty"`
	Status          string    `json:"status"`
	Fresh           bool      `json:"fresh"`
	Consistent      bool      `json:"consistent"`
	ManifestVersion int64     `json:"manifest_version"`
	SnapshotVersion int64     `json:"snapshot_version,omitempty"`
	VisibleVersion  int64     `json:"visible_version"`
	VersionLag      int64     `json:"version_lag"`
	LagMS           int64     `json:"lag_ms"`
	LastSeenAt      time.Time `json:"last_seen_at"`
}

func (s *TenantStore) PutReaderHeartbeat(ctx context.Context, tenantID string, heartbeat ReaderHeartbeat) (ReaderHeartbeat, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return ReaderHeartbeat{}, err
	}
	heartbeat.TenantID = tenantID
	if heartbeat.ReaderID == "" {
		heartbeat.ReaderID = s.InstanceID
	}
	if heartbeat.ReaderID == "" {
		return ReaderHeartbeat{}, fmt.Errorf("reader id is required")
	}
	if heartbeat.InstanceID == "" {
		heartbeat.InstanceID = heartbeat.ReaderID
	}
	if heartbeat.LastSeenAt.IsZero() {
		heartbeat.LastSeenAt = time.Now().UTC()
	}
	data, err := marshalParquetReaderHeartbeat(ctx, heartbeat)
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	if err := s.Objects.Put(ctx, s.readerHeartbeatKey(tenantID, heartbeat.ReaderID), data); err != nil {
		return ReaderHeartbeat{}, err
	}
	return heartbeat, nil
}

func (s *TenantStore) ListReaderHeartbeats(ctx context.Context, tenantID string) ([]ReaderHeartbeat, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	objects, err := s.Objects.List(ctx, s.readerHeartbeatPrefix(tenantID))
	if err != nil {
		return nil, err
	}
	items := make([]ReaderHeartbeat, 0, len(objects))
	for _, object := range objects {
		readerID, ok := readerIDFromHeartbeatKey(object.Key)
		if !ok {
			continue
		}
		data, err := s.Objects.Get(ctx, s.readerHeartbeatKey(tenantID, readerID))
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if !isParquetBytes(data) {
			return nil, fmt.Errorf("unsupported reader heartbeat %q: only parquet heartbeats are readable", object.Key)
		}
		heartbeat, err := decodeParquetReaderHeartbeat(ctx, data)
		if err != nil {
			return nil, err
		}
		if heartbeat.TenantID != tenantID {
			return nil, fmt.Errorf("reader heartbeat tenant mismatch: path tenant %q contains tenant %q", tenantID, heartbeat.TenantID)
		}
		if heartbeat.ReaderID != readerID {
			return nil, fmt.Errorf("reader heartbeat id mismatch: path reader %q contains reader %q", readerID, heartbeat.ReaderID)
		}
		items = append(items, heartbeat)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].LastSeenAt.Equal(items[j].LastSeenAt) {
			return items[i].ReaderID < items[j].ReaderID
		}
		return items[i].LastSeenAt.After(items[j].LastSeenAt)
	})
	return items, nil
}

func (s *TenantStore) readerHeartbeatPrefix(tenantID string) string {
	return path.Join(s.Prefix, "tenants", tenantID, "control", "readers") + "/"
}

func (s *TenantStore) readerHeartbeatKey(tenantID string, readerID string) string {
	return path.Join(s.readerHeartbeatPrefix(tenantID), objectSegment(readerID)+".parquet")
}

func readerIDFromHeartbeatKey(key string) (string, bool) {
	name := path.Base(key)
	if !strings.HasSuffix(name, ".parquet") {
		return "", false
	}
	id, err := url.PathUnescape(strings.TrimSuffix(name, ".parquet"))
	if err != nil || id == "" {
		return "", false
	}
	return id, true
}
