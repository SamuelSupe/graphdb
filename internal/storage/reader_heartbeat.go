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

const (
	readerHeartbeatWriteInterval = 5 * time.Second
	readerHeartbeatCacheLimit    = 4096
	readerHeartbeatListLimit     = 4096
	readerHeartbeatScanLimit     = 512
	readerHeartbeatDefaultMaxAge = 15 * time.Minute
)

var errReaderHeartbeatScanIncomplete = errors.New("reader heartbeat scan incomplete")

type cachedReaderHeartbeat struct {
	heartbeat ReaderHeartbeat
	writtenAt time.Time
}

type ReaderHeartbeatListOptions struct {
	MaxAge        time.Duration
	Limit         int
	ScanLimit     int
	DeleteExpired bool
}

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
		heartbeat.ReaderID = s.ReaderHeartbeatID()
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
	cacheKey := tenantID + "\x00" + heartbeat.ReaderID
	if s.readerHeartbeatWriteCached(cacheKey, heartbeat, time.Now()) {
		return heartbeat, nil
	}
	data, err := marshalParquetReaderHeartbeat(ctx, heartbeat)
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	key := s.readerHeartbeatKey(tenantID, heartbeat.ReaderID)
	if err := s.putTenantGenerationObject(ctx, tenantID, key, data); err != nil {
		return ReaderHeartbeat{}, err
	}
	s.cacheReaderHeartbeatWrite(cacheKey, heartbeat, time.Now())
	return heartbeat, nil
}

func (s *TenantStore) ListReaderHeartbeats(ctx context.Context, tenantID string) ([]ReaderHeartbeat, error) {
	return s.ListReaderHeartbeatsWithOptions(ctx, tenantID, ReaderHeartbeatListOptions{
		MaxAge:        readerHeartbeatDefaultMaxAge,
		Limit:         readerHeartbeatListLimit,
		ScanLimit:     readerHeartbeatScanLimit,
		DeleteExpired: true,
	})
}

func (s *TenantStore) ListReaderHeartbeatsWithOptions(ctx context.Context, tenantID string, options ReaderHeartbeatListOptions) ([]ReaderHeartbeat, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return nil, err
	}
	if options.Limit <= 0 {
		options.Limit = readerHeartbeatListLimit
	}
	if options.ScanLimit <= 0 {
		options.ScanLimit = readerHeartbeatScanLimit
	}
	items := make([]ReaderHeartbeat, 0, min(256, options.Limit))
	now := time.Now().UTC()
	cursor := ""
	scanned := 0
	for {
		pageLimit := min(256, options.ScanLimit-scanned)
		if pageLimit <= 0 {
			return nil, fmt.Errorf("%w after %d records for tenant %q", errReaderHeartbeatScanIncomplete, scanned, tenantID)
		}
		objects, next, err := listObjectPage(ctx, s.Objects, s.readerHeartbeatPrefix(tenantID), cursor, pageLimit)
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			scanned++
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
			expired := options.MaxAge > 0 && !heartbeat.LastSeenAt.IsZero() && now.Sub(heartbeat.LastSeenAt) > options.MaxAge
			if expired && options.DeleteExpired {
				err := s.Objects.DeleteConditional(ctx, object.Key, PutCondition{IfMatch: object.ETag})
				if err != nil && !errors.Is(err, ErrNotFound) && !errors.Is(err, ErrConflict) && !errors.Is(err, ErrConditionalDeleteUnsupported) {
					return nil, err
				}
				if err == nil || errors.Is(err, ErrNotFound) {
					s.deleteReaderHeartbeatWrite(tenantID + "\x00" + readerID)
				}
				continue
			}
			items = append(items, heartbeat)
			if len(items) > options.Limit {
				return nil, fmt.Errorf("reader heartbeat limit exceeded: more than %d records for tenant %q", options.Limit, tenantID)
			}
		}
		if next == "" {
			break
		}
		if scanned >= options.ScanLimit {
			return nil, fmt.Errorf("%w after %d records for tenant %q", errReaderHeartbeatScanIncomplete, scanned, tenantID)
		}
		cursor = next
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].LastSeenAt.Equal(items[j].LastSeenAt) {
			return items[i].ReaderID < items[j].ReaderID
		}
		return items[i].LastSeenAt.After(items[j].LastSeenAt)
	})
	return items, nil
}

func (s *TenantStore) ReaderHeartbeatID() string {
	if s.ReaderID != "" {
		return s.ReaderID
	}
	return s.InstanceID
}

func (s *TenantStore) readerHeartbeatWriteCached(key string, heartbeat ReaderHeartbeat, now time.Time) bool {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	cached, ok := s.readerHeartbeatCache[key]
	return ok && now.Sub(cached.writtenAt) < readerHeartbeatWriteInterval && readerHeartbeatStateEqual(cached.heartbeat, heartbeat)
}

func (s *TenantStore) cacheReaderHeartbeatWrite(key string, heartbeat ReaderHeartbeat, now time.Time) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if _, exists := s.readerHeartbeatCache[key]; !exists && len(s.readerHeartbeatCache) >= readerHeartbeatCacheLimit {
		for candidateKey := range s.readerHeartbeatCache {
			delete(s.readerHeartbeatCache, candidateKey)
			break
		}
	}
	s.readerHeartbeatCache[key] = cachedReaderHeartbeat{heartbeat: heartbeat, writtenAt: now}
}

func (s *TenantStore) deleteReaderHeartbeatWrite(key string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	delete(s.readerHeartbeatCache, key)
}

func readerHeartbeatStateEqual(left ReaderHeartbeat, right ReaderHeartbeat) bool {
	left.LastSeenAt = time.Time{}
	right.LastSeenAt = time.Time{}
	return left == right
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
