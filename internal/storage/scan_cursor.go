package storage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

func parseScanCursor(raw string, version int64, queryHash string) (scanCursor, error) {
	if raw == "" {
		return scanCursor{}, nil
	}
	cursor, err := decodeScanCursor(raw)
	if err != nil {
		return scanCursor{}, err
	}
	if cursor.Version != 0 && cursor.Version != version {
		return scanCursor{}, fmt.Errorf("cursor version %d does not match current version %d", cursor.Version, version)
	}
	if cursor.Query != "" && cursor.Query != queryHash {
		return scanCursor{}, fmt.Errorf("cursor query does not match request")
	}
	return cursor, nil
}

func scanCursorPinnedCatalog(raw string) (int64, string, bool, error) {
	if raw == "" {
		return 0, "", false, nil
	}
	cursor, err := decodeScanCursor(raw)
	if err != nil {
		return 0, "", false, err
	}
	if cursor.Version <= 0 {
		return 0, "", false, nil
	}
	return cursor.Version, cursor.CatalogHash, true, nil
}

func decodeScanCursor(raw string) (scanCursor, error) {
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return scanCursor{}, fmt.Errorf("invalid cursor")
	}
	var cursor scanCursor
	if err := json.Unmarshal(data, &cursor); err != nil {
		return scanCursor{}, fmt.Errorf("invalid cursor")
	}
	return cursor, nil
}

func encodeScanCursor(cursor scanCursor) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

// PinScanCursor binds a graph-produced page to the immutable catalog for the
// same version, so subsequent requests can continue after the graph advances.
func PinScanCursor(raw string, catalog IndexCatalog) (string, error) {
	if raw == "" {
		return "", nil
	}
	hash, err := indexCatalogContentHash(catalog)
	if err != nil {
		return "", err
	}
	return (ScanCursorBinding{version: catalog.Version, contentHash: hash}).Pin(raw)
}

// ScanCursorBinding pins cursors to a verified catalog without exposing mutable
// catalog fields. A zero binding cannot pin a nonempty cursor.
type ScanCursorBinding struct {
	version     int64
	contentHash string
	generation  int64
}

// ValidateScanCursor rejects local cursors from a replaced tenant. The caller
// retains a read view until the page is complete and pins its outgoing cursor
// with the returned binding. Legacy cursors are accepted in generation one.
func (s *TenantStore) ValidateScanCursor(ctx context.Context, tenantID, raw string) (ScanCursorBinding, error) {
	generation, err := s.localIngestGeneration(ctx, tenantID)
	if err != nil {
		return ScanCursorBinding{}, err
	}
	if generation != 0 && raw != "" {
		cursor, err := decodeScanCursor(raw)
		if err != nil {
			return ScanCursorBinding{}, err
		}
		expected := cursor.Generation
		if expected == 0 {
			expected = 1
		}
		if expected != generation {
			return ScanCursorBinding{}, fmt.Errorf("cursor tenant generation is no longer available")
		}
	}
	return ScanCursorBinding{generation: generation}, nil
}

// PinGeneration also binds graph-fallback cursors, which have no index catalog.
func (binding ScanCursorBinding) PinGeneration(raw string) (string, error) {
	if raw == "" || binding.generation == 0 {
		return raw, nil
	}
	cursor, err := decodeScanCursor(raw)
	if err != nil {
		return "", err
	}
	cursor.Generation = binding.generation
	return encodeScanCursor(cursor), nil
}

// GetScanCursorBinding requires the current catalog to match the borrowed graph
// version. The binding remains valid after that catalog is replaced.
func (s *TenantStore) GetScanCursorBinding(ctx context.Context, tenantID string, version int64) (ScanCursorBinding, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return ScanCursorBinding{}, err
	}
	if err := ctx.Err(); err != nil {
		return ScanCursorBinding{}, err
	}
	binding, err := s.ValidateScanCursor(ctx, tenantID, "")
	if err != nil {
		return ScanCursorBinding{}, err
	}
	s.lockMu.Lock()
	cached, ok := s.indexCatalogCache[tenantID]
	fresh := ok && time.Since(cached.checkedAt) < s.lifecycleCacheTTL()
	if fresh && (!cached.meta.Exists || cached.catalog.Version != version) {
		s.lockMu.Unlock()
		return ScanCursorBinding{}, ErrNotFound
	}
	if fresh && cached.meta.Exists && cached.contentHash != "" && cached.catalog.Version == version {
		binding.version, binding.contentHash = version, cached.contentHash
		s.lockMu.Unlock()
		return binding, nil
	}
	s.lockMu.Unlock()
	catalog, _, err := s.loadIndexCatalog(ctx, tenantID)
	if err != nil {
		return ScanCursorBinding{}, err
	}
	if catalog.Version != version || catalog.contentHash == "" {
		return ScanCursorBinding{}, ErrNotFound
	}
	binding.version, binding.contentHash = version, catalog.contentHash
	return binding, nil
}

// Pin preserves the query and position and rejects a different graph version.
func (binding ScanCursorBinding) Pin(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if binding.contentHash == "" {
		return "", fmt.Errorf("scan cursor binding is empty")
	}
	cursor, err := decodeScanCursor(raw)
	if err != nil {
		return "", err
	}
	if cursor.Version != binding.version {
		return "", fmt.Errorf("cursor version %d does not match catalog version %d", cursor.Version, binding.version)
	}
	cursor.CatalogHash = binding.contentHash
	cursor.Generation = binding.generation
	return encodeScanCursor(cursor), nil
}

func entityScanQueryHash(options EntityScanOptions) string {
	options.Cursor = ""
	options.Limit = 0
	options.MinVersion = 0
	options.SkipGraphFallback = false
	return scanQueryHash(options)
}

func edgeScanQueryHash(options EdgeScanOptions) string {
	options.Cursor = ""
	options.Limit = 0
	options.MinVersion = 0
	options.SkipGraphFallback = false
	return scanQueryHash(options)
}

func scanQueryHash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

func normalizedScanLimit(limit int) int {
	if limit <= 0 {
		return defaultScanLimit
	}
	if limit > maxScanLimit {
		return maxScanLimit
	}
	return limit
}

func scanKey(group string, id string) string {
	return group + "\x00" + id
}
