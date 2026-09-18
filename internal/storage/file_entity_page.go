package storage

import (
	"context"
	"errors"
)

func (s *TenantStore) loadLocalEntityPage(ctx context.Context, tenantID string, version int64, spec EntityPageSpec, options EntityScanOptions, cursor scanCursor) (loaded loadedEntityScanPage, err error) {
	key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
	reader, err := openFileReader(ctx, s.Objects, key)
	if errors.Is(err, ErrNotFound) {
		return loaded, nil
	}
	if err != nil {
		return loaded, err
	}
	defer reader.Close()
	loaded.available = true
	loaded.meta = ObjectMeta{Key: key, Exists: true}
	if shouldScanParquetEntityCandidates(options, cursor) {
		// Parquet readers own their source. Use a borrowed handle so the
		// candidate projection cannot close the file before the page decode.
		scan, err := scanParquetEntityObjectCandidatesReader(ctx, borrowedParquetSource{reader}, options)
		if err != nil {
			return loaded, err
		}
		loaded.candidates = filterParquetEntityCandidates(scan, spec.Shard, cursor)
		if len(loaded.candidates) == 0 {
			loaded.skip = true
			return loaded, nil
		}
	}
	loaded.page, err = decodeParquetEntityPageReader(ctx, borrowedParquetSource{reader}, tenantID, spec.Shard, 0)
	return loaded, err
}

// Expose only the read/seek methods; ownership stays with the enclosing operation.
type borrowedParquetSource struct{ fileReader }

func (borrowedParquetSource) Close() error { return nil }
