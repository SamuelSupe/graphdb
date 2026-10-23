package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/apache/arrow-go/v18/parquet"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
)

func edgeRangeMayMatch(source parquet.ReaderAtSeeker, options EdgeScanOptions) (bool, error) {
	reader, err := pqfile.NewParquetReader(source)
	if err != nil {
		return false, err
	}
	defer reader.Close()
	if reader.MetaData().Schema.NumColumns() < parquetEdgeColumnEdgeSourceObservedAt+1 {
		return false, fmt.Errorf("invalid parquet edge schema")
	}
	if reader.NumRowGroups() == 0 {
		return false, fmt.Errorf("empty parquet edge range")
	}
	for i := 0; i < reader.NumRowGroups(); i++ {
		if options.From != "" && !parquetRowGroupStringMayContain(reader, i, parquetEdgeColumnFrom, options.From) {
			continue
		}
		// Source matches the base, existence, or provenance source. Missing
		// statistics remain conservative through parquetRowGroupStringMayContain.
		if options.Source != "" && !parquetRowGroupStringMayContain(reader, i, parquetEdgeColumnSource, options.Source) &&
			!parquetRowGroupStringMayContain(reader, i, parquetEdgeColumnFieldSourceSource, options.Source) &&
			!parquetRowGroupStringMayContain(reader, i, parquetEdgeColumnEdgeSourceSource, options.Source) {
			continue
		}
		return true, nil
	}
	return false, nil
}

func (s *TenantStore) scanEdgeRange(ctx context.Context, tenant string, version int64, spec EdgeShard, n int, options EdgeScanOptions) ([]graph.Edge, bool, error) {
	o := spec.Objects[n]
	cacheKey := edgeRangeRowsCacheKey(tenant, version, spec, o)
	if options.From == "" && options.Source == "" || s.edgeLookupCache.has(cacheKey) {
		return s.borrowEdgeRange(ctx, tenant, version, spec, n)
	}
	part := edgeRangeSpec(spec, o)
	if exclusiveFileStore(s.Objects) != nil {
		reader, err := openFileReader(ctx, s.Objects, o.Key)
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		mayMatch, err := edgeRangeMayMatch(reader, options)
		reader.Close()
		if err != nil || !mayMatch {
			return nil, err == nil, err
		}
		return s.borrowEdgeRange(ctx, tenant, version, spec, n)
	}
	// Reuse only compact footer statistics for rejected pages. Keeping their
	// payload would displace hot data and synchronously write megabytes to disk.
	footer, _, hasFooter, err := s.cachedIndexObject(ctx, "edge_footer", tenant, version, o.Key, o.ContentHash, o.SchemaHash)
	if err != nil {
		return nil, false, err
	}
	if hasFooter {
		matches, err := edgeRangeMayMatch(bytes.NewReader(footer), options)
		if err == nil && !matches {
			return nil, true, nil
		}
		if err != nil {
			s.dropCachedIndexObject("edge_footer", tenant, version, o.Key, o.ContentHash, o.SchemaHash)
		}
	}
	data, meta, cached, err := s.cachedIndexObject(ctx, "edge_shard", tenant, version, o.Key, o.ContentHash, o.SchemaHash)
	if err != nil {
		return nil, false, err
	}
	if !cached {
		data, meta, err = s.Objects.GetWithMeta(ctx, o.Key)
		if errors.Is(err, ErrNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
	}
	mayMatch, err := edgeRangeMayMatch(bytes.NewReader(data), options)
	if err == nil && !mayMatch {
		if footer := compactParquetFooter(data); len(footer) > 0 {
			s.putCachedIndexObject("edge_footer", tenant, version, o.Key, o.ContentHash, o.SchemaHash, footer, meta)
		}
		return nil, true, nil
	}
	if err != nil {
		if cached {
			s.dropCachedIndexObject("edge_shard", tenant, version, o.Key, o.ContentHash, o.SchemaHash)
			return s.borrowEdgeRange(ctx, tenant, version, spec, n)
		}
		return nil, false, err
	}
	decoded, ok, err := s.edgeLookupCache.load(ctx, cacheKey, func() (map[string][]graph.Edge, bool, error) {
		shard, err := decodeParquetEdgeShard(ctx, data, tenant, part.RelationType, part.Shard, 0)
		if err != nil {
			return nil, false, err
		}
		if !edgeShardObjectMatchesCatalog(shard, part, version) || !indexTenantMatches(shard.TenantID, tenant) {
			return nil, false, fmt.Errorf("%w: invalid index edge range", ErrNotFound)
		}
		s.putVerifiedCachedIndexObject("edge_shard", tenant, version, o.Key, o.ContentHash, o.SchemaHash, data, meta)
		return map[string][]graph.Edge{"": shard.Edges}, true, nil
	})
	if err != nil || !ok {
		if cached && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			s.dropCachedIndexObject("edge_shard", tenant, version, o.Key, o.ContentHash, o.SchemaHash)
			return s.borrowEdgeRange(ctx, tenant, version, spec, n)
		}
		return nil, false, err
	}
	rows := decoded[""]
	err = validateEdgeRangeRows(rows, o)
	return rows, err == nil, err
}

// Parquet statistics are in the footer. Column payload offsets are irrelevant
// to pruning, which never opens a column reader against this compact view.
func compactParquetFooter(data []byte) []byte {
	if len(data) < 12 || string(data[:4]) != "PAR1" || string(data[len(data)-4:]) != "PAR1" {
		return nil
	}
	size := int64(binary.LittleEndian.Uint32(data[len(data)-8:])) + 8
	if size > 256<<10 || size > int64(len(data)-4) {
		return nil
	}
	footer := make([]byte, 4+int(size))
	copy(footer, "PAR1")
	copy(footer[4:], data[len(data)-int(size):])
	return footer
}
