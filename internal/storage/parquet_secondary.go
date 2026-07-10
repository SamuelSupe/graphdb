package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"graphdb/internal/query"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	parquetSecondaryColumnTenantID = iota
	parquetSecondaryColumnKind
	parquetSecondaryColumnField
	parquetSecondaryColumnUnique
	parquetSecondaryColumnVersion
	parquetSecondaryColumnUpdatedAt
	parquetSecondaryColumnValue
	parquetSecondaryColumnEntityID
)

func (s *TenantStore) writeParquetSecondaryIndexes(ctx context.Context, tenantID string, indexes []SecondaryIndex) error {
	return s.writeParquetSecondaryIndexesWithOptions(ctx, tenantID, indexes, true)
}

func (s *TenantStore) writeParquetSecondaryIndexesFast(ctx context.Context, tenantID string, indexes []SecondaryIndex) error {
	return s.writeParquetSecondaryIndexesWithOptions(ctx, tenantID, indexes, false)
}

func (s *TenantStore) writeParquetSecondaryIndexesWithOptions(ctx context.Context, tenantID string, indexes []SecondaryIndex, checkExisting bool) error {
	for _, index := range indexes {
		index.TenantID = tenantID
		groups := secondaryIndexObjectGroups(index)
		if len(groups) == 0 {
			if err := s.putParquetSecondaryIndex(ctx, tenantID, index, checkExisting); err != nil {
				return err
			}
			continue
		}
		for _, group := range groups {
			group.Index.TenantID = tenantID
			if err := s.putParquetSecondaryIndexShard(ctx, tenantID, group.ID, group.Index, checkExisting); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *TenantStore) putParquetSecondaryIndex(ctx context.Context, tenantID string, index SecondaryIndex, checkExisting bool) error {
	key := s.parquetSecondaryIndexVersionKey(tenantID, index.Version, index.Kind, index.Field)
	return s.putParquetSecondaryIndexObject(ctx, key, tenantID, index, checkExisting)
}

func (s *TenantStore) putParquetSecondaryIndexShard(ctx context.Context, tenantID string, shardID string, index SecondaryIndex, checkExisting bool) error {
	key := s.parquetSecondaryIndexShardVersionKey(tenantID, index.Version, index.Kind, index.Field, shardID)
	return s.putParquetSecondaryIndexObject(ctx, key, tenantID, index, checkExisting)
}

func (s *TenantStore) putParquetSecondaryIndexObject(ctx context.Context, key string, tenantID string, index SecondaryIndex, checkExisting bool) error {
	if checkExisting {
		if ok, err := s.parquetSecondaryIndexUnchanged(ctx, key, tenantID, index); err != nil || ok {
			return err
		}
	}
	data, err := marshalParquetSecondaryIndex(ctx, index)
	if err != nil {
		return err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		s.markObjectKeyCached(key)
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	return s.putConflictingParquetSecondaryIndexObject(ctx, key, tenantID, index, data)
}

func (s *TenantStore) putConflictingParquetSecondaryIndexObject(ctx context.Context, key string, tenantID string, index SecondaryIndex, data []byte) error {
	existing, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return err
	}
	decoded, decodeErr := decodeParquetSecondaryIndex(ctx, existing, tenantID, index.Kind, index.Field, index.Version, index.Unique)
	if decodeErr == nil && secondaryIndexContentHash(decoded) == secondaryIndexContentHash(index) {
		s.markObjectKeyCached(key)
		return nil
	}
	_, err = s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag})
	if err == nil {
		s.markObjectKeyCached(key)
	}
	return err
}

func (s *TenantStore) parquetSecondaryIndexUnchanged(ctx context.Context, key string, tenantID string, index SecondaryIndex) (bool, error) {
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil || !mayExist {
		return false, err
	}
	data, _, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	existing, err := decodeParquetSecondaryIndex(ctx, data, tenantID, index.Kind, index.Field, index.Version, index.Unique)
	if err != nil {
		return false, nil
	}
	return secondaryIndexContentHash(existing) == secondaryIndexContentHash(index), nil
}

func marshalParquetSecondaryIndex(ctx context.Context, index SecondaryIndex) ([]byte, error) {
	schema := parquetSecondaryIndexArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	values := make([]string, 0, len(index.Values))
	for value := range index.Values {
		values = append(values, value)
	}
	sort.Strings(values)
	for _, value := range values {
		ids := append([]string(nil), index.Values[value]...)
		sort.Strings(ids)
		for _, id := range ids {
			builder.Field(parquetSecondaryColumnTenantID).(*array.StringBuilder).Append(index.TenantID)
			builder.Field(parquetSecondaryColumnKind).(*array.StringBuilder).Append(index.Kind)
			builder.Field(parquetSecondaryColumnField).(*array.StringBuilder).Append(index.Field)
			builder.Field(parquetSecondaryColumnUnique).(*array.BooleanBuilder).Append(index.Unique)
			builder.Field(parquetSecondaryColumnVersion).(*array.Int64Builder).Append(index.Version)
			builder.Field(parquetSecondaryColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(index.UpdatedAt))
			builder.Field(parquetSecondaryColumnValue).(*array.StringBuilder).Append(value)
			builder.Field(parquetSecondaryColumnEntityID).(*array.StringBuilder).Append(id)
		}
	}
	record := builder.NewRecordBatch()
	defer record.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
	defer table.Release()

	var buf bytes.Buffer
	rowGroupSize := table.NumRows()
	if rowGroupSize < 1 {
		rowGroupSize = 1
	}
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, rowGroupSize, writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetSecondaryIndex(ctx context.Context, data []byte, tenantID string, kind string, field string, version int64, unique bool) (SecondaryIndex, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return SecondaryIndex{}, err
	}
	defer table.Release()
	if table.NumCols() < int64(parquetSecondaryColumnEntityID+1) {
		return SecondaryIndex{}, fmt.Errorf("parquet secondary index has %d columns, want at least %d", table.NumCols(), parquetSecondaryColumnEntityID+1)
	}
	index := SecondaryIndex{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      tenantID,
		Kind:          kind,
		Field:         field,
		Unique:        unique,
		Values:        map[string][]string{},
		Version:       version,
	}
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		record := reader.RecordBatch()
		if record.NumCols() < int64(parquetSecondaryColumnEntityID+1) {
			return SecondaryIndex{}, fmt.Errorf("parquet secondary record has %d columns, want at least %d", record.NumCols(), parquetSecondaryColumnEntityID+1)
		}
		tenantColumn, err := parquetStringColumn(record, parquetSecondaryColumnTenantID, "tenant_id")
		if err != nil {
			return SecondaryIndex{}, err
		}
		kindColumn, err := parquetStringColumn(record, parquetSecondaryColumnKind, "kind")
		if err != nil {
			return SecondaryIndex{}, err
		}
		fieldColumn, err := parquetStringColumn(record, parquetSecondaryColumnField, "field")
		if err != nil {
			return SecondaryIndex{}, err
		}
		uniqueColumn, err := parquetBoolColumn(record, parquetSecondaryColumnUnique, "unique")
		if err != nil {
			return SecondaryIndex{}, err
		}
		versionColumn, err := parquetInt64Column(record, parquetSecondaryColumnVersion, "version")
		if err != nil {
			return SecondaryIndex{}, err
		}
		updatedAtColumn, err := parquetStringColumn(record, parquetSecondaryColumnUpdatedAt, "updated_at")
		if err != nil {
			return SecondaryIndex{}, err
		}
		valueColumn, err := parquetStringColumn(record, parquetSecondaryColumnValue, "value")
		if err != nil {
			return SecondaryIndex{}, err
		}
		entityIDColumn, err := parquetStringColumn(record, parquetSecondaryColumnEntityID, "entity_id")
		if err != nil {
			return SecondaryIndex{}, err
		}
		for i := 0; i < int(record.NumRows()); i++ {
			rowTenant := tenantColumn.Value(i)
			if rowTenant != "" {
				if index.TenantID == "" || index.TenantID == tenantID {
					index.TenantID = rowTenant
				} else if index.TenantID != rowTenant {
					return SecondaryIndex{}, fmt.Errorf("parquet secondary index tenant mismatch")
				}
			}
			rowKind := kindColumn.Value(i)
			if rowKind != "" {
				if index.Kind == "" || index.Kind == kind {
					index.Kind = rowKind
				} else if index.Kind != rowKind {
					return SecondaryIndex{}, fmt.Errorf("parquet secondary index kind mismatch")
				}
			}
			rowField := fieldColumn.Value(i)
			if rowField != "" {
				if index.Field == "" || index.Field == field {
					index.Field = rowField
				} else if index.Field != rowField {
					return SecondaryIndex{}, fmt.Errorf("parquet secondary index field mismatch")
				}
			}
			index.Unique = uniqueColumn.Value(i)
			rowVersion := versionColumn.Value(i)
			if rowVersion != 0 {
				if index.Version == 0 || index.Version == version {
					index.Version = rowVersion
				} else if index.Version != rowVersion {
					return SecondaryIndex{}, fmt.Errorf("parquet secondary index version mismatch")
				}
			}
			if index.UpdatedAt.IsZero() {
				index.UpdatedAt = parseParquetTime(updatedAtColumn.Value(i))
			}
			value := valueColumn.Value(i)
			index.Values[value] = append(index.Values[value], entityIDColumn.Value(i))
		}
	}
	for value := range index.Values {
		sort.Strings(index.Values[value])
	}
	return index, nil
}

func (l *PersistedIndexLookup) matchParquetFieldIndex(ctx context.Context, spec IndexSpec, values []any) ([]string, bool, error) {
	if spec.ContentHash == "" {
		return nil, false, nil
	}
	if shards, hasShards := secondaryIndexShardObjectMap(spec.Objects); hasShards {
		seen := map[string]struct{}{}
		for _, value := range values {
			key, ok := secondaryIndexValue(value)
			if !ok {
				return nil, false, nil
			}
			object, ok := secondaryIndexShardObjectForValue(shards, key)
			if !ok {
				continue
			}
			index, ok, err := l.Store.loadParquetSecondaryIndexShardObject(ctx, l.TenantID, l.Version, spec, object)
			if err != nil {
				if ctx.Err() != nil {
					return nil, false, err
				}
				return nil, false, nil
			}
			if !ok {
				return nil, false, nil
			}
			for _, id := range index.Values[key] {
				seen[id] = struct{}{}
			}
		}
		ids := make([]string, 0, len(seen))
		for id := range seen {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return ids, true, nil
	}
	index, ok, err := l.Store.loadParquetSecondaryIndexObject(ctx, l.TenantID, l.Version, spec)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if !ok {
		return nil, false, nil
	}
	if !indexTenantMatches(index.TenantID, l.TenantID) {
		return nil, false, nil
	}
	if !fieldIndexMatchesCatalog(index, spec, l.Version) {
		return nil, false, nil
	}
	seen := map[string]struct{}{}
	for _, value := range values {
		key, ok := secondaryIndexValue(value)
		if !ok {
			return nil, false, nil
		}
		for _, id := range index.Values[key] {
			seen[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, true, nil
}

func (l *PersistedIndexLookup) ScanFieldIndex(ctx context.Context, kind string, field string) (map[string][]string, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return nil, false, nil
	}
	spec, ok := l.catalogFieldSpec(kind, field)
	if !ok || specFormat(spec.Format) != IndexFormatParquet {
		return nil, false, nil
	}
	index, ok, err := l.Store.loadParquetSecondaryIndexObject(ctx, l.TenantID, l.Version, spec)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if !ok || !indexTenantMatches(index.TenantID, l.TenantID) || !fieldIndexMatchesCatalog(index, spec, l.Version) {
		return nil, false, nil
	}
	out := make(map[string][]string, len(index.Values))
	for value, ids := range index.Values {
		out[value] = append([]string(nil), ids...)
	}
	return out, true, nil
}

func (l *PersistedIndexLookup) ScanFieldIndexWithFilters(ctx context.Context, kind string, field string, filters []query.Filter) (map[string][]string, bool, error) {
	if l == nil || l.Catalog.Version != l.Version {
		return nil, false, nil
	}
	spec, ok := l.catalogFieldSpec(kind, field)
	if !ok || specFormat(spec.Format) != IndexFormatParquet {
		return nil, false, nil
	}
	if spec.ContentHash == "" {
		return l.ScanFieldIndex(ctx, kind, field)
	}
	objects, supported := secondaryIndexShardObjectsForFilters(spec.Objects, filters)
	if !supported {
		return l.ScanFieldIndex(ctx, kind, field)
	}
	if len(objects) == 0 {
		return map[string][]string{}, true, nil
	}
	out := map[string][]string{}
	for _, object := range uniqueIndexObjects(objects) {
		index, ok, err := l.Store.loadParquetSecondaryIndexShardObject(ctx, l.TenantID, l.Version, spec, object)
		if err != nil {
			if ctx.Err() != nil {
				return nil, false, err
			}
			return l.ScanFieldIndex(ctx, kind, field)
		}
		if !ok {
			return l.ScanFieldIndex(ctx, kind, field)
		}
		for value, ids := range index.Values {
			out[value] = append(out[value], ids...)
		}
	}
	for value := range out {
		sort.Strings(out[value])
	}
	return out, true, nil
}

func (s *TenantStore) loadParquetSecondaryIndexObject(ctx context.Context, tenantID string, version int64, spec IndexSpec) (SecondaryIndex, bool, error) {
	fallback := IndexObject{Role: "postings", Key: s.parquetSecondaryIndexVersionKey(tenantID, version, spec.Kind, spec.Field), ContentHash: spec.ContentHash, SchemaHash: spec.SchemaHash}
	if object, ok := indexObjectByRole(spec.Objects, "postings"); ok && object.Key != "" {
		if object.ContentHash == "" {
			object.ContentHash = spec.ContentHash
		}
		if object.SchemaHash == "" {
			object.SchemaHash = spec.SchemaHash
		}
		return s.loadParquetSecondaryIndexObjectByObject(ctx, tenantID, version, spec, object)
	}
	objects, ok := secondaryIndexShardObjects(spec.Objects)
	if !ok {
		if len(spec.Objects) == 0 {
			return s.loadParquetSecondaryIndexObjectByObject(ctx, tenantID, version, spec, fallback)
		}
		return SecondaryIndex{}, false, nil
	}
	return s.loadParquetSecondaryIndexObjects(ctx, tenantID, version, spec, objects)
}

func (s *TenantStore) loadParquetSecondaryIndexShardObject(ctx context.Context, tenantID string, version int64, spec IndexSpec, object IndexObject) (SecondaryIndex, bool, error) {
	index, ok, err := s.loadParquetSecondaryIndexObjectByObject(ctx, tenantID, version, spec, object)
	if err != nil || !ok {
		return index, ok, err
	}
	if !secondaryIndexObjectMatches(index, tenantID, version, spec, object.ContentHash) {
		return SecondaryIndex{}, false, nil
	}
	return index, true, nil
}

func (s *TenantStore) loadParquetSecondaryIndexObjectByObject(ctx context.Context, tenantID string, version int64, spec IndexSpec, object IndexObject) (SecondaryIndex, bool, error) {
	if object.Key == "" {
		return SecondaryIndex{}, false, nil
	}
	if data, _, verified, ok, err := s.cachedIndexObjectWithVerification(ctx, "secondary_index", tenantID, version, object.Key, object.ContentHash, object.SchemaHash); err != nil {
		return SecondaryIndex{}, false, err
	} else if ok {
		index, err := decodeParquetSecondaryIndex(ctx, data, tenantID, spec.Kind, spec.Field, 0, spec.Type == "unique")
		index.cacheVerified = verified
		if err == nil && secondaryIndexObjectMatches(index, tenantID, version, spec, object.ContentHash) {
			if !verified {
				s.markCachedIndexObjectVerified("secondary_index", tenantID, version, object.Key, object.ContentHash, object.SchemaHash)
			}
			index.cacheVerified = true
			return index, true, nil
		}
		s.dropCachedIndexObject("secondary_index", tenantID, version, object.Key, object.ContentHash, object.SchemaHash)
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, object.Key)
	if errors.Is(err, ErrNotFound) {
		return SecondaryIndex{}, false, nil
	}
	if err != nil {
		return SecondaryIndex{}, false, err
	}
	index, err := decodeParquetSecondaryIndex(ctx, data, tenantID, spec.Kind, spec.Field, 0, spec.Type == "unique")
	if err != nil {
		return SecondaryIndex{}, false, err
	}
	if secondaryIndexObjectMatches(index, tenantID, version, spec, object.ContentHash) {
		index.cacheVerified = true
		s.putVerifiedCachedIndexObject("secondary_index", tenantID, version, object.Key, object.ContentHash, object.SchemaHash, data, meta)
	}
	return index, true, nil
}

func (s *TenantStore) loadParquetSecondaryIndexObjects(ctx context.Context, tenantID string, version int64, spec IndexSpec, objects []IndexObject) (SecondaryIndex, bool, error) {
	objects = uniqueIndexObjects(objects)
	if len(objects) == 0 {
		return SecondaryIndex{}, false, nil
	}
	indexes := make([]SecondaryIndex, 0, len(objects))
	for _, object := range objects {
		index, ok, err := s.loadParquetSecondaryIndexShardObject(ctx, tenantID, version, spec, object)
		if err != nil || !ok {
			return SecondaryIndex{}, ok, err
		}
		indexes = append(indexes, index)
	}
	merged := mergeSecondaryIndexShards(indexes)
	return merged, true, nil
}

func secondaryIndexObjectMatches(index SecondaryIndex, tenantID string, version int64, spec IndexSpec, contentHash string) bool {
	if index.Kind != spec.Kind || index.Field != spec.Field {
		return false
	}
	if version == 0 {
		return true
	}
	if !indexTenantMatches(index.TenantID, tenantID) || index.Version > version {
		return false
	}
	return index.cacheVerified || (contentHash != "" && secondaryIndexContentHash(index) == contentHash)
}

func secondaryIndexShardObjectsForFilters(objects []IndexObject, filters []query.Filter) ([]IndexObject, bool) {
	shards, hasShards := secondaryIndexShardObjectMap(objects)
	if !hasShards || len(filters) == 0 {
		return nil, false
	}
	needed := map[string]IndexObject{}
	constrained := false
	for shardID, object := range shards {
		prefix, ok := secondaryIndexStringShardPrefix(shardID)
		if !ok {
			continue
		}
		match, used, supported := secondaryIndexStringShardMatchesFilters(prefix, filters)
		if !supported {
			return nil, false
		}
		if used {
			constrained = true
		}
		if match {
			needed[shardID] = object
		}
	}
	if !constrained {
		return nil, false
	}
	return sortedIndexObjects(needed), true
}

func secondaryIndexShardObjectMap(objects []IndexObject) (map[string]IndexObject, bool) {
	out := map[string]IndexObject{}
	for _, object := range objects {
		shardID, ok := secondaryIndexShardIDFromRole(object.Role)
		if !ok || object.Key == "" || object.ContentHash == "" {
			continue
		}
		out[shardID] = object
	}
	return out, len(out) > 0
}

func secondaryIndexShardObjectForValue(shards map[string]IndexObject, valueKey string) (IndexObject, bool) {
	if object, ok := shards[secondaryIndexShardID(valueKey)]; ok {
		return object, true
	}
	bestID := ""
	var best IndexObject
	for shardID, object := range shards {
		if !secondaryIndexShardMayContainValue(shardID, valueKey) {
			continue
		}
		if len(shardID) > len(bestID) {
			bestID = shardID
			best = object
		}
	}
	return best, bestID != ""
}

func secondaryIndexShardMayContainValue(shardID string, valueKey string) bool {
	switch {
	case shardID == "null":
		return valueKey == "null"
	case strings.HasPrefix(valueKey, "s:") && strings.HasPrefix(shardID, "s_"):
		valueID := cleanSecondaryIndexShardID("s_" + strings.ToLower(strings.TrimPrefix(valueKey, "s:")))
		return strings.HasPrefix(valueID, shardID)
	default:
		return shardID == secondaryIndexShardID(valueKey)
	}
}

func secondaryIndexShardObjects(objects []IndexObject) ([]IndexObject, bool) {
	shards, ok := secondaryIndexShardObjectMap(objects)
	if !ok {
		return nil, false
	}
	return sortedIndexObjects(shards), true
}

func indexObjectByRole(objects []IndexObject, role string) (IndexObject, bool) {
	for _, object := range objects {
		if object.Role == role && object.Key != "" {
			return object, true
		}
	}
	return IndexObject{}, false
}

func uniqueIndexObjects(objects []IndexObject) []IndexObject {
	seen := map[string]struct{}{}
	out := make([]IndexObject, 0, len(objects))
	for _, object := range objects {
		key := object.Key + "\x00" + object.ContentHash
		if object.Key == "" {
			key = object.Role + "\x00" + object.ContentHash
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, object)
	}
	return out
}

func secondaryIndexStringShardPrefix(shardID string) (string, bool) {
	prefix, ok := strings.CutPrefix(shardID, "s_")
	return prefix, ok
}

func secondaryIndexStringShardMatchesFilters(shardPrefix string, filters []query.Filter) (bool, bool, bool) {
	used := false
	for _, filter := range filters {
		op := filter.Op
		if op == "" {
			op = "eq"
		}
		switch op {
		case "exists":
			continue
		case "prefix":
			value, ok := filter.Value.(string)
			if !ok {
				return false, false, false
			}
			prefix, ok := secondaryIndexFilterString(value)
			if !ok {
				return false, false, false
			}
			if prefix == "" {
				continue
			}
			used = true
			if !strings.HasPrefix(shardPrefix, prefix) && !strings.HasPrefix(prefix, shardPrefix) {
				return false, true, true
			}
		case "gt", "gte":
			value, ok := filter.Value.(string)
			if !ok {
				return false, false, false
			}
			lower, ok := secondaryIndexFilterString(value)
			if !ok {
				return false, false, false
			}
			used = true
			if shardPrefix+"\xff" <= lower {
				return false, true, true
			}
		case "lt", "lte":
			value, ok := filter.Value.(string)
			if !ok {
				return false, false, false
			}
			upper, ok := secondaryIndexFilterString(value)
			if !ok {
				return false, false, false
			}
			used = true
			if shardPrefix >= upper {
				return false, true, true
			}
		default:
			return false, false, false
		}
	}
	return true, used, true
}

func secondaryIndexFilterString(value string) (string, bool) {
	value = strings.ToLower(value)
	for _, r := range value {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		switch r {
		case '-', '_', '.':
			continue
		default:
			return "", false
		}
	}
	return value, true
}

func sortedIndexObjects(objects map[string]IndexObject) []IndexObject {
	ids := make([]string, 0, len(objects))
	for id := range objects {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]IndexObject, 0, len(ids))
	for _, id := range ids {
		out = append(out, objects[id])
	}
	return out
}

func parquetSecondaryIndexArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "unique", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_id", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetSecondaryIndexSchemaHash() string {
	return objectSchemaHash([]string{
		"tenant_id",
		"kind",
		"field",
		"unique",
		"version",
		"updated_at",
		"value",
		"entity_id",
		parquetSecondaryIndexCodec,
	})
}

func parquetBoolColumn(record arrow.RecordBatch, index int, name string) (*array.Boolean, error) {
	column, ok := record.Column(index).(*array.Boolean)
	if !ok {
		return nil, fmt.Errorf("parquet column %q has unexpected type", name)
	}
	return column, nil
}

func parquetFloat64Column(record arrow.RecordBatch, index int, name string) (*array.Float64, error) {
	column, ok := record.Column(index).(*array.Float64)
	if !ok {
		return nil, fmt.Errorf("parquet column %q has unexpected type", name)
	}
	return column, nil
}
