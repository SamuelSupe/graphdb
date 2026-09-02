package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	parquetEntityColumnTenantID = iota
	parquetEntityColumnShard
	parquetEntityColumnPageVersion
	parquetEntityColumnPageUpdatedAt
	parquetEntityColumnID
	parquetEntityColumnKind
	parquetEntityColumnSource
	parquetEntityColumnExternalID
	parquetEntityColumnEntityVersion
	parquetEntityColumnEntityCreatedAt
	parquetEntityColumnEntityUpdatedAt
	parquetEntityColumnConfidence
	parquetEntityColumnSourceRank
	parquetEntityColumnSplitFrom
	parquetEntityColumnRowKind
	parquetEntityColumnOrdinal
	parquetEntityColumnEntryKey
	parquetEntityColumnValueKind
	parquetEntityColumnStringValue
	parquetEntityColumnBoolValue
	parquetEntityColumnFloatValue
	parquetEntityColumnFieldSourceSource
	parquetEntityColumnFieldSourcePriority
	parquetEntityColumnFieldSourceConfidence
	parquetEntityColumnFieldSourceVersion
	parquetEntityColumnFieldSourceUpdatedAt
	parquetEntityColumnEntitySourceSource
	parquetEntityColumnEntitySourceExternalID
	parquetEntityColumnEntitySourceConfidence
	parquetEntityColumnEntitySourcePriority
	parquetEntityColumnEntitySourceObservedAt
	parquetEntityColumnEntitySourceStale
	parquetEntityColumnEntitySourceStaleAt
)

const (
	entityPageRowMetadata        = "metadata"
	entityPageRowField           = "field"
	entityPageRowFieldSource     = "field_source"
	entityPageRowIdentity        = "identity"
	entityPageRowSource          = "source"
	entityPageRowMergedFrom      = "merged_from"
	entityPageRowExistenceSource = "existence_source"
)

const (
	parquetEntityPageRowGroupSize  int64 = 512
	parquetEntityPageReadBatchSize int64 = 128
)

func (s *TenantStore) writeParquetEntityPages(ctx context.Context, tenantID string, pages []EntityPageData, version int64) error {
	return s.writeParquetEntityPagesWithOptions(ctx, tenantID, pages, version, true)
}

func (s *TenantStore) writeParquetEntityPagesFast(ctx context.Context, tenantID string, pages []EntityPageData, version int64) error {
	return s.writeParquetEntityPagesWithOptions(ctx, tenantID, pages, version, false)
}

func (s *TenantStore) writeParquetEntityPagesWithOptions(ctx context.Context, tenantID string, pages []EntityPageData, version int64, checkExisting bool) error {
	recordJobs := []entityRecordWriteJob{}
	currentIDs := map[string]struct{}{}
	writeRecords := s.WriteEntityRecords
	for _, group := range entityPageDataPackGroups(pages, !s.WriteEntityRecords, s.EntityPagePackMaxBytes) {
		for i := range group.Pages {
			group.Pages[i].TenantID = tenantID
		}
		pack := mergeEntityPagePack(group)
		pack.TenantID = tenantID
		key := s.parquetEntityPageVersionKey(tenantID, pack.Version, pack.Shard)
		if len(group.Pages) > 1 {
			key = s.parquetEntityPagePackVersionKey(tenantID, pack.Version, group.ID)
			if checkExisting {
				if pageMeta, ok, err := s.existingParquetEntityPagePackMeta(ctx, key, tenantID, group.Pages); err != nil || ok {
					if err != nil {
						return err
					}
					if writeRecords {
						for _, page := range group.Pages {
							pageHash := entityPageContentHash(page)
							for _, entity := range page.Entities {
								record := newEntityRecord(tenantID, entity, page.Shard, pageHash, pageMeta.ETag, page.Version, page.UpdatedAt)
								recordKey := s.entityRecordKey(tenantID, entity.ID)
								currentIDs[entity.ID] = struct{}{}
								recordJobs = append(recordJobs, entityRecordWriteJob{Key: recordKey, Record: record})
							}
						}
					}
					continue
				}
			}
		}
		pageMeta, err := s.putParquetEntityPageObject(ctx, key, tenantID, pack, checkExisting)
		if err != nil {
			return err
		}
		if writeRecords {
			for _, page := range group.Pages {
				pageHash := entityPageContentHash(page)
				for _, entity := range page.Entities {
					record := newEntityRecord(tenantID, entity, page.Shard, pageHash, pageMeta.ETag, page.Version, page.UpdatedAt)
					key := s.entityRecordKey(tenantID, entity.ID)
					currentIDs[entity.ID] = struct{}{}
					recordJobs = append(recordJobs, entityRecordWriteJob{Key: key, Record: record})
				}
			}
		}
	}
	if !writeRecords {
		return nil
	}
	if err := s.putEntityRecordBatch(ctx, recordJobs); err != nil {
		return err
	}
	return s.tombstoneStaleEntityRecords(ctx, tenantID, currentIDs, version)
}

func (s *TenantStore) putParquetEntityPage(ctx context.Context, tenantID string, page EntityPageData) (ObjectMeta, error) {
	key := s.parquetEntityPageVersionKey(tenantID, page.Version, page.Shard)
	return s.putParquetEntityPageObject(ctx, key, tenantID, page, true)
}

func (s *TenantStore) putParquetEntityPageObject(ctx context.Context, key string, tenantID string, page EntityPageData, checkExisting bool) (ObjectMeta, error) {
	if checkExisting {
		if meta, ok, err := s.existingParquetEntityPageMeta(ctx, key, tenantID, page); err != nil || ok {
			return meta, err
		}
	}
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		return ObjectMeta{}, err
	}
	meta, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true})
	if err == nil {
		s.markObjectKeyCached(key)
	}
	if errors.Is(err, ErrConflict) {
		meta, err = s.putConflictingParquetEntityPageObject(ctx, key, tenantID, page, data)
	}
	// Do not duplicate the writer's full graph in the decoded read cache. The
	// first reader fills the bounded cache on demand from this immutable object.
	return meta, err
}

func (s *TenantStore) putConflictingParquetEntityPageObject(ctx context.Context, key string, tenantID string, page EntityPageData, data []byte) (ObjectMeta, error) {
	existing, meta, err := s.Objects.GetWithMeta(ctx, key)
	if err != nil {
		return ObjectMeta{}, err
	}
	decoded, decodeErr := decodeParquetEntityPage(ctx, existing, tenantID, page.Shard, page.Version)
	if decodeErr == nil && entityPageContentHash(decoded) == entityPageContentHash(page) {
		s.markObjectKeyCached(key)
		return meta, nil
	}
	nextMeta, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfMatch: meta.ETag})
	if err == nil {
		s.markObjectKeyCached(key)
	}
	return nextMeta, err
}

func (s *TenantStore) existingParquetEntityPageMeta(ctx context.Context, key string, tenantID string, page EntityPageData) (ObjectMeta, bool, error) {
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil || !mayExist {
		return ObjectMeta{}, false, err
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return ObjectMeta{}, false, nil
	}
	if err != nil {
		return ObjectMeta{}, false, err
	}
	existing, err := decodeParquetEntityPage(ctx, data, tenantID, page.Shard, page.Version)
	if err != nil {
		return meta, false, nil
	}
	if entityPageContentHash(existing) == entityPageContentHash(page) {
		s.putCachedEntityPage(tenantID, page.Version, key, entityPageContentHash(page), parquetEntityPageSchemaHash(), existing, meta.ETag)
		return meta, true, nil
	}
	return meta, false, nil
}

func (s *TenantStore) existingParquetEntityPagePackMeta(ctx context.Context, key string, tenantID string, pages []EntityPageData) (ObjectMeta, bool, error) {
	mayExist, err := s.objectKeyMayExist(ctx, key)
	if err != nil || !mayExist {
		return ObjectMeta{}, false, err
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return ObjectMeta{}, false, nil
	}
	if err != nil {
		return ObjectMeta{}, false, err
	}
	for _, page := range pages {
		existing, err := decodeParquetEntityPage(ctx, data, tenantID, page.Shard, page.Version)
		if err != nil {
			return meta, false, nil
		}
		if entityPageContentHash(existing) != entityPageContentHash(page) {
			return meta, false, nil
		}
	}
	return meta, true, nil
}

func marshalParquetEntityPage(ctx context.Context, page EntityPageData) ([]byte, error) {
	schema := parquetEntityPageArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, entity := range page.Entities {
		rowShard := page.Shard
		if isIndexPackID(page.Shard) {
			rowShard = entityShardID(entity.ID)
		}
		rows, err := entityPageRows(entity)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			builder.Field(parquetEntityColumnTenantID).(*array.StringBuilder).Append(page.TenantID)
			builder.Field(parquetEntityColumnShard).(*array.StringBuilder).Append(rowShard)
			builder.Field(parquetEntityColumnPageVersion).(*array.Int64Builder).Append(page.Version)
			builder.Field(parquetEntityColumnPageUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(page.UpdatedAt))
			builder.Field(parquetEntityColumnID).(*array.StringBuilder).Append(entity.ID)
			builder.Field(parquetEntityColumnKind).(*array.StringBuilder).Append(entity.Kind)
			builder.Field(parquetEntityColumnSource).(*array.StringBuilder).Append(entity.Source)
			builder.Field(parquetEntityColumnExternalID).(*array.StringBuilder).Append(entity.ExternalID)
			builder.Field(parquetEntityColumnEntityVersion).(*array.Int64Builder).Append(entity.Version)
			builder.Field(parquetEntityColumnEntityCreatedAt).(*array.StringBuilder).Append(formatParquetTime(entity.CreatedAt))
			builder.Field(parquetEntityColumnEntityUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(entity.UpdatedAt))
			builder.Field(parquetEntityColumnConfidence).(*array.Float64Builder).Append(entity.Confidence)
			builder.Field(parquetEntityColumnSourceRank).(*array.Int64Builder).Append(int64(entity.SourceRank))
			builder.Field(parquetEntityColumnSplitFrom).(*array.StringBuilder).Append(entity.SplitFrom)
			builder.Field(parquetEntityColumnRowKind).(*array.StringBuilder).Append(row.Kind)
			builder.Field(parquetEntityColumnOrdinal).(*array.Int64Builder).Append(int64(row.Ordinal))
			builder.Field(parquetEntityColumnEntryKey).(*array.StringBuilder).Append(row.Key)
			builder.Field(parquetEntityColumnValueKind).(*array.StringBuilder).Append(row.Value.Kind)
			builder.Field(parquetEntityColumnStringValue).(*array.StringBuilder).Append(row.Value.StringValue)
			builder.Field(parquetEntityColumnBoolValue).(*array.BooleanBuilder).Append(row.Value.BoolValue)
			builder.Field(parquetEntityColumnFloatValue).(*array.Float64Builder).Append(row.Value.FloatValue)
			builder.Field(parquetEntityColumnFieldSourceSource).(*array.StringBuilder).Append(row.FieldSource.Source)
			builder.Field(parquetEntityColumnFieldSourcePriority).(*array.Int64Builder).Append(int64(row.FieldSource.Priority))
			builder.Field(parquetEntityColumnFieldSourceConfidence).(*array.Float64Builder).Append(row.FieldSource.Confidence)
			builder.Field(parquetEntityColumnFieldSourceVersion).(*array.Int64Builder).Append(row.FieldSource.Version)
			builder.Field(parquetEntityColumnFieldSourceUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(row.FieldSource.UpdatedAt))
			builder.Field(parquetEntityColumnEntitySourceSource).(*array.StringBuilder).Append(row.EntitySource.Source)
			builder.Field(parquetEntityColumnEntitySourceExternalID).(*array.StringBuilder).Append(row.EntitySource.ExternalID)
			builder.Field(parquetEntityColumnEntitySourceConfidence).(*array.Float64Builder).Append(row.EntitySource.Confidence)
			builder.Field(parquetEntityColumnEntitySourcePriority).(*array.Int64Builder).Append(int64(row.EntitySource.Priority))
			builder.Field(parquetEntityColumnEntitySourceObservedAt).(*array.StringBuilder).Append(formatParquetTime(row.EntitySource.ObservedAt))
			builder.Field(parquetEntityColumnEntitySourceStale).(*array.BooleanBuilder).Append(row.EntitySource.Stale)
			builder.Field(parquetEntityColumnEntitySourceStaleAt).(*array.StringBuilder).Append(formatParquetTime(row.EntitySource.StaleAt))
		}
	}

	record := builder.NewRecordBatch()
	defer record.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{record})
	defer table.Release()

	var buf bytes.Buffer
	rowGroupSize := parquetEntityPageRowGroupSize
	if table.NumRows() > 0 && table.NumRows() < rowGroupSize {
		rowGroupSize = table.NumRows()
	}
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

func decodeParquetEntityPage(ctx context.Context, data []byte, tenantID string, shard string, version int64) (EntityPageData, error) {
	reader, err := pqfile.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return EntityPageData{}, err
	}
	defer reader.Close()
	if reader.MetaData().Schema.NumColumns() < parquetEntityColumnEntitySourceStaleAt+1 {
		return EntityPageData{}, fmt.Errorf("parquet entity page has %d columns, want at least %d", reader.MetaData().Schema.NumColumns(), parquetEntityColumnEntitySourceStaleAt+1)
	}
	columns := make([]int, reader.MetaData().Schema.NumColumns())
	for i := range columns {
		columns[i] = i
	}
	fileReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: parquetEntityPageReadBatchSize}, memory.DefaultAllocator)
	if err != nil {
		return EntityPageData{}, err
	}

	page := EntityPageData{LayoutVersion: CurrentObjectLayoutVersion, TenantID: tenantID, Shard: shard, Version: version}
	byID := map[string]*graph.Entity{}
	recordReader, release, err := readParquetRecordReader(ctx, fileReader, columns, nil)
	if err != nil {
		return EntityPageData{}, err
	}
	defer release()
	defer recordReader.Release()
	for recordReader.Next() {
		err = appendParquetEntityPageRecord(recordReader.RecordBatch(), tenantID, shard, version, &page, byID)
		if err != nil {
			return EntityPageData{}, err
		}
	}
	if err := recordReader.Err(); err != nil {
		return EntityPageData{}, err
	}
	for _, entity := range byID {
		page.Entities = append(page.Entities, decodedEntityPageCopy(*entity))
	}
	sort.Slice(page.Entities, func(i, j int) bool { return page.Entities[i].ID < page.Entities[j].ID })
	return page, nil
}

func appendParquetEntityPageRows(table arrow.Table, tenantID string, shard string, version int64, page *EntityPageData, byID map[string]*graph.Entity) error {
	if table.NumCols() < int64(parquetEntityColumnEntitySourceStaleAt+1) {
		return fmt.Errorf("parquet entity page has %d columns, want at least %d", table.NumCols(), parquetEntityColumnEntitySourceStaleAt+1)
	}
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		if err := appendParquetEntityPageRecord(reader.RecordBatch(), tenantID, shard, version, page, byID); err != nil {
			return err
		}
	}
	return reader.Err()
}

func appendParquetEntityPageRecord(record arrow.RecordBatch, tenantID string, shard string, version int64, page *EntityPageData, byID map[string]*graph.Entity) error {
	if record.NumCols() < int64(parquetEntityColumnEntitySourceStaleAt+1) {
		return fmt.Errorf("parquet entity record has %d columns, want at least %d", record.NumCols(), parquetEntityColumnEntitySourceStaleAt+1)
	}
	columns, err := parquetEntityPageColumns(record)
	if err != nil {
		return err
	}
	for i := 0; i < int(record.NumRows()); i++ {
		rowTenant := columns.tenantID.Value(i)
		if rowTenant != "" {
			if page.TenantID == "" || page.TenantID == tenantID {
				if page.TenantID != rowTenant {
					page.TenantID = strings.Clone(rowTenant)
				}
			} else if page.TenantID != rowTenant {
				return fmt.Errorf("parquet entity page tenant mismatch")
			}
		}
		rowShard := columns.shard.Value(i)
		if shard != "" && rowShard != "" && rowShard != shard {
			continue
		}
		if rowShard != "" {
			if shard == "" {
				if page.Shard == "" {
					page.Shard = strings.Clone(rowShard)
				}
			} else if page.Shard == "" || page.Shard == shard {
				if page.Shard != rowShard {
					page.Shard = strings.Clone(rowShard)
				}
			} else if page.Shard != rowShard {
				return fmt.Errorf("parquet entity page shard mismatch")
			}
		}
		rowVersion := columns.pageVersion.Value(i)
		if rowVersion != 0 {
			if page.Version == 0 || version == 0 || page.Version == version {
				page.Version = rowVersion
			} else if page.Version != rowVersion {
				return fmt.Errorf("parquet entity page version mismatch")
			}
		}
		if page.UpdatedAt.IsZero() {
			page.UpdatedAt = parseParquetTime(columns.pageUpdatedAt.Value(i))
		}
		entityID := columns.id.Value(i)
		entity := byID[entityID]
		if entity == nil {
			created := graph.Entity{
				ID:         strings.Clone(entityID),
				Kind:       strings.Clone(columns.kind.Value(i)),
				Source:     strings.Clone(columns.source.Value(i)),
				ExternalID: strings.Clone(columns.externalID.Value(i)),
				Version:    columns.entityVersion.Value(i),
				CreatedAt:  parseParquetTime(columns.entityCreatedAt.Value(i)),
				UpdatedAt:  parseParquetTime(columns.entityUpdatedAt.Value(i)),
				Confidence: columns.confidence.Value(i),
				SourceRank: int(columns.sourceRank.Value(i)),
				SplitFrom:  strings.Clone(columns.splitFrom.Value(i)),
			}
			byID[created.ID] = &created
			entity = &created
		}
		if err := applyEntityPageRow(entity, entityPageRowFromColumns(columns, i)); err != nil {
			return err
		}
	}
	return nil
}

func decodedEntityPageCopy(entity graph.Entity) graph.Entity {
	out := graph.CopyEntity(entity)
	if len(out.Fields) == 0 {
		out.Fields = nil
	}
	if len(out.FieldSources) == 0 {
		out.FieldSources = nil
	}
	if len(out.Identity) == 0 {
		out.Identity = nil
	}
	if len(out.Sources) == 0 {
		out.Sources = nil
	}
	if len(out.MergedFrom) == 0 {
		out.MergedFrom = nil
	}
	return out
}

type entityPageRow struct {
	Kind         string
	Ordinal      int
	Key          string
	Value        parquetValue
	Strategy     string
	FieldSource  graph.FieldSource
	EntitySource graph.EntitySource
}

type parquetEntityPageColumnSet struct {
	tenantID               *array.String
	shard                  *array.String
	pageVersion            *array.Int64
	pageUpdatedAt          *array.String
	id                     *array.String
	kind                   *array.String
	source                 *array.String
	externalID             *array.String
	entityVersion          *array.Int64
	entityCreatedAt        *array.String
	entityUpdatedAt        *array.String
	confidence             *array.Float64
	sourceRank             *array.Int64
	splitFrom              *array.String
	rowKind                *array.String
	ordinal                *array.Int64
	entryKey               *array.String
	valueKind              *array.String
	stringValue            *array.String
	boolValue              *array.Boolean
	floatValue             *array.Float64
	fieldSourceSource      *array.String
	fieldSourcePriority    *array.Int64
	fieldSourceConfidence  *array.Float64
	fieldSourceVersion     *array.Int64
	fieldSourceUpdatedAt   *array.String
	entitySourceSource     *array.String
	entitySourceExternalID *array.String
	entitySourceConfidence *array.Float64
	entitySourcePriority   *array.Int64
	entitySourceObservedAt *array.String
	entitySourceStale      *array.Boolean
	entitySourceStaleAt    *array.String
}

func entityPageRows(entity graph.Entity) ([]entityPageRow, error) {
	entity = graph.CopyEntity(entity)
	rows := []entityPageRow{}
	fieldNames := sortedAnyMapKeys(entity.Fields)
	for i, field := range fieldNames {
		value, err := parquetValueFromAny(entity.Fields[field])
		if err != nil {
			return nil, err
		}
		rows = append(rows, entityPageRow{Kind: entityPageRowField, Ordinal: i, Key: field, Value: value})
	}
	sourceFields := sortedFieldSourceKeys(entity.FieldSources)
	for i, field := range sourceFields {
		rows = append(rows, entityPageRow{Kind: entityPageRowFieldSource, Ordinal: i, Key: field, FieldSource: entity.FieldSources[field]})
	}
	identityKeys := sortedAnyMapKeys(entity.Identity)
	for i, key := range identityKeys {
		value, err := parquetValueFromAny(entity.Identity[key])
		if err != nil {
			return nil, err
		}
		rows = append(rows, entityPageRow{Kind: entityPageRowIdentity, Ordinal: i, Key: key, Value: value})
	}
	for i, source := range entity.Sources {
		rows = append(rows, entityPageRow{Kind: entityPageRowSource, Ordinal: i, EntitySource: source})
	}
	for i, mergedFrom := range entity.MergedFrom {
		rows = append(rows, entityPageRow{
			Kind:    entityPageRowMergedFrom,
			Ordinal: i,
			Value:   parquetValue{Kind: parquetValueKindString, StringValue: mergedFrom},
		})
	}
	if entity.ExistenceSource != nil {
		rows = append(rows, entityPageRow{Kind: entityPageRowExistenceSource, FieldSource: *entity.ExistenceSource})
	}
	if len(rows) == 0 {
		rows = append(rows, entityPageRow{Kind: entityPageRowMetadata})
	}
	return rows, nil
}

func applyEntityPageRow(entity *graph.Entity, row entityPageRow) error {
	switch row.Kind {
	case entityPageRowMetadata:
		return nil
	case entityPageRowField:
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		if entity.Fields == nil {
			entity.Fields = graph.Fields{}
		}
		entity.Fields[row.Key] = value
		if row.Strategy != "" {
			if entity.FieldWriteModes == nil {
				entity.FieldWriteModes = map[string]string{}
			}
			entity.FieldWriteModes[row.Key] = row.Strategy
		}
	case entityPageRowFieldSource:
		if entity.FieldSources == nil {
			entity.FieldSources = map[string]graph.FieldSource{}
		}
		entity.FieldSources[row.Key] = row.FieldSource
	case entityPageRowIdentity:
		value, err := anyFromParquetValue(row.Value)
		if err != nil {
			return err
		}
		if entity.Identity == nil {
			entity.Identity = map[string]any{}
		}
		entity.Identity[row.Key] = value
	case entityPageRowSource:
		if row.Ordinal < 0 {
			return fmt.Errorf("entity page source ordinal mismatch")
		}
		for len(entity.Sources) <= row.Ordinal {
			entity.Sources = append(entity.Sources, graph.EntitySource{})
		}
		entity.Sources[row.Ordinal] = row.EntitySource
	case entityPageRowMergedFrom:
		if row.Ordinal < 0 {
			return fmt.Errorf("entity page merged_from ordinal mismatch")
		}
		for len(entity.MergedFrom) <= row.Ordinal {
			entity.MergedFrom = append(entity.MergedFrom, "")
		}
		entity.MergedFrom[row.Ordinal] = row.Value.StringValue
	case entityPageRowExistenceSource:
		source := row.FieldSource
		entity.ExistenceSource = &source
	default:
		return fmt.Errorf("unknown entity page row kind %q", row.Kind)
	}
	return nil
}

func parquetEntityPageColumns(record arrow.RecordBatch) (parquetEntityPageColumnSet, error) {
	var columns parquetEntityPageColumnSet
	var err error
	if columns.tenantID, err = parquetStringColumn(record, parquetEntityColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.shard, err = parquetStringColumn(record, parquetEntityColumnShard, "shard"); err != nil {
		return columns, err
	}
	if columns.pageVersion, err = parquetInt64Column(record, parquetEntityColumnPageVersion, "page_version"); err != nil {
		return columns, err
	}
	if columns.pageUpdatedAt, err = parquetStringColumn(record, parquetEntityColumnPageUpdatedAt, "page_updated_at"); err != nil {
		return columns, err
	}
	if columns.id, err = parquetStringColumn(record, parquetEntityColumnID, "entity_id"); err != nil {
		return columns, err
	}
	if columns.kind, err = parquetStringColumn(record, parquetEntityColumnKind, "kind"); err != nil {
		return columns, err
	}
	if columns.source, err = parquetStringColumn(record, parquetEntityColumnSource, "source"); err != nil {
		return columns, err
	}
	if columns.externalID, err = parquetStringColumn(record, parquetEntityColumnExternalID, "external_id"); err != nil {
		return columns, err
	}
	if columns.entityVersion, err = parquetInt64Column(record, parquetEntityColumnEntityVersion, "entity_version"); err != nil {
		return columns, err
	}
	if columns.entityCreatedAt, err = parquetStringColumn(record, parquetEntityColumnEntityCreatedAt, "entity_created_at"); err != nil {
		return columns, err
	}
	if columns.entityUpdatedAt, err = parquetStringColumn(record, parquetEntityColumnEntityUpdatedAt, "entity_updated_at"); err != nil {
		return columns, err
	}
	if columns.confidence, err = parquetFloat64Column(record, parquetEntityColumnConfidence, "confidence"); err != nil {
		return columns, err
	}
	if columns.sourceRank, err = parquetInt64Column(record, parquetEntityColumnSourceRank, "source_priority"); err != nil {
		return columns, err
	}
	if columns.splitFrom, err = parquetStringColumn(record, parquetEntityColumnSplitFrom, "split_from"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetStringColumn(record, parquetEntityColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetInt64Column(record, parquetEntityColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.entryKey, err = parquetStringColumn(record, parquetEntityColumnEntryKey, "entry_key"); err != nil {
		return columns, err
	}
	if columns.valueKind, err = parquetStringColumn(record, parquetEntityColumnValueKind, "value_kind"); err != nil {
		return columns, err
	}
	if columns.stringValue, err = parquetStringColumn(record, parquetEntityColumnStringValue, "string_value"); err != nil {
		return columns, err
	}
	if columns.boolValue, err = parquetBoolColumn(record, parquetEntityColumnBoolValue, "bool_value"); err != nil {
		return columns, err
	}
	if columns.floatValue, err = parquetFloat64Column(record, parquetEntityColumnFloatValue, "float_value"); err != nil {
		return columns, err
	}
	if columns.fieldSourceSource, err = parquetStringColumn(record, parquetEntityColumnFieldSourceSource, "field_source_source"); err != nil {
		return columns, err
	}
	if columns.fieldSourcePriority, err = parquetInt64Column(record, parquetEntityColumnFieldSourcePriority, "field_source_priority"); err != nil {
		return columns, err
	}
	if columns.fieldSourceConfidence, err = parquetFloat64Column(record, parquetEntityColumnFieldSourceConfidence, "field_source_confidence"); err != nil {
		return columns, err
	}
	if columns.fieldSourceVersion, err = parquetInt64Column(record, parquetEntityColumnFieldSourceVersion, "field_source_version"); err != nil {
		return columns, err
	}
	if columns.fieldSourceUpdatedAt, err = parquetStringColumn(record, parquetEntityColumnFieldSourceUpdatedAt, "field_source_updated_at"); err != nil {
		return columns, err
	}
	if columns.entitySourceSource, err = parquetStringColumn(record, parquetEntityColumnEntitySourceSource, "entity_source_source"); err != nil {
		return columns, err
	}
	if columns.entitySourceExternalID, err = parquetStringColumn(record, parquetEntityColumnEntitySourceExternalID, "entity_source_external_id"); err != nil {
		return columns, err
	}
	if columns.entitySourceConfidence, err = parquetFloat64Column(record, parquetEntityColumnEntitySourceConfidence, "entity_source_confidence"); err != nil {
		return columns, err
	}
	if columns.entitySourcePriority, err = parquetInt64Column(record, parquetEntityColumnEntitySourcePriority, "entity_source_priority"); err != nil {
		return columns, err
	}
	if columns.entitySourceObservedAt, err = parquetStringColumn(record, parquetEntityColumnEntitySourceObservedAt, "entity_source_observed_at"); err != nil {
		return columns, err
	}
	if columns.entitySourceStale, err = parquetBoolColumn(record, parquetEntityColumnEntitySourceStale, "entity_source_stale"); err != nil {
		return columns, err
	}
	if columns.entitySourceStaleAt, err = parquetStringColumn(record, parquetEntityColumnEntitySourceStaleAt, "entity_source_stale_at"); err != nil {
		return columns, err
	}
	return columns, nil
}

func entityPageRowFromColumns(columns parquetEntityPageColumnSet, row int) entityPageRow {
	return entityPageRow{
		Kind:    columns.rowKind.Value(row),
		Ordinal: int(columns.ordinal.Value(row)),
		Key:     strings.Clone(columns.entryKey.Value(row)),
		Value: parquetValue{
			Kind:        columns.valueKind.Value(row),
			StringValue: strings.Clone(columns.stringValue.Value(row)),
			BoolValue:   columns.boolValue.Value(row),
			FloatValue:  columns.floatValue.Value(row),
		},
		FieldSource: graph.FieldSource{
			Source:     strings.Clone(columns.fieldSourceSource.Value(row)),
			Priority:   int(columns.fieldSourcePriority.Value(row)),
			Confidence: columns.fieldSourceConfidence.Value(row),
			Version:    columns.fieldSourceVersion.Value(row),
			UpdatedAt:  parseParquetTime(columns.fieldSourceUpdatedAt.Value(row)),
		},
		EntitySource: graph.EntitySource{
			Source:     strings.Clone(columns.entitySourceSource.Value(row)),
			ExternalID: strings.Clone(columns.entitySourceExternalID.Value(row)),
			Confidence: columns.entitySourceConfidence.Value(row),
			Priority:   int(columns.entitySourcePriority.Value(row)),
			ObservedAt: parseParquetTime(columns.entitySourceObservedAt.Value(row)),
			Stale:      columns.entitySourceStale.Value(row),
			StaleAt:    parseParquetTime(columns.entitySourceStaleAt.Value(row)),
		},
	}
}

func sortedAnyMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedFieldSourceKeys(values map[string]graph.FieldSource) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func parquetStringColumn(record arrow.RecordBatch, index int, name string) (*array.String, error) {
	column, ok := record.Column(index).(*array.String)
	if !ok {
		return nil, fmt.Errorf("parquet entity page column %q has unexpected type", name)
	}
	return column, nil
}

func parquetInt64Column(record arrow.RecordBatch, index int, name string) (*array.Int64, error) {
	column, ok := record.Column(index).(*array.Int64)
	if !ok {
		return nil, fmt.Errorf("parquet entity page column %q has unexpected type", name)
	}
	return column, nil
}

func parquetEntityPageArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "shard", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "page_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "page_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_created_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "split_from", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entry_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "value_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "float_value", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "field_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "field_source_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "field_source_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_source", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_external_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_confidence", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		{Name: "entity_source_priority", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_source_observed_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_source_stale", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "entity_source_stale_at", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetEntityPageSchemaHash() string {
	return objectSchemaHash([]string{
		"tenant_id",
		"shard",
		"page_version",
		"page_updated_at",
		"entity_id",
		"kind",
		"source",
		"external_id",
		"entity_version",
		"entity_created_at",
		"entity_updated_at",
		"confidence",
		"source_priority",
		"split_from",
		"row_kind",
		"ordinal",
		"entry_key",
		"value_kind",
		"string_value",
		"bool_value",
		"float_value",
		"field_source_source",
		"field_source_priority",
		"field_source_confidence",
		"field_source_version",
		"field_source_updated_at",
		"entity_source_source",
		"entity_source_external_id",
		"entity_source_confidence",
		"entity_source_priority",
		"entity_source_observed_at",
		"entity_source_stale",
		"entity_source_stale_at",
		parquetEntityPageCodec,
	})
}

func formatParquetTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func parseParquetTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func (l *PersistedIndexLookup) getEntityByIDFromParquetPage(ctx context.Context, id string, fields []string, spec EntityPageSpec) (graph.Entity, bool, error) {
	page, ok, err := l.loadParquetEntityPage(ctx, spec)
	if err != nil {
		if ctx.Err() != nil {
			return graph.Entity{}, false, err
		}
		return graph.Entity{}, false, nil
	}
	if !ok {
		return graph.Entity{}, false, nil
	}
	entity, ok := l.entityFromCachedPage(spec.Shard, id, page)
	if !ok {
		return graph.Entity{}, false, nil
	}
	return trimEntityFields(entity, fields), true, nil
}

func (l *PersistedIndexLookup) getEntityFromParquetPage(ctx context.Context, record EntityRecord, fields []string, spec EntityPageSpec) (graph.Entity, bool, error) {
	if record.Page != spec.Shard {
		return graph.Entity{}, false, nil
	}
	matched, err := l.entityRecordMatchesPage(ctx, record)
	if err != nil {
		if ctx.Err() != nil {
			return graph.Entity{}, false, err
		}
		return graph.Entity{}, false, nil
	}
	if !matched || record.Entity.ID != record.ID {
		return graph.Entity{}, false, nil
	}
	return trimEntityFields(record.Entity, fields), true, nil
}

func (l *PersistedIndexLookup) listParquetEntitiesFromPage(ctx context.Context, spec EntityPageSpec, kind string, fields []string) ([]graph.Entity, bool, error) {
	page, ok, err := l.loadParquetEntityPage(ctx, spec)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if !ok {
		return nil, false, nil
	}
	out := make([]graph.Entity, 0, len(page.Entities))
	for _, entity := range page.Entities {
		if err := objectContextErr(ctx); err != nil {
			return nil, false, err
		}
		if kind != "" && entity.Kind != kind {
			continue
		}
		out = append(out, trimEntityFields(entity, fields))
	}
	return out, true, nil
}

func (l *PersistedIndexLookup) loadParquetEntityPage(ctx context.Context, spec EntityPageSpec) (EntityPageData, bool, error) {
	page, err := l.loadEntityPage(ctx, spec.Shard)
	if errors.Is(err, ErrNotFound) {
		return EntityPageData{}, false, nil
	}
	if err != nil {
		return EntityPageData{}, false, err
	}
	return page, true, nil
}

func (s *TenantStore) loadParquetEntityPageObject(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) (EntityPageData, string, bool, error) {
	var result EntityPageData
	var resultETag string
	ok, err := s.withParquetEntityPageObject(ctx, tenantID, version, spec, func(page EntityPageData, etag string, _ bool) error {
		result = copyEntityPage(page)
		resultETag = etag
		return nil
	})
	return result, resultETag, ok, err
}

func (s *TenantStore) loadValidatedParquetEntityPageObject(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) (EntityPageData, string, bool, error) {
	var result EntityPageData
	var resultETag string
	valid := false
	ok, err := s.withParquetEntityPageObject(ctx, tenantID, version, spec, func(page EntityPageData, etag string, validated bool) error {
		if validated {
			result = copyEntityPage(page)
			resultETag = etag
			valid = true
		}
		return nil
	})
	return result, resultETag, ok && valid, err
}

func (s *TenantStore) withParquetEntityPageObject(ctx context.Context, tenantID string, version int64, spec EntityPageSpec, visit func(EntityPageData, string, bool) error) (bool, error) {
	traceStats := entityPageVisitTraceStatsFromContext(ctx)
	key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
	if entry, revalidate, ok := s.borrowCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash); ok {
		if traceStats != nil {
			traceStats.decodedCacheHits++
		}
		page, etag := entry.page, entry.etag
		cacheValid := true
		if revalidate {
			if traceStats != nil {
				traceStats.cacheRevalidations++
			}
			meta, err := objectMeta(ctx, s.Objects, key)
			if errors.Is(err, ErrNotFound) {
				if traceStats != nil {
					traceStats.cacheInvalidations++
				}
				s.dropCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash)
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if etag != "" && meta.ETag != etag {
				if traceStats != nil {
					traceStats.cacheInvalidations++
				}
				s.dropCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash)
				cacheValid = false
			} else {
				s.entityPageCache.markValidated(entityPageCacheKey(tenantID, version, key, spec.ContentHash, spec.SchemaHash), meta.ETag)
				etag = meta.ETag
			}
		}
		if cacheValid {
			// Pages enter this cache only after tenant/version/content/schema checks.
			// The immutable cache key plus ETag revalidation preserves that result,
			// so recomputing the full logical page hash on every hit is redundant.
			return true, visit(page, etag, true)
		}
	}
	if traceStats != nil {
		traceStats.decodedCacheMisses++
	}
	data, meta, ok, cached, err := s.loadParquetEntityPageObjectBytes(ctx, tenantID, version, spec)
	if err != nil || !ok {
		return ok, err
	}
	if traceStats != nil {
		if cached {
			traceStats.rawCacheHits++
		} else {
			traceStats.objectLoads++
		}
	}
	decodeStart := time.Now()
	page, err := decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, 0)
	if traceStats != nil {
		traceStats.parquetDecodes++
		traceStats.parquetDecodeDuration += time.Since(decodeStart)
	}
	readable := err == nil && entityPageReadable(page, tenantID, version, spec)
	if !readable && cached {
		s.dropCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash)
		var retryCached bool
		data, meta, ok, retryCached, err = s.loadParquetEntityPageObjectBytes(ctx, tenantID, version, spec)
		if err != nil || !ok {
			return ok, err
		}
		if traceStats != nil {
			if retryCached {
				traceStats.rawCacheHits++
			} else {
				traceStats.objectLoads++
			}
		}
		decodeStart = time.Now()
		page, err = decodeParquetEntityPage(ctx, data, tenantID, spec.Shard, 0)
		if traceStats != nil {
			traceStats.parquetDecodes++
			traceStats.parquetDecodeDuration += time.Since(decodeStart)
		}
		readable = err == nil && entityPageReadable(page, tenantID, version, spec)
	}
	if err != nil {
		return false, err
	}
	if readable {
		s.putCachedIndexObject("entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash, data, meta)
		s.putCachedEntityPage(tenantID, version, key, spec.ContentHash, spec.SchemaHash, page, meta.ETag)
	}
	return true, visit(page, meta.ETag, readable)
}

func (s *TenantStore) loadParquetEntityPageObjectBytes(ctx context.Context, tenantID string, version int64, spec EntityPageSpec) ([]byte, ObjectMeta, bool, bool, error) {
	key := firstIndexObjectKey(spec.Objects, "page", s.parquetEntityPageVersionKey(tenantID, version, spec.Shard))
	if data, meta, cached, err := s.cachedIndexObject(ctx, "entity_page", tenantID, version, key, spec.ContentHash, spec.SchemaHash); err != nil {
		return nil, ObjectMeta{}, false, false, err
	} else if cached {
		return data, meta, true, true, nil
	}
	data, meta, err := s.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, ObjectMeta{}, false, false, nil
	}
	if err != nil {
		return nil, ObjectMeta{}, false, false, err
	}
	return data, meta, true, false, nil
}

func (s *TenantStore) inspectParquetObject(ctx context.Context, data []byte, tenantID string, object IndexObject, item *IndexInspectionObject) {
	switch object.Codec {
	case parquetSecondaryIndexCodec:
		index, err := decodeParquetSecondaryIndex(
			ctx,
			data,
			tenantID,
			object.inspectKind,
			object.inspectField,
			0,
			object.inspectUnique,
		)
		if err != nil {
			item.InspectionError = err.Error()
			return
		}
		item.ObjectKind = "secondary-index"
		item.Codec = parquetSecondaryIndexCodec
		entryCount, _ := secondaryIndexCounts(index)
		item.RowCount = entryCount
		item.ContentHash = secondaryIndexContentHash(index)
	case parquetEdgeShardCodec:
		shard, err := decodeParquetEdgeShard(ctx, data, tenantID, object.inspectRelationType, object.inspectShard, 0)
		if err != nil {
			item.InspectionError = err.Error()
			return
		}
		item.ObjectKind = "edge-shard"
		item.Codec = parquetEdgeShardCodec
		item.RowCount = len(shard.Edges)
		item.ContentHash = edgeShardContentHash(shard)
	case parquetEntityPageCodec, "":
		page, err := decodeParquetEntityPage(ctx, data, tenantID, object.inspectShard, 0)
		if err != nil {
			item.InspectionError = err.Error()
			return
		}
		item.ObjectKind = "entity-page"
		item.Codec = parquetEntityPageCodec
		item.RowCount = len(page.Entities)
		item.ContentHash = entityPageContentHash(page)
	default:
		item.InspectionError = "unsupported parquet codec " + object.Codec
		return
	}
	item.HashMatches = object.ContentHash != "" && item.ContentHash == object.ContentHash
}
