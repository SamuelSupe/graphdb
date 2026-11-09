package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/codec"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func (s *TenantStore) writeParquetEntityPages(ctx context.Context, tenantID string, pages []EntityPageData, version int64) error {
	return s.writeParquetEntityPagesWithOptions(ctx, tenantID, pages, version, true)
}

func (s *TenantStore) writeParquetEntityPagesWithOptions(ctx context.Context, tenantID string, pages []EntityPageData, version int64, checkExisting bool) error {
	writeRecords := s.WriteEntityRecords
	groups := entityPageDataPackGroups(pages, !writeRecords, s.EntityPagePackMaxBytes)
	metas, err := s.writeParquetEntityPageGroups(ctx, tenantID, groups, checkExisting)
	if err != nil || !writeRecords {
		return err
	}
	recordJobs := []entityRecordWriteJob{}
	currentIDs := map[string]struct{}{}
	for index, group := range groups {
		for _, page := range group.Pages {
			pageHash := entityPageContentHash(page)
			for _, entity := range page.Entities {
				record := newEntityRecord(tenantID, entity, page.Shard, pageHash, metas[index].ETag, page.Version, page.UpdatedAt)
				key := s.entityRecordKey(tenantID, entity.ID)
				currentIDs[entity.ID] = struct{}{}
				recordJobs = append(recordJobs, entityRecordWriteJob{Key: key, Record: record})
			}
		}
	}
	if err := s.putEntityRecordBatch(ctx, recordJobs); err != nil {
		return err
	}
	return s.tombstoneStaleEntityRecords(ctx, tenantID, currentIDs, version)
}

func (s *TenantStore) writeParquetEntityPageGroups(ctx context.Context, tenantID string, groups []entityPageDataPackGroup, checkExisting bool) ([]ObjectMeta, error) {
	metas := make([]ObjectMeta, len(groups))
	err := s.runFileWriteJobs(ctx, len(groups), func(workCtx context.Context, index int) error {
		group := groups[index]
		for i := range group.Pages {
			group.Pages[i].TenantID = tenantID
		}
		pack := mergeEntityPagePack(group)
		pack.TenantID = tenantID
		key := s.parquetEntityPageVersionKey(tenantID, pack.Version, pack.Shard)
		if pack.pageRole != "" {
			key = s.parquetEntityPageVersionKey(tenantID, pack.Version, group.ID)
		}
		if len(group.Pages) > 1 {
			key = s.parquetEntityPagePackVersionKey(tenantID, pack.Version, group.ID)
			if checkExisting {
				if pageMeta, ok, err := s.existingParquetEntityPagePackMeta(workCtx, key, tenantID, group.Pages); err != nil || ok {
					metas[index] = pageMeta
					return err
				}
			}
		}
		pageMeta, err := s.putParquetEntityPageObject(workCtx, key, tenantID, pack, checkExisting)
		metas[index] = pageMeta
		return err
	})
	return metas, err
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
	pageUpdatedAt := parquetio.FormatTime(page.UpdatedAt)

	// Physical shard locality lets min/max statistics skip unrelated row groups.
	// Keep page.Entities in its canonical order for logical content hashes.
	type rowPosition struct {
		index int
		shard string
	}
	var order []rowPosition
	if isIndexPackID(page.Shard) {
		order = make([]rowPosition, len(page.Entities))
		for i, entity := range page.Entities {
			order[i] = rowPosition{index: i, shard: entityShardID(entity.ID)}
		}
		sort.Slice(order, func(i, j int) bool {
			if order[i].shard != order[j].shard {
				return order[i].shard < order[j].shard
			}
			return page.Entities[order[i].index].ID < page.Entities[order[j].index].ID
		})
	}
	var shardRowEnds []int64
	lastShard := ""
	for i := range page.Entities {
		if i&255 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		entity := page.Entities[i]
		rowShard := page.Shard
		if order != nil {
			entity = page.Entities[order[i].index]
			rowShard = order[i].shard
		}
		if i > 0 && rowShard != lastShard {
			shardRowEnds = append(shardRowEnds, int64(builder.Field(0).Len()))
		}
		lastShard = rowShard
		rows, err := codec.EntityRows(entity)
		if err != nil {
			return nil, err
		}
		createdAt := parquetio.FormatTime(entity.CreatedAt)
		updatedAt := parquetio.FormatTime(entity.UpdatedAt)
		for _, row := range rows {
			builder.Field(parquetEntityColumnTenantID).(*array.StringBuilder).Append(page.TenantID)
			builder.Field(parquetEntityColumnShard).(*array.StringBuilder).Append(rowShard)
			builder.Field(parquetEntityColumnPageVersion).(*array.Int64Builder).Append(page.Version)
			builder.Field(parquetEntityColumnPageUpdatedAt).(*array.StringBuilder).Append(pageUpdatedAt)
			builder.Field(parquetEntityColumnID).(*array.StringBuilder).Append(entity.ID)
			builder.Field(parquetEntityColumnKind).(*array.StringBuilder).Append(entity.Kind)
			builder.Field(parquetEntityColumnSource).(*array.StringBuilder).Append(entity.Source)
			builder.Field(parquetEntityColumnExternalID).(*array.StringBuilder).Append(entity.ExternalID)
			builder.Field(parquetEntityColumnEntityVersion).(*array.Int64Builder).Append(entity.Version)
			builder.Field(parquetEntityColumnEntityCreatedAt).(*array.StringBuilder).Append(createdAt)
			builder.Field(parquetEntityColumnEntityUpdatedAt).(*array.StringBuilder).Append(updatedAt)
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
			builder.Field(parquetEntityColumnFieldSourceUpdatedAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.FieldSource.UpdatedAt))
			builder.Field(parquetEntityColumnEntitySourceSource).(*array.StringBuilder).Append(row.EntitySource.Source)
			builder.Field(parquetEntityColumnEntitySourceExternalID).(*array.StringBuilder).Append(row.EntitySource.ExternalID)
			builder.Field(parquetEntityColumnEntitySourceConfidence).(*array.Float64Builder).Append(row.EntitySource.Confidence)
			builder.Field(parquetEntityColumnEntitySourcePriority).(*array.Int64Builder).Append(int64(row.EntitySource.Priority))
			builder.Field(parquetEntityColumnEntitySourceObservedAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.EntitySource.ObservedAt))
			builder.Field(parquetEntityColumnEntitySourceStale).(*array.BooleanBuilder).Append(row.EntitySource.Stale)
			builder.Field(parquetEntityColumnEntitySourceStaleAt).(*array.StringBuilder).Append(parquetio.FormatTime(row.EntitySource.StaleAt))
		}
	}

	record := builder.NewRecordBatch()
	defer record.Release()
	var buf bytes.Buffer
	writerProps := parquetio.DataWriterProperties(schema)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	writer, err := pqarrow.NewFileWriter(schema, &buf, writerProps, arrowProps)
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	// Align larger shards while avoiding a wide footer for each tiny shard.
	// Small packs retain the existing 512-row statistics-pruning granularity.
	shardRowEnds = append(shardRowEnds, record.NumRows())
	rowGroupSize := parquetEntityPageRowGroupSize
	previousEnd := int64(0)
	for _, end := range shardRowEnds {
		if end-previousEnd < 512 {
			shardRowEnds = []int64{record.NumRows()}
			rowGroupSize = 512
			break
		}
		previousEnd = end
	}
	start := int64(0)
	for _, end := range shardRowEnds {
		for start < end {
			stop := min(start+rowGroupSize, end)
			slice := record.NewSlice(start, stop)
			err := writer.Write(slice)
			slice.Release()
			if err != nil {
				return nil, err
			}
			start = stop
		}
	}
	if record.NumRows() == 0 {
		if err := writer.Write(record); err != nil {
			return nil, err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetEntityPage(ctx context.Context, data []byte, tenantID string, shard string, version int64) (EntityPageData, error) {
	return decodeParquetEntityPageReader(ctx, bytes.NewReader(data), tenantID, shard, version)
}

func decodeParquetEntityPageReader(ctx context.Context, source parquet.ReaderAtSeeker, tenantID string, shard string, version int64) (EntityPageData, error) {
	reader, err := pqfile.NewParquetReader(source)
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
	rowGroups := parquetEntityShardRowGroups(reader, shard)
	if len(rowGroups) == 0 {
		return page, objectContextErr(ctx)
	}
	recordReader, release, err := parquetio.ReadRecordReader(ctx, fileReader, columns, rowGroups)
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
		page.Entities = append(page.Entities, codec.DecodedEntityCopy(*entity))
	}
	sort.Slice(page.Entities, func(i, j int) bool { return page.Entities[i].ID < page.Entities[j].ID })
	return page, nil
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
			page.UpdatedAt = parquetio.ParseTime(columns.pageUpdatedAt.Value(i))
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
				CreatedAt:  parquetio.ParseTime(columns.entityCreatedAt.Value(i)),
				UpdatedAt:  parquetio.ParseTime(columns.entityUpdatedAt.Value(i)),
				Confidence: columns.confidence.Value(i),
				SourceRank: int(columns.sourceRank.Value(i)),
				SplitFrom:  strings.Clone(columns.splitFrom.Value(i)),
			}
			byID[created.ID] = &created
			entity = &created
		}
		if err := codec.ApplyEntityRow(entity, entityPageRowFromColumns(columns, i)); err != nil {
			return err
		}
	}
	return nil
}
