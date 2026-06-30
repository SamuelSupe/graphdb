package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"graphdb/internal/graph"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	snapshotSchemaCodecParquet  = "snapshot-schema-arrow-parquet-v1"
	snapshotCatalogCodecParquet = "snapshot-catalog-arrow-parquet-v1"
)

const (
	parquetSnapshotCatalogColumnLayoutVersion = iota
	parquetSnapshotCatalogColumnTenantID
	parquetSnapshotCatalogColumnVersion
	parquetSnapshotCatalogColumnKey
	parquetSnapshotCatalogColumnFormat
	parquetSnapshotCatalogColumnUpdatedAt
	parquetSnapshotCatalogColumnContentHash
	parquetSnapshotCatalogColumnSchemaKey
	parquetSnapshotCatalogColumnSchemaFormat
	parquetSnapshotCatalogColumnSchemaContentHash
	parquetSnapshotCatalogColumnEntityPageCount
	parquetSnapshotCatalogColumnEdgeShardCount
	parquetSnapshotCatalogColumnRowKind
	parquetSnapshotCatalogColumnOrdinal
	parquetSnapshotCatalogColumnShard
	parquetSnapshotCatalogColumnRelationType
	parquetSnapshotCatalogColumnObjectKey
	parquetSnapshotCatalogColumnObjectFormat
	parquetSnapshotCatalogColumnObjectCount
	parquetSnapshotCatalogColumnObjectContentHash
)

const (
	snapshotCatalogRowMetadata   = "metadata"
	snapshotCatalogRowEntityPage = "entity_page"
	snapshotCatalogRowEdgeShard  = "edge_shard"
)

func marshalParquetSnapshotSchema(ctx context.Context, schemaData snapshotSchemaData) ([]byte, error) {
	normalized, hash, err := normalizeSnapshotSchemaForParquet(schemaData)
	if err != nil {
		return nil, err
	}
	rows := snapshotSchemaRows(normalized)
	schema := parquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	header := graphCommitHeaderForSnapshotSchema(normalized)
	for _, row := range rows {
		appendParquetCommitRow(builder, normalized.TenantID, "", header, hash, row)
	}
	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
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

func decodeParquetSnapshotSchema(ctx context.Context, data []byte) (snapshotSchemaData, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return snapshotSchemaData{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return snapshotSchemaData{}, fmt.Errorf("parquet snapshot schema is empty")
	}
	if table.NumCols() < int64(parquetCommitColumnEdgeSourceObservedAt+1) {
		return snapshotSchemaData{}, fmt.Errorf("parquet snapshot schema has %d columns, want at least %d", table.NumCols(), parquetCommitColumnEdgeSourceObservedAt+1)
	}

	var schemaData snapshotSchemaData
	var expectedHash string
	build := &commitBuild{}
	rows := 0
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetCommitColumns(batch)
		if err != nil {
			return snapshotSchemaData{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowSchema := snapshotSchemaData{
				LayoutVersion: CurrentObjectLayoutVersion,
				TenantID:      columns.commitTenantID.Value(i),
				Version:       columns.version.Value(i),
				UpdatedAt:     parseParquetTime(columns.createdAt.Value(i)),
			}
			if rows == 0 {
				schemaData = rowSchema
				expectedHash = columns.contentHash.Value(i)
			} else if schemaData.TenantID != rowSchema.TenantID || schemaData.Version != rowSchema.Version || !schemaData.UpdatedAt.Equal(rowSchema.UpdatedAt) || expectedHash != columns.contentHash.Value(i) {
				return snapshotSchemaData{}, fmt.Errorf("snapshot schema identity mismatch")
			}
			row := parquetCommitRowFromColumns(columns, i)
			switch row.Kind {
			case commitRowMetadata:
			case commitRowUpsertCIType, commitRowCITypeExtends, commitRowCITypeField, commitRowCITypeFieldEnum, commitRowCITypeFieldDefault, commitRowCITypeIdentity, commitRowCITypeIdentityField:
				if err := build.applyCITypeRow(row); err != nil {
					return snapshotSchemaData{}, err
				}
			case commitRowUpsertRelationType, commitRowRelationFromKind, commitRowRelationToKind:
				if err := build.applyRelationRow(row); err != nil {
					return snapshotSchemaData{}, err
				}
			default:
				return snapshotSchemaData{}, fmt.Errorf("unknown snapshot schema row kind %q", row.Kind)
			}
			rows++
		}
	}
	for _, ordinal := range sortedIntKeys(build.ciTypes) {
		schemaData.CITypes = setCITypeAt(schemaData.CITypes, ordinal, build.ciTypes[ordinal].item)
	}
	for _, ordinal := range sortedIntKeys(build.relations) {
		schemaData.RelationTypes = setRelationTypeAt(schemaData.RelationTypes, ordinal, build.relations[ordinal].item)
	}
	if err := normalizeObjectAfterRead(&schemaData, "snapshot schema"); err != nil {
		return snapshotSchemaData{}, err
	}
	if expectedHash == "" || snapshotSchemaContentHash(schemaData) != expectedHash {
		return snapshotSchemaData{}, fmt.Errorf("snapshot schema content hash mismatch")
	}
	return schemaData, nil
}

func normalizeSnapshotSchemaForParquet(schemaData snapshotSchemaData) (snapshotSchemaData, string, error) {
	payload, err := snapshotSchemaPayloadJSON(schemaData)
	if err != nil {
		return snapshotSchemaData{}, "", err
	}
	var normalized snapshotSchemaData
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return snapshotSchemaData{}, "", err
	}
	if err := normalizeObjectAfterRead(&normalized, "snapshot schema"); err != nil {
		return snapshotSchemaData{}, "", err
	}
	return normalized, objectContentHash(payload), nil
}

func graphCommitHeaderForSnapshotSchema(schemaData snapshotSchemaData) graph.Commit {
	return graph.Commit{
		LayoutVersion: CurrentObjectLayoutVersion,
		TenantID:      schemaData.TenantID,
		ID:            "snapshot-schema",
		Version:       schemaData.Version,
		CreatedAt:     schemaData.UpdatedAt,
	}
}

func snapshotSchemaRows(schemaData snapshotSchemaData) []parquetCommitRow {
	rows := []parquetCommitRow{{Kind: commitRowMetadata}}
	for i, ciType := range schemaData.CITypes {
		rows = append(rows, ciTypeRows(i, ciType)...)
	}
	for i, relationType := range schemaData.RelationTypes {
		rows = append(rows, relationTypeRows(i, relationType)...)
	}
	return rows
}

func marshalParquetShardedSnapshotCatalog(ctx context.Context, catalog ShardedSnapshotCatalog) ([]byte, error) {
	catalog = normalizeShardedSnapshotCatalog(catalog)
	hash := shardedSnapshotCatalogContentHash(catalog)
	schema := parquetSnapshotCatalogArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendRow := func(row snapshotCatalogRow) {
		builder.Field(parquetSnapshotCatalogColumnLayoutVersion).(*array.Int64Builder).Append(int64(catalog.LayoutVersion))
		builder.Field(parquetSnapshotCatalogColumnTenantID).(*array.StringBuilder).Append(catalog.TenantID)
		builder.Field(parquetSnapshotCatalogColumnVersion).(*array.Int64Builder).Append(catalog.Version)
		builder.Field(parquetSnapshotCatalogColumnKey).(*array.StringBuilder).Append(catalog.Key)
		builder.Field(parquetSnapshotCatalogColumnFormat).(*array.StringBuilder).Append(catalog.Format)
		builder.Field(parquetSnapshotCatalogColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(catalog.UpdatedAt))
		builder.Field(parquetSnapshotCatalogColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetSnapshotCatalogColumnSchemaKey).(*array.StringBuilder).Append(catalog.Schema.Key)
		builder.Field(parquetSnapshotCatalogColumnSchemaFormat).(*array.StringBuilder).Append(catalog.Schema.Format)
		builder.Field(parquetSnapshotCatalogColumnSchemaContentHash).(*array.StringBuilder).Append(catalog.Schema.ContentHash)
		builder.Field(parquetSnapshotCatalogColumnEntityPageCount).(*array.Int64Builder).Append(int64(len(catalog.EntityPages)))
		builder.Field(parquetSnapshotCatalogColumnEdgeShardCount).(*array.Int64Builder).Append(int64(len(catalog.EdgeShards)))
		builder.Field(parquetSnapshotCatalogColumnRowKind).(*array.StringBuilder).Append(row.Kind)
		builder.Field(parquetSnapshotCatalogColumnOrdinal).(*array.Int64Builder).Append(int64(row.Ordinal))
		builder.Field(parquetSnapshotCatalogColumnShard).(*array.StringBuilder).Append(row.Shard)
		builder.Field(parquetSnapshotCatalogColumnRelationType).(*array.StringBuilder).Append(row.RelationType)
		builder.Field(parquetSnapshotCatalogColumnObjectKey).(*array.StringBuilder).Append(row.Key)
		builder.Field(parquetSnapshotCatalogColumnObjectFormat).(*array.StringBuilder).Append(row.Format)
		builder.Field(parquetSnapshotCatalogColumnObjectCount).(*array.Int64Builder).Append(int64(row.Count))
		builder.Field(parquetSnapshotCatalogColumnObjectContentHash).(*array.StringBuilder).Append(row.ContentHash)
	}
	rows := shardedSnapshotCatalogRows(catalog)
	if len(rows) == 0 {
		rows = []snapshotCatalogRow{{Kind: snapshotCatalogRowMetadata}}
	}
	for _, row := range rows {
		appendRow(row)
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, 1, writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetShardedSnapshotCatalog(ctx context.Context, data []byte) (ShardedSnapshotCatalog, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return ShardedSnapshotCatalog{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return ShardedSnapshotCatalog{}, fmt.Errorf("parquet snapshot catalog is empty")
	}
	if table.NumCols() < int64(parquetSnapshotCatalogColumnObjectContentHash+1) {
		return ShardedSnapshotCatalog{}, fmt.Errorf("parquet snapshot catalog has %d columns, want at least %d", table.NumCols(), parquetSnapshotCatalogColumnObjectContentHash+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	var catalog ShardedSnapshotCatalog
	var expectedHash string
	var entityPageCount int64
	var edgeShardCount int64
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetSnapshotCatalogColumns(batch)
		if err != nil {
			return ShardedSnapshotCatalog{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowCatalog := ShardedSnapshotCatalog{
				LayoutVersion: int(columns.layoutVersion.Value(i)),
				TenantID:      columns.tenantID.Value(i),
				Key:           columns.key.Value(i),
				Version:       columns.version.Value(i),
				Format:        columns.format.Value(i),
				Schema: SnapshotSchemaSpec{
					Key:         columns.schemaKey.Value(i),
					Format:      columns.schemaFormat.Value(i),
					ContentHash: columns.schemaContentHash.Value(i),
				},
				UpdatedAt: parseParquetTime(columns.updatedAt.Value(i)),
			}
			if rows == 0 {
				catalog = rowCatalog
				expectedHash = columns.contentHash.Value(i)
				entityPageCount = columns.entityPageCount.Value(i)
				edgeShardCount = columns.edgeShardCount.Value(i)
			} else if !sameShardedSnapshotCatalogMetadata(catalog, rowCatalog) ||
				expectedHash != columns.contentHash.Value(i) ||
				entityPageCount != columns.entityPageCount.Value(i) ||
				edgeShardCount != columns.edgeShardCount.Value(i) {
				return ShardedSnapshotCatalog{}, fmt.Errorf("snapshot catalog identity mismatch")
			}
			if err := applySnapshotCatalogRow(&catalog, snapshotCatalogRowFromColumns(columns, i)); err != nil {
				return ShardedSnapshotCatalog{}, err
			}
			rows++
		}
	}
	if int64(len(catalog.EntityPages)) != entityPageCount || int64(len(catalog.EdgeShards)) != edgeShardCount {
		return ShardedSnapshotCatalog{}, fmt.Errorf("snapshot catalog count mismatch")
	}
	if expectedHash == "" || shardedSnapshotCatalogContentHash(catalog) != expectedHash {
		return ShardedSnapshotCatalog{}, fmt.Errorf("snapshot catalog content hash mismatch")
	}
	return catalog, nil
}

type snapshotCatalogRow struct {
	Kind         string
	Ordinal      int
	Shard        string
	RelationType string
	Key          string
	Format       string
	Count        int
	ContentHash  string
}

type parquetSnapshotCatalogColumnSet struct {
	layoutVersion     *array.Int64
	tenantID          *array.String
	version           *array.Int64
	key               *array.String
	format            *array.String
	updatedAt         *array.String
	contentHash       *array.String
	schemaKey         *array.String
	schemaFormat      *array.String
	schemaContentHash *array.String
	entityPageCount   *array.Int64
	edgeShardCount    *array.Int64
	rowKind           *array.String
	ordinal           *array.Int64
	shard             *array.String
	relationType      *array.String
	objectKey         *array.String
	objectFormat      *array.String
	objectCount       *array.Int64
	objectContentHash *array.String
}

func shardedSnapshotCatalogRows(catalog ShardedSnapshotCatalog) []snapshotCatalogRow {
	rows := []snapshotCatalogRow{}
	for i, page := range catalog.EntityPages {
		rows = append(rows, snapshotCatalogRow{
			Kind:        snapshotCatalogRowEntityPage,
			Ordinal:     i,
			Shard:       page.Shard,
			Key:         page.Key,
			Format:      page.Format,
			Count:       page.EntityCount,
			ContentHash: page.ContentHash,
		})
	}
	for i, shard := range catalog.EdgeShards {
		rows = append(rows, snapshotCatalogRow{
			Kind:         snapshotCatalogRowEdgeShard,
			Ordinal:      i,
			RelationType: shard.RelationType,
			Shard:        shard.Shard,
			Key:          shard.Key,
			Format:       shard.Format,
			Count:        shard.EdgeCount,
			ContentHash:  shard.ContentHash,
		})
	}
	return rows
}

func applySnapshotCatalogRow(catalog *ShardedSnapshotCatalog, row snapshotCatalogRow) error {
	switch row.Kind {
	case snapshotCatalogRowMetadata:
		return nil
	case snapshotCatalogRowEntityPage:
		if len(catalog.EntityPages) != row.Ordinal {
			return fmt.Errorf("snapshot catalog entity page ordinal mismatch")
		}
		catalog.EntityPages = append(catalog.EntityPages, SnapshotEntityPageSpec{
			Shard:       row.Shard,
			Key:         row.Key,
			Format:      row.Format,
			EntityCount: row.Count,
			ContentHash: row.ContentHash,
		})
	case snapshotCatalogRowEdgeShard:
		if len(catalog.EdgeShards) != row.Ordinal {
			return fmt.Errorf("snapshot catalog edge shard ordinal mismatch")
		}
		catalog.EdgeShards = append(catalog.EdgeShards, SnapshotEdgeShardSpec{
			RelationType: row.RelationType,
			Shard:        row.Shard,
			Key:          row.Key,
			Format:       row.Format,
			EdgeCount:    row.Count,
			ContentHash:  row.ContentHash,
		})
	default:
		return fmt.Errorf("unknown snapshot catalog row kind %q", row.Kind)
	}
	return nil
}

func parquetSnapshotCatalogColumns(batch arrow.RecordBatch) (parquetSnapshotCatalogColumnSet, error) {
	var columns parquetSnapshotCatalogColumnSet
	var err error
	if columns.layoutVersion, err = parquetInt64Column(batch, parquetSnapshotCatalogColumnLayoutVersion, "layout_version"); err != nil {
		return columns, err
	}
	if columns.tenantID, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.version, err = parquetInt64Column(batch, parquetSnapshotCatalogColumnVersion, "version"); err != nil {
		return columns, err
	}
	if columns.key, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnKey, "key"); err != nil {
		return columns, err
	}
	if columns.format, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnFormat, "format"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	if columns.contentHash, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnContentHash, "content_hash"); err != nil {
		return columns, err
	}
	if columns.schemaKey, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnSchemaKey, "schema_key"); err != nil {
		return columns, err
	}
	if columns.schemaFormat, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnSchemaFormat, "schema_format"); err != nil {
		return columns, err
	}
	if columns.schemaContentHash, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnSchemaContentHash, "schema_content_hash"); err != nil {
		return columns, err
	}
	if columns.entityPageCount, err = parquetInt64Column(batch, parquetSnapshotCatalogColumnEntityPageCount, "entity_page_count"); err != nil {
		return columns, err
	}
	if columns.edgeShardCount, err = parquetInt64Column(batch, parquetSnapshotCatalogColumnEdgeShardCount, "edge_shard_count"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetInt64Column(batch, parquetSnapshotCatalogColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.shard, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnShard, "shard"); err != nil {
		return columns, err
	}
	if columns.relationType, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnRelationType, "relation_type"); err != nil {
		return columns, err
	}
	if columns.objectKey, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnObjectKey, "object_key"); err != nil {
		return columns, err
	}
	if columns.objectFormat, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnObjectFormat, "object_format"); err != nil {
		return columns, err
	}
	if columns.objectCount, err = parquetInt64Column(batch, parquetSnapshotCatalogColumnObjectCount, "object_count"); err != nil {
		return columns, err
	}
	if columns.objectContentHash, err = parquetStringColumn(batch, parquetSnapshotCatalogColumnObjectContentHash, "object_content_hash"); err != nil {
		return columns, err
	}
	return columns, nil
}

func snapshotCatalogRowFromColumns(columns parquetSnapshotCatalogColumnSet, row int) snapshotCatalogRow {
	return snapshotCatalogRow{
		Kind:         columns.rowKind.Value(row),
		Ordinal:      int(columns.ordinal.Value(row)),
		Shard:        columns.shard.Value(row),
		RelationType: columns.relationType.Value(row),
		Key:          columns.objectKey.Value(row),
		Format:       columns.objectFormat.Value(row),
		Count:        int(columns.objectCount.Value(row)),
		ContentHash:  columns.objectContentHash.Value(row),
	}
}

func parquetSnapshotCatalogArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "layout_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "format", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "schema_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "schema_format", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "schema_content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "entity_page_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "edge_shard_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "shard", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "relation_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "object_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "object_format", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "object_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "object_content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetSnapshotCatalogSchemaHash() string {
	return objectSchemaHash([]string{
		"layout_version",
		"tenant_id",
		"version",
		"key",
		"format",
		"updated_at",
		"content_hash",
		"schema_key",
		"schema_format",
		"schema_content_hash",
		"entity_page_count",
		"edge_shard_count",
		"row_kind",
		"ordinal",
		"shard",
		"relation_type",
		"object_key",
		"object_format",
		"object_count",
		"object_content_hash",
		snapshotCatalogCodecParquet,
	})
}

func normalizeShardedSnapshotCatalog(catalog ShardedSnapshotCatalog) ShardedSnapshotCatalog {
	catalog.LayoutVersion = CurrentObjectLayoutVersion
	catalog.EntityPages = append([]SnapshotEntityPageSpec(nil), catalog.EntityPages...)
	catalog.EdgeShards = append([]SnapshotEdgeShardSpec(nil), catalog.EdgeShards...)
	return catalog
}

func sameShardedSnapshotCatalogMetadata(left ShardedSnapshotCatalog, right ShardedSnapshotCatalog) bool {
	return left.LayoutVersion == right.LayoutVersion &&
		left.TenantID == right.TenantID &&
		left.Key == right.Key &&
		left.Version == right.Version &&
		left.Format == right.Format &&
		left.Schema == right.Schema &&
		formatParquetTime(left.UpdatedAt) == formatParquetTime(right.UpdatedAt)
}

func snapshotSchemaPayloadJSON(schemaData snapshotSchemaData) ([]byte, error) {
	schemaData.LayoutVersion = CurrentObjectLayoutVersion
	return json.Marshal(schemaData)
}

func (s *TenantStore) putSnapshotSchemaIfAbsentOrSame(ctx context.Context, key string, schemaData snapshotSchemaData) error {
	data, err := marshalParquetSnapshotSchema(ctx, schemaData)
	if err != nil {
		return err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	decoded, err := decodeSnapshotSchemaObject(ctx, existing)
	if err != nil || snapshotSchemaContentHash(decoded) != snapshotSchemaContentHash(schemaData) {
		return fmt.Errorf("%w: object %q already exists with different content", ErrConflict, key)
	}
	return nil
}

func (s *TenantStore) putShardedSnapshotCatalogIfAbsentOrSame(ctx context.Context, key string, catalog ShardedSnapshotCatalog) error {
	data, err := marshalParquetShardedSnapshotCatalog(ctx, catalog)
	if err != nil {
		return err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err == nil {
		return nil
	} else if !errors.Is(err, ErrConflict) {
		return err
	}
	existing, err := s.Objects.Get(ctx, key)
	if err != nil {
		return err
	}
	decoded, err := decodeShardedSnapshotCatalogObject(ctx, existing)
	if err != nil || shardedSnapshotCatalogContentHash(decoded) != shardedSnapshotCatalogContentHash(catalog) {
		return fmt.Errorf("%w: object %q already exists with different content", ErrConflict, key)
	}
	return nil
}

func decodeSnapshotSchemaObject(ctx context.Context, data []byte) (snapshotSchemaData, error) {
	if !isParquetBytes(data) {
		return snapshotSchemaData{}, fmt.Errorf("unsupported snapshot schema: only parquet schemas are readable")
	}
	return decodeParquetSnapshotSchema(ctx, data)
}

func decodeShardedSnapshotCatalogObject(ctx context.Context, data []byte) (ShardedSnapshotCatalog, error) {
	if !isParquetBytes(data) {
		return ShardedSnapshotCatalog{}, fmt.Errorf("unsupported snapshot catalog: only parquet catalogs are readable")
	}
	return decodeParquetShardedSnapshotCatalog(ctx, data)
}

func snapshotSchemaContentHash(schemaData snapshotSchemaData) string {
	payload, err := snapshotSchemaPayloadJSON(schemaData)
	if err != nil {
		return ""
	}
	return objectContentHash(payload)
}

func shardedSnapshotCatalogContentHash(catalog ShardedSnapshotCatalog) string {
	catalog = normalizeShardedSnapshotCatalog(catalog)
	parts := []string{
		formatInt64ForHash(int64(catalog.LayoutVersion)),
		catalog.TenantID,
		catalog.Key,
		formatInt64ForHash(catalog.Version),
		catalog.Format,
		catalog.Schema.Key,
		catalog.Schema.Format,
		catalog.Schema.ContentHash,
		formatParquetTime(catalog.UpdatedAt),
		formatInt64ForHash(int64(len(catalog.EntityPages))),
		formatInt64ForHash(int64(len(catalog.EdgeShards))),
	}
	for _, row := range shardedSnapshotCatalogRows(catalog) {
		parts = append(parts,
			row.Kind,
			formatInt64ForHash(int64(row.Ordinal)),
			row.RelationType,
			row.Shard,
			row.Key,
			row.Format,
			formatInt64ForHash(int64(row.Count)),
			row.ContentHash,
		)
	}
	return parquetScalarContentHash(parts...)
}
