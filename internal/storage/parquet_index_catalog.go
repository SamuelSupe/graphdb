package storage

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const indexCatalogCodecParquet = "index-catalog-arrow-parquet-v1"

const (
	parquetIndexCatalogColumnLayoutVersion = iota
	parquetIndexCatalogColumnTenantID
	parquetIndexCatalogColumnVersion
	parquetIndexCatalogColumnUpdatedAt
	parquetIndexCatalogColumnCatalogHash
	parquetIndexCatalogColumnIndexCount
	parquetIndexCatalogColumnEdgeShardCount
	parquetIndexCatalogColumnEntityPageCount
	parquetIndexCatalogColumnRowKind
	parquetIndexCatalogColumnParentOrdinal
	parquetIndexCatalogColumnChildOrdinal
	parquetIndexCatalogColumnName
	parquetIndexCatalogColumnKind
	parquetIndexCatalogColumnField
	parquetIndexCatalogColumnRelationType
	parquetIndexCatalogColumnShard
	parquetIndexCatalogColumnIndexType
	parquetIndexCatalogColumnStatus
	parquetIndexCatalogColumnFormat
	parquetIndexCatalogColumnCodec
	parquetIndexCatalogColumnRole
	parquetIndexCatalogColumnObjectKey
	parquetIndexCatalogColumnRowCount
	parquetIndexCatalogColumnEntryCount
	parquetIndexCatalogColumnDistinctValues
	parquetIndexCatalogColumnEdgeCount
	parquetIndexCatalogColumnEntityCount
	parquetIndexCatalogColumnStatValue
	parquetIndexCatalogColumnStatCount
	parquetIndexCatalogColumnObjectContentHash
	parquetIndexCatalogColumnSchemaHash
	parquetIndexCatalogColumnSpecUpdatedAt
)

const (
	indexCatalogRowMetadata         = "metadata"
	indexCatalogRowIndex            = "index"
	indexCatalogRowIndexObject      = "index_object"
	indexCatalogRowIndexTopValue    = "index_top_value"
	indexCatalogRowEdgeShard        = "edge_shard"
	indexCatalogRowEdgeShardObject  = "edge_shard_object"
	indexCatalogRowEntityPage       = "entity_page"
	indexCatalogRowEntityPageObject = "entity_page_object"
)

func marshalParquetIndexCatalog(ctx context.Context, catalog IndexCatalog) ([]byte, error) {
	catalog = normalizeIndexCatalogForParquet(catalog)
	hash, err := indexCatalogContentHash(catalog)
	if err != nil {
		return nil, err
	}
	schema := parquetIndexCatalogArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendRow := func(row indexCatalogRow) {
		builder.Field(parquetIndexCatalogColumnLayoutVersion).(*array.Int64Builder).Append(int64(catalog.LayoutVersion))
		builder.Field(parquetIndexCatalogColumnTenantID).(*array.StringBuilder).Append(catalog.TenantID)
		builder.Field(parquetIndexCatalogColumnVersion).(*array.Int64Builder).Append(catalog.Version)
		builder.Field(parquetIndexCatalogColumnUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(catalog.UpdatedAt))
		builder.Field(parquetIndexCatalogColumnCatalogHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetIndexCatalogColumnIndexCount).(*array.Int64Builder).Append(int64(len(catalog.Indexes)))
		builder.Field(parquetIndexCatalogColumnEdgeShardCount).(*array.Int64Builder).Append(int64(len(catalog.EdgeShards)))
		builder.Field(parquetIndexCatalogColumnEntityPageCount).(*array.Int64Builder).Append(int64(len(catalog.EntityPages)))
		builder.Field(parquetIndexCatalogColumnRowKind).(*array.StringBuilder).Append(row.Kind)
		builder.Field(parquetIndexCatalogColumnParentOrdinal).(*array.Int64Builder).Append(int64(row.ParentOrdinal))
		builder.Field(parquetIndexCatalogColumnChildOrdinal).(*array.Int64Builder).Append(int64(row.ChildOrdinal))
		builder.Field(parquetIndexCatalogColumnName).(*array.StringBuilder).Append(row.Name)
		builder.Field(parquetIndexCatalogColumnKind).(*array.StringBuilder).Append(row.EntityKind)
		builder.Field(parquetIndexCatalogColumnField).(*array.StringBuilder).Append(row.Field)
		builder.Field(parquetIndexCatalogColumnRelationType).(*array.StringBuilder).Append(row.RelationType)
		builder.Field(parquetIndexCatalogColumnShard).(*array.StringBuilder).Append(row.Shard)
		builder.Field(parquetIndexCatalogColumnIndexType).(*array.StringBuilder).Append(row.IndexType)
		builder.Field(parquetIndexCatalogColumnStatus).(*array.StringBuilder).Append(row.Status)
		builder.Field(parquetIndexCatalogColumnFormat).(*array.StringBuilder).Append(row.Format)
		builder.Field(parquetIndexCatalogColumnCodec).(*array.StringBuilder).Append(row.Codec)
		builder.Field(parquetIndexCatalogColumnRole).(*array.StringBuilder).Append(row.Role)
		builder.Field(parquetIndexCatalogColumnObjectKey).(*array.StringBuilder).Append(row.ObjectKey)
		builder.Field(parquetIndexCatalogColumnRowCount).(*array.Int64Builder).Append(int64(row.RowCount))
		builder.Field(parquetIndexCatalogColumnEntryCount).(*array.Int64Builder).Append(int64(row.EntryCount))
		builder.Field(parquetIndexCatalogColumnDistinctValues).(*array.Int64Builder).Append(int64(row.DistinctValues))
		builder.Field(parquetIndexCatalogColumnEdgeCount).(*array.Int64Builder).Append(int64(row.EdgeCount))
		builder.Field(parquetIndexCatalogColumnEntityCount).(*array.Int64Builder).Append(int64(row.EntityCount))
		builder.Field(parquetIndexCatalogColumnStatValue).(*array.StringBuilder).Append(row.StatValue)
		builder.Field(parquetIndexCatalogColumnStatCount).(*array.Int64Builder).Append(int64(row.StatCount))
		builder.Field(parquetIndexCatalogColumnObjectContentHash).(*array.StringBuilder).Append(row.ContentHash)
		builder.Field(parquetIndexCatalogColumnSchemaHash).(*array.StringBuilder).Append(row.SchemaHash)
		builder.Field(parquetIndexCatalogColumnSpecUpdatedAt).(*array.StringBuilder).Append(formatParquetTime(row.UpdatedAt))
	}

	rows := indexCatalogRows(catalog)
	if len(rows) == 0 {
		rows = []indexCatalogRow{{Kind: indexCatalogRowMetadata}}
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

func decodeParquetIndexCatalog(ctx context.Context, data []byte) (IndexCatalog, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return IndexCatalog{}, err
	}
	defer table.Release()
	if table.NumRows() < 1 {
		return IndexCatalog{}, fmt.Errorf("parquet index catalog is empty")
	}
	if table.NumCols() < int64(parquetIndexCatalogColumnSpecUpdatedAt+1) {
		return IndexCatalog{}, fmt.Errorf("parquet index catalog has %d columns, want at least %d", table.NumCols(), parquetIndexCatalogColumnSpecUpdatedAt+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	var catalog IndexCatalog
	var expectedHash string
	var indexCount int64
	var edgeShardCount int64
	var entityPageCount int64
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetIndexCatalogColumns(batch)
		if err != nil {
			return IndexCatalog{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowCatalog := IndexCatalog{
				LayoutVersion: int(columns.layoutVersion.Value(i)),
				TenantID:      columns.tenantID.Value(i),
				Version:       columns.version.Value(i),
				UpdatedAt:     parseParquetTime(columns.updatedAt.Value(i)),
			}
			if rows == 0 {
				catalog = rowCatalog
				expectedHash = columns.catalogHash.Value(i)
				indexCount = columns.indexCount.Value(i)
				edgeShardCount = columns.edgeShardCount.Value(i)
				entityPageCount = columns.entityPageCount.Value(i)
			} else if !sameIndexCatalogMetadata(catalog, rowCatalog) ||
				expectedHash != columns.catalogHash.Value(i) ||
				indexCount != columns.indexCount.Value(i) ||
				edgeShardCount != columns.edgeShardCount.Value(i) ||
				entityPageCount != columns.entityPageCount.Value(i) {
				return IndexCatalog{}, fmt.Errorf("index catalog identity mismatch")
			}
			row := indexCatalogRowFromColumns(columns, i)
			if err := applyIndexCatalogRow(&catalog, row); err != nil {
				return IndexCatalog{}, err
			}
			rows++
		}
	}
	if int64(len(catalog.Indexes)) != indexCount ||
		int64(len(catalog.EdgeShards)) != edgeShardCount ||
		int64(len(catalog.EntityPages)) != entityPageCount {
		return IndexCatalog{}, fmt.Errorf("index catalog count mismatch")
	}
	hash, err := indexCatalogContentHash(catalog)
	if err != nil {
		return IndexCatalog{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return IndexCatalog{}, fmt.Errorf("index catalog content hash mismatch")
	}
	return catalog, nil
}

type indexCatalogRow struct {
	Kind           string
	ParentOrdinal  int
	ChildOrdinal   int
	Name           string
	EntityKind     string
	Field          string
	RelationType   string
	Shard          string
	IndexType      string
	Status         string
	Format         string
	Codec          string
	Role           string
	ObjectKey      string
	RowCount       int
	EntryCount     int
	DistinctValues int
	EdgeCount      int
	EntityCount    int
	StatValue      string
	StatCount      int
	ContentHash    string
	SchemaHash     string
	UpdatedAt      time.Time
}

type parquetIndexCatalogColumnSet struct {
	layoutVersion     *array.Int64
	tenantID          *array.String
	version           *array.Int64
	updatedAt         *array.String
	catalogHash       *array.String
	indexCount        *array.Int64
	edgeShardCount    *array.Int64
	entityPageCount   *array.Int64
	rowKind           *array.String
	parentOrdinal     *array.Int64
	childOrdinal      *array.Int64
	name              *array.String
	kind              *array.String
	field             *array.String
	relationType      *array.String
	shard             *array.String
	indexType         *array.String
	status            *array.String
	format            *array.String
	codec             *array.String
	role              *array.String
	objectKey         *array.String
	rowCount          *array.Int64
	entryCount        *array.Int64
	distinctValues    *array.Int64
	edgeCount         *array.Int64
	entityCount       *array.Int64
	statValue         *array.String
	statCount         *array.Int64
	objectContentHash *array.String
	schemaHash        *array.String
	specUpdatedAt     *array.String
}

func indexCatalogRows(catalog IndexCatalog) []indexCatalogRow {
	rows := []indexCatalogRow{}
	for i, index := range catalog.Indexes {
		rows = append(rows, indexCatalogRow{
			Kind:           indexCatalogRowIndex,
			ParentOrdinal:  i,
			Name:           index.Name,
			EntityKind:     index.Kind,
			Field:          index.Field,
			IndexType:      index.Type,
			Status:         index.Status,
			Format:         index.Format,
			Codec:          index.Codec,
			RowCount:       index.RowCount,
			EntryCount:     index.EntryCount,
			DistinctValues: index.DistinctValues,
			ContentHash:    index.ContentHash,
			SchemaHash:     index.SchemaHash,
			UpdatedAt:      index.UpdatedAt,
		})
		for j, object := range index.Objects {
			rows = append(rows, indexCatalogRowFromObject(indexCatalogRowIndexObject, i, j, object))
		}
		for j, value := range index.TopValues {
			rows = append(rows, indexCatalogRow{
				Kind:          indexCatalogRowIndexTopValue,
				ParentOrdinal: i,
				ChildOrdinal:  j,
				StatValue:     value.Value,
				StatCount:     value.Count,
			})
		}
	}
	for i, shard := range catalog.EdgeShards {
		rows = append(rows, indexCatalogRow{
			Kind:          indexCatalogRowEdgeShard,
			ParentOrdinal: i,
			RelationType:  shard.RelationType,
			Shard:         shard.Shard,
			Format:        shard.Format,
			Codec:         shard.Codec,
			RowCount:      shard.RowCount,
			EdgeCount:     shard.EdgeCount,
			ContentHash:   shard.ContentHash,
			SchemaHash:    shard.SchemaHash,
			UpdatedAt:     shard.UpdatedAt,
		})
		for j, object := range shard.Objects {
			rows = append(rows, indexCatalogRowFromObject(indexCatalogRowEdgeShardObject, i, j, object))
		}
	}
	for i, page := range catalog.EntityPages {
		rows = append(rows, indexCatalogRow{
			Kind:          indexCatalogRowEntityPage,
			ParentOrdinal: i,
			Shard:         page.Shard,
			Format:        page.Format,
			Codec:         page.Codec,
			RowCount:      page.RowCount,
			EntityCount:   page.EntityCount,
			ContentHash:   page.ContentHash,
			SchemaHash:    page.SchemaHash,
			UpdatedAt:     page.UpdatedAt,
		})
		for j, object := range page.Objects {
			rows = append(rows, indexCatalogRowFromObject(indexCatalogRowEntityPageObject, i, j, object))
		}
	}
	return rows
}

func indexCatalogRowFromObject(kind string, parent int, child int, object IndexObject) indexCatalogRow {
	return indexCatalogRow{
		Kind:          kind,
		ParentOrdinal: parent,
		ChildOrdinal:  child,
		Role:          object.Role,
		ObjectKey:     object.Key,
		Format:        object.Format,
		Codec:         object.Codec,
		RowCount:      object.RowCount,
		ContentHash:   object.ContentHash,
		SchemaHash:    object.SchemaHash,
	}
}

func applyIndexCatalogRow(catalog *IndexCatalog, row indexCatalogRow) error {
	switch row.Kind {
	case indexCatalogRowMetadata:
		return nil
	case indexCatalogRowIndex:
		if len(catalog.Indexes) != row.ParentOrdinal {
			return fmt.Errorf("index catalog index ordinal mismatch")
		}
		catalog.Indexes = append(catalog.Indexes, IndexSpec{
			Name:           row.Name,
			Kind:           row.EntityKind,
			Field:          row.Field,
			Type:           row.IndexType,
			Status:         row.Status,
			Format:         row.Format,
			Codec:          row.Codec,
			RowCount:       row.RowCount,
			EntryCount:     row.EntryCount,
			DistinctValues: row.DistinctValues,
			ContentHash:    row.ContentHash,
			SchemaHash:     row.SchemaHash,
			UpdatedAt:      row.UpdatedAt,
		})
	case indexCatalogRowIndexObject:
		if row.ParentOrdinal >= len(catalog.Indexes) || len(catalog.Indexes[row.ParentOrdinal].Objects) != row.ChildOrdinal {
			return fmt.Errorf("index catalog index object ordinal mismatch")
		}
		catalog.Indexes[row.ParentOrdinal].Objects = append(catalog.Indexes[row.ParentOrdinal].Objects, row.indexObject())
	case indexCatalogRowIndexTopValue:
		if row.ParentOrdinal >= len(catalog.Indexes) || len(catalog.Indexes[row.ParentOrdinal].TopValues) != row.ChildOrdinal {
			return fmt.Errorf("index catalog top value ordinal mismatch")
		}
		catalog.Indexes[row.ParentOrdinal].TopValues = append(catalog.Indexes[row.ParentOrdinal].TopValues, IndexValueStat{Value: row.StatValue, Count: row.StatCount})
	case indexCatalogRowEdgeShard:
		if len(catalog.EdgeShards) != row.ParentOrdinal {
			return fmt.Errorf("index catalog edge shard ordinal mismatch")
		}
		catalog.EdgeShards = append(catalog.EdgeShards, EdgeShard{
			RelationType: row.RelationType,
			Shard:        row.Shard,
			Format:       row.Format,
			Codec:        row.Codec,
			RowCount:     row.RowCount,
			EdgeCount:    row.EdgeCount,
			ContentHash:  row.ContentHash,
			SchemaHash:   row.SchemaHash,
			UpdatedAt:    row.UpdatedAt,
		})
	case indexCatalogRowEdgeShardObject:
		if row.ParentOrdinal >= len(catalog.EdgeShards) || len(catalog.EdgeShards[row.ParentOrdinal].Objects) != row.ChildOrdinal {
			return fmt.Errorf("index catalog edge shard object ordinal mismatch")
		}
		catalog.EdgeShards[row.ParentOrdinal].Objects = append(catalog.EdgeShards[row.ParentOrdinal].Objects, row.indexObject())
	case indexCatalogRowEntityPage:
		if len(catalog.EntityPages) != row.ParentOrdinal {
			return fmt.Errorf("index catalog entity page ordinal mismatch")
		}
		catalog.EntityPages = append(catalog.EntityPages, EntityPageSpec{
			Shard:       row.Shard,
			Format:      row.Format,
			Codec:       row.Codec,
			RowCount:    row.RowCount,
			EntityCount: row.EntityCount,
			ContentHash: row.ContentHash,
			SchemaHash:  row.SchemaHash,
			UpdatedAt:   row.UpdatedAt,
		})
	case indexCatalogRowEntityPageObject:
		if row.ParentOrdinal >= len(catalog.EntityPages) || len(catalog.EntityPages[row.ParentOrdinal].Objects) != row.ChildOrdinal {
			return fmt.Errorf("index catalog entity page object ordinal mismatch")
		}
		catalog.EntityPages[row.ParentOrdinal].Objects = append(catalog.EntityPages[row.ParentOrdinal].Objects, row.indexObject())
	default:
		return fmt.Errorf("unknown index catalog row kind %q", row.Kind)
	}
	return nil
}

func (row indexCatalogRow) indexObject() IndexObject {
	return IndexObject{
		Role:        row.Role,
		Key:         row.ObjectKey,
		Format:      row.Format,
		Codec:       row.Codec,
		RowCount:    row.RowCount,
		ContentHash: row.ContentHash,
		SchemaHash:  row.SchemaHash,
	}
}

func parquetIndexCatalogColumns(batch arrow.RecordBatch) (parquetIndexCatalogColumnSet, error) {
	var columns parquetIndexCatalogColumnSet
	var err error
	if columns.layoutVersion, err = parquetInt64Column(batch, parquetIndexCatalogColumnLayoutVersion, "layout_version"); err != nil {
		return columns, err
	}
	if columns.tenantID, err = parquetStringColumn(batch, parquetIndexCatalogColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.version, err = parquetInt64Column(batch, parquetIndexCatalogColumnVersion, "version"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetStringColumn(batch, parquetIndexCatalogColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	if columns.catalogHash, err = parquetStringColumn(batch, parquetIndexCatalogColumnCatalogHash, "catalog_hash"); err != nil {
		return columns, err
	}
	if columns.indexCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnIndexCount, "index_count"); err != nil {
		return columns, err
	}
	if columns.edgeShardCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnEdgeShardCount, "edge_shard_count"); err != nil {
		return columns, err
	}
	if columns.entityPageCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnEntityPageCount, "entity_page_count"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetStringColumn(batch, parquetIndexCatalogColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.parentOrdinal, err = parquetInt64Column(batch, parquetIndexCatalogColumnParentOrdinal, "parent_ordinal"); err != nil {
		return columns, err
	}
	if columns.childOrdinal, err = parquetInt64Column(batch, parquetIndexCatalogColumnChildOrdinal, "child_ordinal"); err != nil {
		return columns, err
	}
	if columns.name, err = parquetStringColumn(batch, parquetIndexCatalogColumnName, "name"); err != nil {
		return columns, err
	}
	if columns.kind, err = parquetStringColumn(batch, parquetIndexCatalogColumnKind, "kind"); err != nil {
		return columns, err
	}
	if columns.field, err = parquetStringColumn(batch, parquetIndexCatalogColumnField, "field"); err != nil {
		return columns, err
	}
	if columns.relationType, err = parquetStringColumn(batch, parquetIndexCatalogColumnRelationType, "relation_type"); err != nil {
		return columns, err
	}
	if columns.shard, err = parquetStringColumn(batch, parquetIndexCatalogColumnShard, "shard"); err != nil {
		return columns, err
	}
	if columns.indexType, err = parquetStringColumn(batch, parquetIndexCatalogColumnIndexType, "index_type"); err != nil {
		return columns, err
	}
	if columns.status, err = parquetStringColumn(batch, parquetIndexCatalogColumnStatus, "status"); err != nil {
		return columns, err
	}
	if columns.format, err = parquetStringColumn(batch, parquetIndexCatalogColumnFormat, "format"); err != nil {
		return columns, err
	}
	if columns.codec, err = parquetStringColumn(batch, parquetIndexCatalogColumnCodec, "codec"); err != nil {
		return columns, err
	}
	if columns.role, err = parquetStringColumn(batch, parquetIndexCatalogColumnRole, "role"); err != nil {
		return columns, err
	}
	if columns.objectKey, err = parquetStringColumn(batch, parquetIndexCatalogColumnObjectKey, "object_key"); err != nil {
		return columns, err
	}
	if columns.rowCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnRowCount, "row_count"); err != nil {
		return columns, err
	}
	if columns.entryCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnEntryCount, "entry_count"); err != nil {
		return columns, err
	}
	if columns.distinctValues, err = parquetInt64Column(batch, parquetIndexCatalogColumnDistinctValues, "distinct_values"); err != nil {
		return columns, err
	}
	if columns.edgeCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnEdgeCount, "edge_count"); err != nil {
		return columns, err
	}
	if columns.entityCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnEntityCount, "entity_count"); err != nil {
		return columns, err
	}
	if columns.statValue, err = parquetStringColumn(batch, parquetIndexCatalogColumnStatValue, "stat_value"); err != nil {
		return columns, err
	}
	if columns.statCount, err = parquetInt64Column(batch, parquetIndexCatalogColumnStatCount, "stat_count"); err != nil {
		return columns, err
	}
	if columns.objectContentHash, err = parquetStringColumn(batch, parquetIndexCatalogColumnObjectContentHash, "object_content_hash"); err != nil {
		return columns, err
	}
	if columns.schemaHash, err = parquetStringColumn(batch, parquetIndexCatalogColumnSchemaHash, "schema_hash"); err != nil {
		return columns, err
	}
	if columns.specUpdatedAt, err = parquetStringColumn(batch, parquetIndexCatalogColumnSpecUpdatedAt, "spec_updated_at"); err != nil {
		return columns, err
	}
	return columns, nil
}

func indexCatalogRowFromColumns(columns parquetIndexCatalogColumnSet, row int) indexCatalogRow {
	return indexCatalogRow{
		Kind:           columns.rowKind.Value(row),
		ParentOrdinal:  int(columns.parentOrdinal.Value(row)),
		ChildOrdinal:   int(columns.childOrdinal.Value(row)),
		Name:           columns.name.Value(row),
		EntityKind:     columns.kind.Value(row),
		Field:          columns.field.Value(row),
		RelationType:   columns.relationType.Value(row),
		Shard:          columns.shard.Value(row),
		IndexType:      columns.indexType.Value(row),
		Status:         columns.status.Value(row),
		Format:         columns.format.Value(row),
		Codec:          columns.codec.Value(row),
		Role:           columns.role.Value(row),
		ObjectKey:      columns.objectKey.Value(row),
		RowCount:       int(columns.rowCount.Value(row)),
		EntryCount:     int(columns.entryCount.Value(row)),
		DistinctValues: int(columns.distinctValues.Value(row)),
		EdgeCount:      int(columns.edgeCount.Value(row)),
		EntityCount:    int(columns.entityCount.Value(row)),
		StatValue:      columns.statValue.Value(row),
		StatCount:      int(columns.statCount.Value(row)),
		ContentHash:    columns.objectContentHash.Value(row),
		SchemaHash:     columns.schemaHash.Value(row),
		UpdatedAt:      parseParquetTime(columns.specUpdatedAt.Value(row)),
	}
}

func parquetIndexCatalogArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "layout_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "catalog_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "index_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "edge_shard_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_page_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "parent_ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "child_ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "field", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "relation_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "shard", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "index_type", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "format", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "codec", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "role", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "object_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "row_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entry_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "distinct_values", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "edge_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "entity_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "stat_value", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "stat_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "object_content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "schema_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "spec_updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func parquetIndexCatalogSchemaHash() string {
	return objectSchemaHash([]string{
		"layout_version",
		"tenant_id",
		"version",
		"updated_at",
		"catalog_hash",
		"index_count",
		"edge_shard_count",
		"entity_page_count",
		"row_kind",
		"parent_ordinal",
		"child_ordinal",
		"name",
		"kind",
		"field",
		"relation_type",
		"shard",
		"index_type",
		"status",
		"format",
		"codec",
		"role",
		"object_key",
		"row_count",
		"entry_count",
		"distinct_values",
		"edge_count",
		"entity_count",
		"stat_value",
		"stat_count",
		"object_content_hash",
		"schema_hash",
		"spec_updated_at",
		indexCatalogCodecParquet,
	})
}

func normalizeIndexCatalogForParquet(catalog IndexCatalog) IndexCatalog {
	catalog.LayoutVersion = CurrentObjectLayoutVersion
	catalog.Indexes = append([]IndexSpec(nil), catalog.Indexes...)
	catalog.EdgeShards = append([]EdgeShard(nil), catalog.EdgeShards...)
	catalog.EntityPages = append([]EntityPageSpec(nil), catalog.EntityPages...)
	return catalog
}

func sameIndexCatalogMetadata(left IndexCatalog, right IndexCatalog) bool {
	return left.LayoutVersion == right.LayoutVersion &&
		left.TenantID == right.TenantID &&
		left.Version == right.Version &&
		formatParquetTime(left.UpdatedAt) == formatParquetTime(right.UpdatedAt)
}

func indexCatalogContentHash(catalog IndexCatalog) (string, error) {
	catalog = normalizeIndexCatalogForParquet(catalog)
	parts := []string{
		formatInt64ForHash(int64(catalog.LayoutVersion)),
		catalog.TenantID,
		formatInt64ForHash(catalog.Version),
		formatInt64ForHash(int64(len(catalog.Indexes))),
		formatInt64ForHash(int64(len(catalog.EdgeShards))),
		formatInt64ForHash(int64(len(catalog.EntityPages))),
	}
	for _, row := range indexCatalogRows(catalog) {
		parts = append(parts, indexCatalogRowHashParts(row)...)
	}
	return parquetScalarContentHash(parts...), nil
}

func indexCatalogRowHashParts(row indexCatalogRow) []string {
	return []string{
		row.Kind,
		formatInt64ForHash(int64(row.ParentOrdinal)),
		formatInt64ForHash(int64(row.ChildOrdinal)),
		row.Name,
		row.EntityKind,
		row.Field,
		row.RelationType,
		row.Shard,
		row.IndexType,
		row.Status,
		row.Format,
		row.Codec,
		row.Role,
		row.ObjectKey,
		formatInt64ForHash(int64(row.RowCount)),
		formatInt64ForHash(int64(row.EntryCount)),
		formatInt64ForHash(int64(row.DistinctValues)),
		formatInt64ForHash(int64(row.EdgeCount)),
		formatInt64ForHash(int64(row.EntityCount)),
		row.StatValue,
		formatInt64ForHash(int64(row.StatCount)),
		row.ContentHash,
		row.SchemaHash,
	}
}
