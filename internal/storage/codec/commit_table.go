package codec

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func MarshalCommitItems(ctx context.Context, tenantID string, items []CommitTableItem) ([]byte, error) {
	normalized := make([]CommitTableItem, 0, len(items))
	for _, item := range items {
		commit, hash, err := NormalizeCommitForParquet(item.Commit)
		if err != nil {
			return nil, err
		}
		item.Commit = commit
		if item.ContentHash == "" {
			item.ContentHash = hash
		}
		normalized = append(normalized, item)
	}
	return MarshalNormalizedCommitItems(ctx, tenantID, normalized)
}

func MarshalNormalizedCommitItems(
	ctx context.Context,
	tenantID string,
	items []CommitTableItem,
) ([]byte, error) {
	schema := ParquetCommitArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	for _, item := range items {
		rows, err := CommitRows(item.Commit)
		if err != nil {
			return nil, err
		}
		rowTenant := tenantID
		if rowTenant == "" {
			rowTenant = item.Commit.TenantID
		}
		for _, row := range rows {
			AppendParquetCommitRow(
				builder,
				rowTenant,
				item.Key,
				item.Commit,
				item.ContentHash,
				row,
			)
		}
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
	writerProps := parquetio.DataWriterProperties(schema)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, rowGroupSize, writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), ctx.Err()
}

func DecodeCommitItems(ctx context.Context, data []byte) ([]CommitTableItem, error) {
	table, release, err := parquetio.ReadTable(ctx, data)
	if err != nil {
		return nil, err
	}
	defer release()
	defer table.Release()
	if table.NumRows() < 1 {
		return nil, fmt.Errorf("parquet commit table is empty")
	}
	if table.NumCols() < int64(ParquetCommitColumnEdgeSourceObservedAt+1) {
		return nil, fmt.Errorf("parquet commit table has %d columns, want at least %d", table.NumCols(), ParquetCommitColumnEdgeSourceObservedAt+1)
	}

	builders := map[string]*CommitBuilder{}
	entityOrdinals := table.NumRows()
	order := []string{}
	reader := array.NewTableReader(table, 4096)
	defer reader.Release()
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := ParquetCommitColumns(batch)
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			if err := columns.ValidateOrdinals(i, table.NumRows(), true); err != nil {
				return nil, err
			}
			key := commitBuildKey(columns.segmentKey.Value(i), columns.commitID.Value(i), columns.version.Value(i))
			build := builders[key]
			if build == nil {
				build = &CommitBuilder{
					entityOrdinals: &entityOrdinals,
					key:            strings.Clone(columns.segmentKey.Value(i)),
					contentHash:    strings.Clone(columns.contentHash.Value(i)),
					commit: graph.Commit{
						LayoutVersion: CurrentObjectLayoutVersion,
						TenantID:      strings.Clone(columns.commitTenantID.Value(i)),
						ID:            strings.Clone(columns.commitID.Value(i)),
						Version:       columns.version.Value(i),
						CreatedAt:     parquetio.ParseTime(columns.createdAt.Value(i)),
					},
				}
				builders[key] = build
				order = append(order, key)
			}
			if build.contentHash != columns.contentHash.Value(i) ||
				build.commit.TenantID != columns.commitTenantID.Value(i) ||
				build.commit.ID != columns.commitID.Value(i) ||
				build.commit.Version != columns.version.Value(i) {
				return nil, fmt.Errorf("parquet commit row identity mismatch")
			}
			if err := build.Apply(ParquetCommitRowFromColumns(columns, i)); err != nil {
				return nil, err
			}
		}
	}

	items := make([]CommitTableItem, 0, len(order))
	for _, key := range order {
		item, err := builders[key].finish()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func NormalizeCommitForParquet(commit graph.Commit) (graph.Commit, string, error) {
	payload, err := CommitPayloadJSON(commit)
	if err != nil {
		return graph.Commit{}, "", err
	}
	var normalized graph.Commit
	if err := json.Unmarshal(payload, &normalized); err != nil {
		return graph.Commit{}, "", err
	}
	return normalized, ContentHash(payload), nil
}
