package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/parquetio"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const manifestCodecParquet = "manifest-arrow-parquet-v2"

const (
	parquetManifestColumnLayoutVersion = iota
	parquetManifestColumnTenantID
	parquetManifestColumnVersion
	parquetManifestColumnHeadCommitID
	parquetManifestColumnSnapshotKey
	parquetManifestColumnSnapshotCatalogKey
	parquetManifestColumnSnapshotVersion
	parquetManifestColumnUpdatedAt
	parquetManifestColumnContentHash
	parquetManifestColumnCommitSegmentCount
	parquetManifestColumnCommitKeyCount
	parquetManifestColumnRowKind
	parquetManifestColumnOrdinal
	parquetManifestColumnCommitKey
	parquetManifestColumnSegmentKey
	parquetManifestColumnSegmentCodec
	parquetManifestColumnSegmentFirstVersion
	parquetManifestColumnSegmentLastVersion
	parquetManifestColumnSegmentCount
	parquetManifestColumnSegmentContentHash
	parquetManifestColumnWriterFence
	parquetManifestColumnWriterFenceEpoch
	parquetManifestColumnDataHash
)

const (
	manifestRowKindMetadata      = "metadata"
	manifestRowKindCommitSegment = "commit_segment"
	manifestRowKindCommitKey     = "commit_key"
)

func marshalParquetManifest(ctx context.Context, manifest Manifest) ([]byte, error) {
	normalized := normalizeManifestForParquet(manifest)
	hash, err := manifestContentHash(normalized)
	if err != nil {
		return nil, err
	}

	schema := parquetManifestArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	appendRow := func(kind string, ordinal int, commitKey string, segment CommitSegmentRef) {
		builder.Field(parquetManifestColumnLayoutVersion).(*array.Int64Builder).Append(int64(normalized.LayoutVersion))
		builder.Field(parquetManifestColumnTenantID).(*array.StringBuilder).Append(normalized.TenantID)
		builder.Field(parquetManifestColumnVersion).(*array.Int64Builder).Append(normalized.Version)
		builder.Field(parquetManifestColumnHeadCommitID).(*array.StringBuilder).Append(normalized.HeadCommitID)
		builder.Field(parquetManifestColumnSnapshotKey).(*array.StringBuilder).Append(normalized.SnapshotKey)
		builder.Field(parquetManifestColumnSnapshotCatalogKey).(*array.StringBuilder).Append(normalized.SnapshotCatalogKey)
		builder.Field(parquetManifestColumnSnapshotVersion).(*array.Int64Builder).Append(normalized.SnapshotVersion)
		builder.Field(parquetManifestColumnUpdatedAt).(*array.StringBuilder).Append(parquetio.FormatTime(normalized.UpdatedAt))
		builder.Field(parquetManifestColumnContentHash).(*array.StringBuilder).Append(hash)
		builder.Field(parquetManifestColumnCommitSegmentCount).(*array.Int64Builder).Append(int64(len(normalized.CommitSegments)))
		builder.Field(parquetManifestColumnCommitKeyCount).(*array.Int64Builder).Append(int64(len(normalized.CommitKeys)))
		builder.Field(parquetManifestColumnRowKind).(*array.StringBuilder).Append(kind)
		builder.Field(parquetManifestColumnOrdinal).(*array.Int64Builder).Append(int64(ordinal))
		builder.Field(parquetManifestColumnCommitKey).(*array.StringBuilder).Append(commitKey)
		builder.Field(parquetManifestColumnSegmentKey).(*array.StringBuilder).Append(segment.Key)
		builder.Field(parquetManifestColumnSegmentCodec).(*array.StringBuilder).Append(segment.Codec)
		builder.Field(parquetManifestColumnSegmentFirstVersion).(*array.Int64Builder).Append(segment.FirstVersion)
		builder.Field(parquetManifestColumnSegmentLastVersion).(*array.Int64Builder).Append(segment.LastVersion)
		builder.Field(parquetManifestColumnSegmentCount).(*array.Int64Builder).Append(int64(segment.Count))
		builder.Field(parquetManifestColumnSegmentContentHash).(*array.StringBuilder).Append(segment.ContentHash)
		builder.Field(parquetManifestColumnWriterFence).(*array.StringBuilder).Append(normalized.WriterFence)
		builder.Field(parquetManifestColumnWriterFenceEpoch).(*array.Int64Builder).Append(normalized.WriterFenceEpoch)
		builder.Field(parquetManifestColumnDataHash).(*array.StringBuilder).Append(normalized.DataHash)
	}

	if len(normalized.CommitSegments) == 0 && len(normalized.CommitKeys) == 0 {
		appendRow(manifestRowKindMetadata, 0, "", CommitSegmentRef{})
	} else {
		for i, segment := range normalized.CommitSegments {
			appendRow(manifestRowKindCommitSegment, i, "", segment)
		}
		for i, key := range normalized.CommitKeys {
			appendRow(manifestRowKindCommitKey, i, key, CommitSegmentRef{})
		}
	}

	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, parquetio.MetadataRowGroupRows(table.NumRows()), writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetManifest(ctx context.Context, data []byte) (Manifest, error) {
	table, release, err := parquetio.ReadControlTable(ctx, data)
	if err != nil {
		return Manifest{}, err
	}
	defer release()
	defer table.Release()
	if table.NumCols() <= int64(parquetManifestColumnDataHash) || table.Schema().Field(parquetManifestColumnDataHash).Name != "data_hash" {
		return Manifest{}, fmt.Errorf("unsupported pre-2.0 manifest: use a new data directory for GGraphDB 2.0")
	}
	if table.NumRows() < 1 {
		return Manifest{}, fmt.Errorf("parquet manifest is empty")
	}
	if table.NumCols() < int64(parquetManifestColumnSegmentContentHash+1) {
		return Manifest{}, fmt.Errorf("parquet manifest has %d columns, want at least %d", table.NumCols(), parquetManifestColumnSegmentContentHash+1)
	}

	reader := array.NewTableReader(table, 1024)
	defer reader.Release()

	var manifest Manifest
	var expectedHash string
	var segmentCount int64
	var commitKeyCount int64
	rows := 0
	for reader.Next() {
		batch := reader.RecordBatch()
		columns, err := parquetManifestColumns(batch)
		if err != nil {
			return Manifest{}, err
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			rowManifest := Manifest{
				LayoutVersion:      int(columns.layoutVersion.Value(i)),
				TenantID:           columns.tenantID.Value(i),
				Version:            columns.version.Value(i),
				HeadCommitID:       columns.headCommitID.Value(i),
				SnapshotKey:        columns.snapshotKey.Value(i),
				SnapshotCatalogKey: columns.snapshotCatalogKey.Value(i),
				SnapshotVersion:    columns.snapshotVersion.Value(i),
				UpdatedAt:          parquetio.ParseTime(columns.updatedAt.Value(i)),
			}
			if columns.writerFence != nil {
				rowManifest.WriterFence = columns.writerFence.Value(i)
			}
			if columns.writerFenceEpoch != nil {
				rowManifest.WriterFenceEpoch = columns.writerFenceEpoch.Value(i)
			}
			if columns.dataHash != nil {
				rowManifest.DataHash = columns.dataHash.Value(i)
			}
			if rows == 0 {
				manifest = rowManifest
				expectedHash = columns.contentHash.Value(i)
				segmentCount = columns.commitSegmentCount.Value(i)
				commitKeyCount = columns.commitKeyCount.Value(i)
			} else if !sameManifestMetadata(manifest, rowManifest) ||
				expectedHash != columns.contentHash.Value(i) ||
				segmentCount != columns.commitSegmentCount.Value(i) ||
				commitKeyCount != columns.commitKeyCount.Value(i) {
				return Manifest{}, fmt.Errorf("manifest identity mismatch")
			}
			switch kind := columns.rowKind.Value(i); kind {
			case manifestRowKindMetadata:
				if segmentCount != 0 || commitKeyCount != 0 {
					return Manifest{}, fmt.Errorf("manifest metadata row cannot contain tail entries")
				}
			case manifestRowKindCommitSegment:
				if int64(len(manifest.CommitSegments)) != columns.ordinal.Value(i) {
					return Manifest{}, fmt.Errorf("manifest commit segment ordinal mismatch")
				}
				manifest.CommitSegments = append(manifest.CommitSegments, CommitSegmentRef{
					Key:          columns.segmentKey.Value(i),
					Codec:        columns.segmentCodec.Value(i),
					FirstVersion: columns.segmentFirstVersion.Value(i),
					LastVersion:  columns.segmentLastVersion.Value(i),
					Count:        int(columns.segmentCount.Value(i)),
					ContentHash:  columns.segmentContentHash.Value(i),
				})
			case manifestRowKindCommitKey:
				if int64(len(manifest.CommitKeys)) != columns.ordinal.Value(i) {
					return Manifest{}, fmt.Errorf("manifest commit key ordinal mismatch")
				}
				manifest.CommitKeys = append(manifest.CommitKeys, columns.commitKey.Value(i))
			default:
				return Manifest{}, fmt.Errorf("unknown manifest row kind %q", kind)
			}
			rows++
		}
	}
	if int64(len(manifest.CommitSegments)) != segmentCount || int64(len(manifest.CommitKeys)) != commitKeyCount {
		return Manifest{}, fmt.Errorf("manifest tail count mismatch")
	}
	hash, err := manifestContentHash(manifest)
	if err != nil {
		return Manifest{}, err
	}
	if expectedHash == "" || expectedHash != hash {
		return Manifest{}, fmt.Errorf("manifest content hash mismatch")
	}
	return manifest, nil
}

type parquetManifestColumnSet struct {
	layoutVersion       *array.Int64
	tenantID            *array.String
	version             *array.Int64
	headCommitID        *array.String
	snapshotKey         *array.String
	snapshotCatalogKey  *array.String
	snapshotVersion     *array.Int64
	updatedAt           *array.String
	contentHash         *array.String
	commitSegmentCount  *array.Int64
	commitKeyCount      *array.Int64
	rowKind             *array.String
	ordinal             *array.Int64
	commitKey           *array.String
	segmentKey          *array.String
	segmentCodec        *array.String
	segmentFirstVersion *array.Int64
	segmentLastVersion  *array.Int64
	segmentCount        *array.Int64
	segmentContentHash  *array.String
	writerFence         *array.String
	writerFenceEpoch    *array.Int64
	dataHash            *array.String
}

func parquetManifestColumns(batch arrow.RecordBatch) (parquetManifestColumnSet, error) {
	var columns parquetManifestColumnSet
	var err error
	if columns.layoutVersion, err = parquetio.Int64Column(batch, parquetManifestColumnLayoutVersion, "layout_version"); err != nil {
		return columns, err
	}
	if columns.tenantID, err = parquetio.StringColumn(batch, parquetManifestColumnTenantID, "tenant_id"); err != nil {
		return columns, err
	}
	if columns.version, err = parquetio.Int64Column(batch, parquetManifestColumnVersion, "version"); err != nil {
		return columns, err
	}
	if columns.headCommitID, err = parquetio.StringColumn(batch, parquetManifestColumnHeadCommitID, "head_commit_id"); err != nil {
		return columns, err
	}
	if columns.snapshotKey, err = parquetio.StringColumn(batch, parquetManifestColumnSnapshotKey, "snapshot_key"); err != nil {
		return columns, err
	}
	if columns.snapshotCatalogKey, err = parquetio.StringColumn(batch, parquetManifestColumnSnapshotCatalogKey, "snapshot_catalog_key"); err != nil {
		return columns, err
	}
	if columns.snapshotVersion, err = parquetio.Int64Column(batch, parquetManifestColumnSnapshotVersion, "snapshot_version"); err != nil {
		return columns, err
	}
	if columns.updatedAt, err = parquetio.StringColumn(batch, parquetManifestColumnUpdatedAt, "updated_at"); err != nil {
		return columns, err
	}
	if columns.contentHash, err = parquetio.StringColumn(batch, parquetManifestColumnContentHash, "content_hash"); err != nil {
		return columns, err
	}
	if columns.commitSegmentCount, err = parquetio.Int64Column(batch, parquetManifestColumnCommitSegmentCount, "commit_segment_count"); err != nil {
		return columns, err
	}
	if columns.commitKeyCount, err = parquetio.Int64Column(batch, parquetManifestColumnCommitKeyCount, "commit_key_count"); err != nil {
		return columns, err
	}
	if columns.rowKind, err = parquetio.StringColumn(batch, parquetManifestColumnRowKind, "row_kind"); err != nil {
		return columns, err
	}
	if columns.ordinal, err = parquetio.Int64Column(batch, parquetManifestColumnOrdinal, "ordinal"); err != nil {
		return columns, err
	}
	if columns.commitKey, err = parquetio.StringColumn(batch, parquetManifestColumnCommitKey, "commit_key"); err != nil {
		return columns, err
	}
	if columns.segmentKey, err = parquetio.StringColumn(batch, parquetManifestColumnSegmentKey, "segment_key"); err != nil {
		return columns, err
	}
	if columns.segmentCodec, err = parquetio.StringColumn(batch, parquetManifestColumnSegmentCodec, "segment_codec"); err != nil {
		return columns, err
	}
	if columns.segmentFirstVersion, err = parquetio.Int64Column(batch, parquetManifestColumnSegmentFirstVersion, "segment_first_version"); err != nil {
		return columns, err
	}
	if columns.segmentLastVersion, err = parquetio.Int64Column(batch, parquetManifestColumnSegmentLastVersion, "segment_last_version"); err != nil {
		return columns, err
	}
	if columns.segmentCount, err = parquetio.Int64Column(batch, parquetManifestColumnSegmentCount, "segment_count"); err != nil {
		return columns, err
	}
	if columns.segmentContentHash, err = parquetio.StringColumn(batch, parquetManifestColumnSegmentContentHash, "segment_content_hash"); err != nil {
		return columns, err
	}
	if batch.NumCols() > int64(parquetManifestColumnWriterFence) {
		if columns.writerFence, err = parquetio.StringColumn(batch, parquetManifestColumnWriterFence, "writer_fence"); err != nil {
			return columns, err
		}
	}
	if batch.NumCols() > int64(parquetManifestColumnWriterFenceEpoch) {
		if columns.writerFenceEpoch, err = parquetio.Int64Column(batch, parquetManifestColumnWriterFenceEpoch, "writer_fence_epoch"); err != nil {
			return columns, err
		}
	}
	if batch.NumCols() > int64(parquetManifestColumnDataHash) {
		if columns.dataHash, err = parquetio.StringColumn(batch, parquetManifestColumnDataHash, "data_hash"); err != nil {
			return columns, err
		}
	}
	return columns, nil
}

func parquetManifestArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "layout_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "head_commit_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "snapshot_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "snapshot_catalog_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "snapshot_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "updated_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "commit_segment_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "commit_key_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "row_kind", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "ordinal", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "commit_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "segment_key", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "segment_codec", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "segment_first_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "segment_last_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "segment_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "segment_content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "writer_fence", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "writer_fence_epoch", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "data_hash", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

func normalizeManifestForParquet(manifest Manifest) Manifest {
	manifest.LayoutVersion = CurrentObjectLayoutVersion
	manifest.CommitSegments = append([]CommitSegmentRef(nil), manifest.CommitSegments...)
	manifest.CommitKeys = append([]string(nil), manifest.CommitKeys...)
	return manifest
}

func sameManifestMetadata(left Manifest, right Manifest) bool {
	return left.LayoutVersion == right.LayoutVersion &&
		left.TenantID == right.TenantID &&
		left.Version == right.Version &&
		left.HeadCommitID == right.HeadCommitID &&
		left.SnapshotKey == right.SnapshotKey &&
		left.SnapshotCatalogKey == right.SnapshotCatalogKey &&
		left.SnapshotVersion == right.SnapshotVersion &&
		left.WriterFence == right.WriterFence &&
		left.WriterFenceEpoch == right.WriterFenceEpoch &&
		left.DataHash == right.DataHash &&
		parquetio.FormatTime(left.UpdatedAt) == parquetio.FormatTime(right.UpdatedAt)
}

func manifestContentHash(manifest Manifest) (string, error) {
	normalized := normalizeManifestForParquet(manifest)
	parts := []string{
		formatInt64ForHash(int64(normalized.LayoutVersion)),
		normalized.TenantID,
		formatInt64ForHash(normalized.Version),
		normalized.HeadCommitID,
		normalized.SnapshotKey,
		normalized.SnapshotCatalogKey,
		formatInt64ForHash(normalized.SnapshotVersion),
		parquetio.FormatTime(normalized.UpdatedAt),
		formatInt64ForHash(int64(len(normalized.CommitSegments))),
		formatInt64ForHash(int64(len(normalized.CommitKeys))),
	}
	if normalized.WriterFence != "" {
		parts = append(parts, normalized.WriterFence)
	}
	if normalized.WriterFenceEpoch > 0 {
		parts = append(parts, formatInt64ForHash(normalized.WriterFenceEpoch))
	}
	if normalized.DataHash != "" {
		parts = append(parts, normalized.DataHash)
	}
	for _, segment := range normalized.CommitSegments {
		parts = append(parts,
			segment.Key,
			segment.Codec,
			formatInt64ForHash(segment.FirstVersion),
			formatInt64ForHash(segment.LastVersion),
			formatInt64ForHash(int64(segment.Count)),
			segment.ContentHash,
		)
	}
	parts = append(parts, normalized.CommitKeys...)
	return parquetScalarContentHash(parts...), nil
}
