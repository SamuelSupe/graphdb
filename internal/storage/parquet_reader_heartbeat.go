package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const readerHeartbeatCodecParquet = "reader-heartbeat-arrow-parquet-v1"

const (
	parquetReaderHeartbeatColumnTenantID = iota
	parquetReaderHeartbeatColumnReaderID
	parquetReaderHeartbeatColumnVisibleVersion
	parquetReaderHeartbeatColumnLastSeenAt
	parquetReaderHeartbeatColumnContentHash
	parquetReaderHeartbeatColumnInstanceID
	parquetReaderHeartbeatColumnMode
	parquetReaderHeartbeatColumnStatus
	parquetReaderHeartbeatColumnFresh
	parquetReaderHeartbeatColumnConsistent
	parquetReaderHeartbeatColumnManifestVersion
	parquetReaderHeartbeatColumnVersionLag
	parquetReaderHeartbeatColumnLagMS
	parquetReaderHeartbeatColumnSnapshotVersion
)

func marshalParquetReaderHeartbeat(ctx context.Context, heartbeat ReaderHeartbeat) ([]byte, error) {
	schema := parquetReaderHeartbeatArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()

	builder.Field(parquetReaderHeartbeatColumnTenantID).(*array.StringBuilder).Append(heartbeat.TenantID)
	builder.Field(parquetReaderHeartbeatColumnReaderID).(*array.StringBuilder).Append(heartbeat.ReaderID)
	builder.Field(parquetReaderHeartbeatColumnVisibleVersion).(*array.Int64Builder).Append(heartbeat.VisibleVersion)
	builder.Field(parquetReaderHeartbeatColumnLastSeenAt).(*array.StringBuilder).Append(formatParquetTime(heartbeat.LastSeenAt))
	builder.Field(parquetReaderHeartbeatColumnContentHash).(*array.StringBuilder).Append(readerHeartbeatContentHash(heartbeat))
	builder.Field(parquetReaderHeartbeatColumnInstanceID).(*array.StringBuilder).Append(heartbeat.InstanceID)
	builder.Field(parquetReaderHeartbeatColumnMode).(*array.StringBuilder).Append(heartbeat.Mode)
	builder.Field(parquetReaderHeartbeatColumnStatus).(*array.StringBuilder).Append(heartbeat.Status)
	builder.Field(parquetReaderHeartbeatColumnFresh).(*array.BooleanBuilder).Append(heartbeat.Fresh)
	builder.Field(parquetReaderHeartbeatColumnConsistent).(*array.BooleanBuilder).Append(heartbeat.Consistent)
	builder.Field(parquetReaderHeartbeatColumnManifestVersion).(*array.Int64Builder).Append(heartbeat.ManifestVersion)
	builder.Field(parquetReaderHeartbeatColumnVersionLag).(*array.Int64Builder).Append(heartbeat.VersionLag)
	builder.Field(parquetReaderHeartbeatColumnLagMS).(*array.Int64Builder).Append(heartbeat.LagMS)
	builder.Field(parquetReaderHeartbeatColumnSnapshotVersion).(*array.Int64Builder).Append(heartbeat.SnapshotVersion)

	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema(), pqarrow.WithAllocator(memory.DefaultAllocator))
	if err := pqarrow.WriteTable(table, &buf, parquetMetadataRowGroupRows(table.NumRows()), writerProps, arrowProps); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetReaderHeartbeat(ctx context.Context, data []byte) (ReaderHeartbeat, error) {
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	defer table.Release()
	if table.NumRows() != 1 {
		return ReaderHeartbeat{}, fmt.Errorf("parquet reader heartbeat has %d rows, want 1", table.NumRows())
	}
	if table.NumCols() < int64(parquetReaderHeartbeatColumnLagMS+1) {
		return ReaderHeartbeat{}, fmt.Errorf("parquet reader heartbeat has %d columns, want at least %d", table.NumCols(), parquetReaderHeartbeatColumnLagMS+1)
	}

	reader := array.NewTableReader(table, 1)
	defer reader.Release()
	if !reader.Next() {
		return ReaderHeartbeat{}, fmt.Errorf("parquet reader heartbeat is empty")
	}
	batch := reader.RecordBatch()
	tenantColumn, err := parquetStringColumn(batch, parquetReaderHeartbeatColumnTenantID, "tenant_id")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	readerColumn, err := parquetStringColumn(batch, parquetReaderHeartbeatColumnReaderID, "reader_id")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	versionColumn, err := parquetInt64Column(batch, parquetReaderHeartbeatColumnVisibleVersion, "visible_version")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	lastSeenColumn, err := parquetStringColumn(batch, parquetReaderHeartbeatColumnLastSeenAt, "last_seen_at")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	hashColumn, err := parquetStringColumn(batch, parquetReaderHeartbeatColumnContentHash, "content_hash")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	instanceColumn, err := parquetStringColumn(batch, parquetReaderHeartbeatColumnInstanceID, "instance_id")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	modeColumn, err := parquetStringColumn(batch, parquetReaderHeartbeatColumnMode, "mode")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	statusColumn, err := parquetStringColumn(batch, parquetReaderHeartbeatColumnStatus, "status")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	freshColumn, err := parquetBoolColumn(batch, parquetReaderHeartbeatColumnFresh, "fresh")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	consistentColumn, err := parquetBoolColumn(batch, parquetReaderHeartbeatColumnConsistent, "consistent")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	manifestVersionColumn, err := parquetInt64Column(batch, parquetReaderHeartbeatColumnManifestVersion, "manifest_version")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	versionLagColumn, err := parquetInt64Column(batch, parquetReaderHeartbeatColumnVersionLag, "version_lag")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	lagMSColumn, err := parquetInt64Column(batch, parquetReaderHeartbeatColumnLagMS, "lag_ms")
	if err != nil {
		return ReaderHeartbeat{}, err
	}
	var snapshotVersion int64
	hasSnapshotVersion := table.NumCols() > int64(parquetReaderHeartbeatColumnSnapshotVersion)
	if hasSnapshotVersion {
		snapshotVersionColumn, err := parquetInt64Column(batch, parquetReaderHeartbeatColumnSnapshotVersion, "snapshot_version")
		if err != nil {
			return ReaderHeartbeat{}, err
		}
		snapshotVersion = snapshotVersionColumn.Value(0)
	}
	heartbeat := ReaderHeartbeat{
		TenantID:        tenantColumn.Value(0),
		ReaderID:        readerColumn.Value(0),
		VisibleVersion:  versionColumn.Value(0),
		LastSeenAt:      parseParquetTime(lastSeenColumn.Value(0)),
		InstanceID:      instanceColumn.Value(0),
		Mode:            modeColumn.Value(0),
		Status:          statusColumn.Value(0),
		Fresh:           freshColumn.Value(0),
		Consistent:      consistentColumn.Value(0),
		ManifestVersion: manifestVersionColumn.Value(0),
		SnapshotVersion: snapshotVersion,
		VersionLag:      versionLagColumn.Value(0),
		LagMS:           lagMSColumn.Value(0),
	}
	if heartbeat.TenantID != tenantColumn.Value(0) || heartbeat.ReaderID != readerColumn.Value(0) || heartbeat.VisibleVersion != versionColumn.Value(0) {
		return ReaderHeartbeat{}, fmt.Errorf("reader heartbeat identity mismatch")
	}
	hash := readerHeartbeatContentHash(heartbeat)
	if hashColumn.Value(0) == "" || hashColumn.Value(0) != hash {
		if hasSnapshotVersion || hashColumn.Value(0) != legacyReaderHeartbeatContentHash(heartbeat) {
			return ReaderHeartbeat{}, fmt.Errorf("reader heartbeat content hash mismatch")
		}
	}
	return heartbeat, nil
}

func parquetReaderHeartbeatArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "tenant_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "reader_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "visible_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "last_seen_at", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content_hash", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "instance_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "mode", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "fresh", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "consistent", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		{Name: "manifest_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "version_lag", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "lag_ms", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "snapshot_version", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)
}

func readerHeartbeatContentHash(heartbeat ReaderHeartbeat) string {
	return parquetScalarContentHash(
		heartbeat.TenantID,
		heartbeat.ReaderID,
		formatInt64ForHash(heartbeat.VisibleVersion),
		formatParquetTime(heartbeat.LastSeenAt),
		heartbeat.InstanceID,
		heartbeat.Mode,
		heartbeat.Status,
		formatBoolForHash(heartbeat.Fresh),
		formatBoolForHash(heartbeat.Consistent),
		formatInt64ForHash(heartbeat.ManifestVersion),
		formatInt64ForHash(heartbeat.VersionLag),
		formatInt64ForHash(heartbeat.LagMS),
		formatInt64ForHash(heartbeat.SnapshotVersion),
	)
}

func legacyReaderHeartbeatContentHash(heartbeat ReaderHeartbeat) string {
	return parquetScalarContentHash(
		heartbeat.TenantID,
		heartbeat.ReaderID,
		formatInt64ForHash(heartbeat.VisibleVersion),
		formatParquetTime(heartbeat.LastSeenAt),
		heartbeat.InstanceID,
		heartbeat.Mode,
		heartbeat.Status,
		formatBoolForHash(heartbeat.Fresh),
		formatBoolForHash(heartbeat.Consistent),
		formatInt64ForHash(heartbeat.ManifestVersion),
		formatInt64ForHash(heartbeat.VersionLag),
		formatInt64ForHash(heartbeat.LagMS),
	)
}
