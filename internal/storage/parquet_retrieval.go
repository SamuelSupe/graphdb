package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

const (
	retrievalObjectDefinitions = "retrieval_definitions"
	retrievalObjectCatalog     = "retrieval_catalog"
	retrievalObjectHead        = "retrieval_head"
)

const (
	parquetRetrievalColumnLayoutVersion = iota
	parquetRetrievalColumnObjectKind
	parquetRetrievalColumnTenantID
	parquetRetrievalColumnRevision
	parquetRetrievalColumnGraphVersion
	parquetRetrievalColumnContentHash
	parquetRetrievalColumnPayload
)

type parquetRetrievalEnvelope struct {
	LayoutVersion int
	ObjectKind    string
	TenantID      string
	Revision      int64
	GraphVersion  int64
	ContentHash   string
	Payload       string
}

func marshalParquetRetrievalObject(
	ctx context.Context,
	objectKind string,
	tenantID string,
	revision int64,
	graphVersion int64,
	value any,
) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	envelope := parquetRetrievalEnvelope{
		LayoutVersion: RetrievalExtensionLayoutVersion,
		ObjectKind:    objectKind,
		TenantID:      tenantID,
		Revision:      revision,
		GraphVersion:  graphVersion,
		ContentHash:   objectContentHash(payload),
		Payload:       string(payload),
	}
	schema := parquetRetrievalArrowSchema()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer builder.Release()
	builder.Field(parquetRetrievalColumnLayoutVersion).(*array.Int64Builder).
		Append(int64(envelope.LayoutVersion))
	builder.Field(parquetRetrievalColumnObjectKind).(*array.StringBuilder).
		Append(envelope.ObjectKind)
	builder.Field(parquetRetrievalColumnTenantID).(*array.StringBuilder).
		Append(envelope.TenantID)
	builder.Field(parquetRetrievalColumnRevision).(*array.Int64Builder).
		Append(envelope.Revision)
	builder.Field(parquetRetrievalColumnGraphVersion).(*array.Int64Builder).
		Append(envelope.GraphVersion)
	builder.Field(parquetRetrievalColumnContentHash).(*array.StringBuilder).
		Append(envelope.ContentHash)
	builder.Field(parquetRetrievalColumnPayload).(*array.StringBuilder).
		Append(envelope.Payload)

	batch := builder.NewRecordBatch()
	defer batch.Release()
	table := array.NewTableFromRecords(schema, []arrow.RecordBatch{batch})
	defer table.Release()

	var buf bytes.Buffer
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithAllocator(memory.DefaultAllocator),
	)
	if err := pqarrow.WriteTable(
		table,
		&buf,
		1,
		writerProps,
		arrowProps,
	); err != nil {
		return nil, err
	}
	return buf.Bytes(), objectContextErr(ctx)
}

func decodeParquetRetrievalObject(
	ctx context.Context,
	data []byte,
	expectedKind string,
	target any,
) (parquetRetrievalEnvelope, error) {
	table, release, err := readParquetTable(ctx, data)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	defer release()
	defer table.Release()
	if table.NumRows() != 1 {
		return parquetRetrievalEnvelope{}, fmt.Errorf(
			"parquet %s has %d rows, want 1",
			expectedKind,
			table.NumRows(),
		)
	}
	if table.NumCols() < int64(parquetRetrievalColumnPayload+1) {
		return parquetRetrievalEnvelope{}, fmt.Errorf(
			"parquet %s has %d columns, want at least %d",
			expectedKind,
			table.NumCols(),
			parquetRetrievalColumnPayload+1,
		)
	}
	reader := array.NewTableReader(table, 1)
	defer reader.Release()
	if !reader.Next() {
		return parquetRetrievalEnvelope{}, fmt.Errorf(
			"parquet %s is empty",
			expectedKind,
		)
	}
	batch := reader.RecordBatch()
	layoutVersion, err := parquetInt64Column(
		batch,
		parquetRetrievalColumnLayoutVersion,
		"layout_version",
	)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	objectKind, err := parquetStringColumn(
		batch,
		parquetRetrievalColumnObjectKind,
		"object_kind",
	)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	tenantID, err := parquetStringColumn(
		batch,
		parquetRetrievalColumnTenantID,
		"tenant_id",
	)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	revision, err := parquetInt64Column(
		batch,
		parquetRetrievalColumnRevision,
		"revision",
	)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	graphVersion, err := parquetInt64Column(
		batch,
		parquetRetrievalColumnGraphVersion,
		"graph_version",
	)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	contentHash, err := parquetStringColumn(
		batch,
		parquetRetrievalColumnContentHash,
		"content_hash",
	)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	payload, err := parquetStringColumn(
		batch,
		parquetRetrievalColumnPayload,
		"payload",
	)
	if err != nil {
		return parquetRetrievalEnvelope{}, err
	}
	envelope := parquetRetrievalEnvelope{
		LayoutVersion: int(layoutVersion.Value(0)),
		ObjectKind:    objectKind.Value(0),
		TenantID:      tenantID.Value(0),
		Revision:      revision.Value(0),
		GraphVersion:  graphVersion.Value(0),
		ContentHash:   contentHash.Value(0),
		Payload:       payload.Value(0),
	}
	if envelope.LayoutVersion != RetrievalExtensionLayoutVersion {
		return parquetRetrievalEnvelope{}, fmt.Errorf(
			"unsupported retrieval extension layout version %d",
			envelope.LayoutVersion,
		)
	}
	if envelope.ObjectKind != expectedKind {
		return parquetRetrievalEnvelope{}, fmt.Errorf(
			"retrieval object kind %q, want %q",
			envelope.ObjectKind,
			expectedKind,
		)
	}
	if envelope.ContentHash == "" ||
		objectContentHash([]byte(envelope.Payload)) != envelope.ContentHash {
		return parquetRetrievalEnvelope{}, fmt.Errorf(
			"%s content hash mismatch",
			expectedKind,
		)
	}
	if err := json.Unmarshal([]byte(envelope.Payload), target); err != nil {
		return parquetRetrievalEnvelope{}, fmt.Errorf(
			"decode %s payload: %w",
			expectedKind,
			err,
		)
	}
	return envelope, nil
}

func parquetRetrievalArrowSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{
			Name:     "layout_version",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
		{
			Name:     "object_kind",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
		},
		{
			Name:     "tenant_id",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
		},
		{
			Name:     "revision",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
		{
			Name:     "graph_version",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
		{
			Name:     "content_hash",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
		},
		{
			Name:     "payload",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
		},
	}, nil)
}
