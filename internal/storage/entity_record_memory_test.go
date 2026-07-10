package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestParquetEntityRecordUsesSingleRowGroup(t *testing.T) {
	data, err := marshalParquetEntityRecord(context.Background(), benchmarkEntityRecord("host:a"))
	if err != nil {
		t.Fatalf("marshal entity record: %v", err)
	}
	reader, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("open entity record: %v", err)
	}
	if got := reader.NumRowGroups(); got != 1 {
		t.Fatalf("row groups = %d, want 1", got)
	}
}

func TestDecodeParquetEntityRecordAcceptsLegacyRowGroups(t *testing.T) {
	ctx := context.Background()
	want := benchmarkEntityRecord("host:a")
	current, err := marshalParquetEntityRecord(ctx, want)
	if err != nil {
		t.Fatalf("marshal current entity record: %v", err)
	}
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(current), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("read current entity record: %v", err)
	}
	defer table.Release()

	var legacy bytes.Buffer
	legacyWriterProperties := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Snappy))
	legacyArrowProperties := pqarrow.NewArrowWriterProperties(
		pqarrow.WithStoreSchema(),
		pqarrow.WithAllocator(memory.DefaultAllocator),
	)
	if err := pqarrow.WriteTable(table, &legacy, 1, legacyWriterProperties, legacyArrowProperties); err != nil {
		t.Fatalf("write legacy entity record: %v", err)
	}
	legacyReader, err := file.NewParquetReader(bytes.NewReader(legacy.Bytes()))
	if err != nil {
		t.Fatalf("open legacy entity record: %v", err)
	}
	if legacyReader.NumRowGroups() < 2 {
		t.Fatalf("legacy row groups = %d, want multiple", legacyReader.NumRowGroups())
	}

	got, err := decodeEntityRecordObject(ctx, legacy.Bytes(), "legacy.parquet", want.TenantID, want.ID)
	if err != nil {
		t.Fatalf("decode legacy entity record: %v", err)
	}
	if got.ContentHash != want.ContentHash || entityRecordContentHash(got) != entityRecordContentHash(want) {
		t.Fatalf("decoded legacy content mismatch: got=%#v want=%#v", got, want)
	}
}

func TestPutEntityRecordIfChangedRepairsTamperedContent(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	record := benchmarkEntityRecord("host:a")
	job := entityRecordWriteJob{Key: store.entityRecordKey(record.TenantID, record.ID), Record: record}
	if err := store.putEntityRecordIfChanged(ctx, job); err != nil {
		t.Fatalf("put entity record: %v", err)
	}

	tampered := record
	tampered.Entity.Fields = map[string]any{"hostname": "tampered"}
	data, err := marshalParquetEntityRecord(ctx, tampered)
	if err != nil {
		t.Fatalf("marshal tampered entity record: %v", err)
	}
	if err := store.Objects.Put(ctx, job.Key, data); err != nil {
		t.Fatalf("put tampered entity record: %v", err)
	}
	if err := store.putEntityRecordIfChanged(ctx, job); err != nil {
		t.Fatalf("repair entity record: %v", err)
	}

	got, err := store.loadEntityRecord(ctx, record.TenantID, record.ID)
	if err != nil {
		t.Fatalf("load repaired entity record: %v", err)
	}
	if got.ContentHash != record.ContentHash || entityRecordContentHash(got) != record.ContentHash {
		t.Fatalf("repaired content hash mismatch: got=%q recomputed=%q want=%q", got.ContentHash, entityRecordContentHash(got), record.ContentHash)
	}
}

func TestPutEntityRecordIfChangedRejectsNewerRecord(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	record := benchmarkEntityRecord("host:a")
	record.Version = 2
	stampEntityRecordHash(&record)
	key := store.entityRecordKey(record.TenantID, record.ID)
	if err := store.putEntityRecordIfChanged(ctx, entityRecordWriteJob{Key: key, Record: record}); err != nil {
		t.Fatalf("put newer entity record: %v", err)
	}

	older := record
	older.Version = 1
	older.PageETag = "older-page-etag"
	stampEntityRecordHash(&older)
	err := store.putEntityRecordIfChanged(ctx, entityRecordWriteJob{Key: key, Record: older})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("put older record error = %v, want conflict", err)
	}
}

func TestPutEntityRecordIfChangedUpdatesPageBinding(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	record := benchmarkEntityRecord("host:a")
	key := store.entityRecordKey(record.TenantID, record.ID)
	if err := store.putEntityRecordIfChanged(ctx, entityRecordWriteJob{Key: key, Record: record}); err != nil {
		t.Fatalf("put entity record: %v", err)
	}

	record.PageHash = "new-page-hash"
	record.PageETag = "new-page-etag"
	record.Version++
	stampEntityRecordHash(&record)
	if err := store.putEntityRecordIfChanged(ctx, entityRecordWriteJob{Key: key, Record: record}); err != nil {
		t.Fatalf("update page binding: %v", err)
	}
	got, err := store.loadEntityRecord(ctx, record.TenantID, record.ID)
	if err != nil {
		t.Fatalf("load updated entity record: %v", err)
	}
	if got.PageHash != record.PageHash || got.PageETag != record.PageETag || entityRecordContentHash(got) != record.ContentHash {
		t.Fatalf("updated entity record mismatch: got=%#v want=%#v", got, record)
	}
}
