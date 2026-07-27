package storage

import (
	"context"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresEntityRecordReadIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	record := benchmarkEntityRecord("host:a")
	key := store.entityRecordKey(record.TenantID, record.ID)
	putEntityRecordCacheFixture(t, ctx, objects, key, record)
	if loaded, err := store.loadEntityRecord(
		ctx,
		record.TenantID,
		record.ID,
	); err != nil || loaded.Version != record.Version {
		t.Fatalf("prime entity record = %#v, err %v", loaded, err)
	}

	record.Version++
	record.PageETag = "new-page-etag"
	stampEntityRecordHash(&record)
	putEntityRecordCacheFixture(t, ctx, objects, key, record)
	loaded, err := store.loadEntityRecord(
		ctx,
		record.TenantID,
		record.ID,
	)
	if err != nil {
		t.Fatalf("load updated entity record: %v", err)
	}
	if loaded.Version != record.Version ||
		loaded.PageETag != record.PageETag {
		t.Fatalf("updated entity record = %#v", loaded)
	}
}

func TestPostgresEntityRecordWriteIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	record := benchmarkEntityRecord("host:a")
	key := store.entityRecordKey(record.TenantID, record.ID)
	putEntityRecordCacheFixture(t, ctx, objects, key, record)
	if _, err := store.loadEntityRecord(
		ctx,
		record.TenantID,
		record.ID,
	); err != nil {
		t.Fatalf("prime entity record: %v", err)
	}

	current := record
	current.Version++
	current.PageETag = "external-page-etag"
	stampEntityRecordHash(&current)
	putEntityRecordCacheFixture(t, ctx, objects, key, current)
	next := current
	next.Version++
	next.PageETag = "next-page-etag"
	stampEntityRecordHash(&next)
	if err := store.putEntityRecordIfChanged(
		ctx,
		entityRecordWriteJob{Key: key, Record: next},
	); err != nil {
		t.Fatalf("write updated entity record: %v", err)
	}
}

func TestPostgresEntityRecordTombstoneIgnoresStaleWriterCache(t *testing.T) {
	ctx := context.Background()
	store, objects := newCoordinatedCachedStore()
	record := benchmarkEntityRecord("host:a")
	key := store.entityRecordKey(record.TenantID, record.ID)
	putEntityRecordCacheFixture(t, ctx, objects, key, record)
	if _, err := store.loadEntityRecord(
		ctx,
		record.TenantID,
		record.ID,
	); err != nil {
		t.Fatalf("prime entity record: %v", err)
	}

	current := record
	current.Version++
	current.PageETag = "external-page-etag"
	stampEntityRecordHash(&current)
	putEntityRecordCacheFixture(t, ctx, objects, key, current)
	before := graph.New()
	before.Entities[record.ID] = record.Entity
	after := graph.New()
	if err := store.tombstoneDeletedEntityRecords(
		ctx,
		record.TenantID,
		before,
		after,
		[]string{record.ID},
		current.Version+1,
	); err != nil {
		t.Fatalf("tombstone entity record: %v", err)
	}
	data, err := objects.Get(ctx, key)
	if err != nil {
		t.Fatalf("get tombstone: %v", err)
	}
	tombstone, err := decodeEntityRecordObject(
		ctx,
		data,
		key,
		record.TenantID,
		record.ID,
	)
	if err != nil {
		t.Fatalf("decode tombstone: %v", err)
	}
	if !tombstone.Deleted || tombstone.Version != current.Version+1 {
		t.Fatalf("tombstone = %#v", tombstone)
	}
}

func putEntityRecordCacheFixture(
	t *testing.T,
	ctx context.Context,
	objects ObjectStore,
	key string,
	record EntityRecord,
) {
	t.Helper()
	data, err := marshalParquetEntityRecord(ctx, record)
	if err != nil {
		t.Fatalf("marshal entity record: %v", err)
	}
	if err := objects.Put(ctx, key, data); err != nil {
		t.Fatalf("put entity record: %v", err)
	}
}
