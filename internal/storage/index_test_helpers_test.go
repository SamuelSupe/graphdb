package storage

import (
	"context"
	"errors"
	"testing"
)

func newParquetIndexTenantStore(objects ObjectStore, prefix string) *TenantStore {
	return NewTenantStore(objects, prefix)
}

func requireFieldIndexSpec(t *testing.T, catalog IndexCatalog, kind string, field string) IndexSpec {
	t.Helper()
	for _, spec := range catalog.Indexes {
		if spec.Kind == kind && spec.Field == field {
			return spec
		}
	}
	t.Fatalf("missing field index spec %s.%s in %#v", kind, field, catalog.Indexes)
	return IndexSpec{}
}

func requireEdgeShardSpec(t *testing.T, catalog IndexCatalog, relationType string, shard string) EdgeShard {
	t.Helper()
	for _, spec := range catalog.EdgeShards {
		if spec.RelationType == relationType && spec.Shard == shard {
			return spec
		}
	}
	t.Fatalf("missing edge shard spec %s/%s in %#v", relationType, shard, catalog.EdgeShards)
	return EdgeShard{}
}

func requireEntityPageSpec(t *testing.T, catalog IndexCatalog, shard string) EntityPageSpec {
	t.Helper()
	for _, spec := range catalog.EntityPages {
		if spec.Shard == shard {
			return spec
		}
	}
	t.Fatalf("missing entity page spec %s in %#v", shard, catalog.EntityPages)
	return EntityPageSpec{}
}

func requireIndexObjectKey(t *testing.T, objects []IndexObject, role string) string {
	t.Helper()
	for _, object := range objects {
		if object.Role == role && object.Key != "" {
			return object.Key
		}
	}
	t.Fatalf("missing index object role %q in %#v", role, objects)
	return ""
}

func requireAnyIndexObjectKey(t *testing.T, objects []IndexObject) string {
	t.Helper()
	for _, object := range objects {
		if object.Key != "" {
			return object.Key
		}
	}
	t.Fatalf("missing index object key in %#v", objects)
	return ""
}

func readParquetFieldIndexForTest(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, catalog IndexCatalog, kind string, field string) (SecondaryIndex, string) {
	t.Helper()
	spec := requireFieldIndexSpec(t, catalog, kind, field)
	index, ok, err := store.loadParquetSecondaryIndexObject(ctx, tenantID, catalog.Version, spec)
	if err != nil || !ok {
		t.Fatalf("load parquet field index %s.%s ok=%v err=%v", kind, field, ok, err)
	}
	return index, requireAnyIndexObjectKey(t, spec.Objects)
}

func writeParquetFieldIndexForTest(t *testing.T, ctx context.Context, store *TenantStore, key string, index SecondaryIndex) {
	t.Helper()
	data, err := marshalParquetSecondaryIndex(ctx, index)
	if err != nil {
		t.Fatalf("marshal parquet field index: %v", err)
	}
	if err := store.Objects.Put(ctx, key, data); err != nil {
		t.Fatalf("write parquet field index %s: %v", key, err)
	}
}

func readParquetEdgeShardForTest(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, catalog IndexCatalog, relationType string, shardID string) (EdgeShardData, string) {
	t.Helper()
	spec := requireEdgeShardSpec(t, catalog, relationType, shardID)
	shard, ok, err := store.loadParquetEdgeShardObject(ctx, tenantID, catalog.Version, spec)
	if err != nil || !ok {
		t.Fatalf("load parquet edge shard %s/%s ok=%v err=%v", relationType, shardID, ok, err)
	}
	return shard, requireIndexObjectKey(t, spec.Objects, "shard")
}

func writeParquetEdgeShardForTest(t *testing.T, ctx context.Context, store *TenantStore, key string, shard EdgeShardData) {
	t.Helper()
	data, err := marshalParquetEdgeShard(ctx, shard)
	if err != nil {
		t.Fatalf("marshal parquet edge shard: %v", err)
	}
	if err := store.Objects.Put(ctx, key, data); err != nil {
		t.Fatalf("write parquet edge shard %s: %v", key, err)
	}
}

func readParquetEntityPageForTest(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, catalog IndexCatalog, shardID string) (EntityPageData, string) {
	t.Helper()
	spec := requireEntityPageSpec(t, catalog, shardID)
	page, _, ok, err := store.loadParquetEntityPageObject(ctx, tenantID, catalog.Version, spec)
	if err != nil || !ok {
		t.Fatalf("load parquet entity page %s ok=%v err=%v", shardID, ok, err)
	}
	return page, requireIndexObjectKey(t, spec.Objects, "page")
}

func writeParquetEntityPageForTest(t *testing.T, ctx context.Context, store *TenantStore, key string, page EntityPageData) {
	t.Helper()
	data, err := marshalParquetEntityPage(ctx, page)
	if err != nil {
		t.Fatalf("marshal parquet entity page: %v", err)
	}
	if err := store.Objects.Put(ctx, key, data); err != nil {
		t.Fatalf("write parquet entity page %s: %v", key, err)
	}
}

func writeParquetIndexCatalogForTest(t *testing.T, ctx context.Context, store *TenantStore, tenantID string, catalog IndexCatalog) {
	t.Helper()
	key := store.indexCatalogKey(tenantID)
	_, meta, err := store.Objects.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		meta = ObjectMeta{Key: key}
	} else if err != nil {
		t.Fatalf("get index catalog meta: %v", err)
	}
	if err := store.putIndexCatalogWithMeta(ctx, tenantID, catalog, meta); err != nil {
		t.Fatalf("write parquet index catalog: %v", err)
	}
}
