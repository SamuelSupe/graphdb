package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/retrieval"
)

func TestRetrievalHeadCASPublishesOnlyCompleteCompatibleSnapshot(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	store := NewTenantStore(objects, "test")
	seedRetrievalGraphVersion(t, ctx, store, "tenant-a", "doc:1")
	definitions := publishRetrievalTestDefinitions(
		t,
		ctx,
		store,
		"tenant-a",
		0,
	)
	catalog := retrievalTestCatalog(
		t,
		ctx,
		store,
		"tenant-a",
		definitions.Revision,
		"embedding-v1",
	)
	manifestBefore, err := objects.Get(ctx, store.manifestKey("tenant-a"))
	if err != nil {
		t.Fatalf("get manifest before publish: %v", err)
	}

	head, err := store.PublishRetrievalCatalog(
		ctx,
		"tenant-a",
		0,
		catalog,
	)
	if err != nil {
		t.Fatalf("publish retrieval catalog: %v", err)
	}
	if head.Revision != 1 ||
		head.GraphVersion != 1 ||
		head.DefinitionRevision != definitions.Revision ||
		head.EmbeddingGeneration != "embedding-v1" {
		t.Fatalf("head = %#v", head)
	}
	manifestAfter, err := objects.Get(ctx, store.manifestKey("tenant-a"))
	if err != nil {
		t.Fatalf("get manifest after publish: %v", err)
	}
	if !bytes.Equal(manifestBefore, manifestAfter) {
		t.Fatal("retrieval publication changed the layout-v2 core manifest")
	}
	for _, key := range []string{head.CatalogKey, store.retrievalHeadKey("tenant-a")} {
		data, err := objects.Get(ctx, key)
		if err != nil || !isParquetBytes(data) {
			t.Fatalf("retrieval object %q parquet=%v err=%v", key, isParquetBytes(data), err)
		}
		if !strings.Contains(key, "/extensions/v1.2/retrieval/") {
			t.Fatalf("retrieval object escaped compatibility extension: %q", key)
		}
	}
	snapshot, err := store.ResolveRetrievalSnapshot(ctx, "tenant-a", 1)
	if err != nil {
		t.Fatalf("resolve retrieval snapshot: %v", err)
	}
	if snapshot.Revision != 1 ||
		snapshot.GraphVersion != 1 ||
		snapshot.CatalogHash != head.CatalogHash {
		t.Fatalf("snapshot = %#v", snapshot)
	}

	if _, err := store.PublishRetrievalCatalog(
		ctx,
		"tenant-a",
		0,
		catalog,
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale publish err = %v, want ErrConflict", err)
	}
	current, err := store.GetRetrievalHead(ctx, "tenant-a")
	if err != nil || current.CatalogHash != head.CatalogHash {
		t.Fatalf("head after stale publish = %#v err=%v", current, err)
	}
}

func TestRetrievalHeadIsNotVisibleWhenAnySegmentIsMissing(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedRetrievalGraphVersion(t, ctx, store, "tenant-a", "doc:1")
	definitions := publishRetrievalTestDefinitions(
		t,
		ctx,
		store,
		"tenant-a",
		0,
	)
	catalog := retrievalTestCatalog(
		t,
		ctx,
		store,
		"tenant-a",
		definitions.Revision,
		"embedding-v1",
	)
	catalog.Segments[0].Key = strings.Replace(
		catalog.Segments[0].Key,
		"0000.parquet",
		"missing.parquet",
		1,
	)

	if _, err := store.PublishRetrievalCatalog(
		ctx,
		"tenant-a",
		0,
		catalog,
	); err == nil || !strings.Contains(err.Error(), "load retrieval segment") {
		t.Fatalf("incomplete publish err = %v", err)
	}
	if _, err := store.GetRetrievalHead(ctx, "tenant-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("head err = %v, want ErrNotFound", err)
	}
}

func TestRetrievalSnapshotCacheRefreshesToSatisfyMinVersion(t *testing.T) {
	ctx := context.Background()
	objects := NewMemoryStore()
	writer := NewTenantStore(objects, "test")
	seedRetrievalGraphVersion(t, ctx, writer, "tenant-a", "doc:1")
	definitions := publishRetrievalTestDefinitions(
		t,
		ctx,
		writer,
		"tenant-a",
		0,
	)
	firstCatalog := retrievalTestCatalog(
		t,
		ctx,
		writer,
		"tenant-a",
		definitions.Revision,
		"embedding-v1",
	)
	if _, err := writer.PublishRetrievalCatalog(
		ctx,
		"tenant-a",
		0,
		firstCatalog,
	); err != nil {
		t.Fatalf("publish retrieval v1: %v", err)
	}

	reader := NewTenantStore(objects, "test")
	reader.LifecycleCacheTTL = time.Hour
	first, err := reader.ResolveRetrievalSnapshot(ctx, "tenant-a", 0)
	if err != nil || first.GraphVersion != 1 {
		t.Fatalf("resolve retrieval v1 = %#v err=%v", first, err)
	}

	seedRetrievalGraphVersion(t, ctx, writer, "tenant-a", "doc:2")
	secondCatalog := retrievalTestCatalog(
		t,
		ctx,
		writer,
		"tenant-a",
		definitions.Revision,
		"embedding-v2",
	)
	if _, err := writer.PublishRetrievalCatalog(
		ctx,
		"tenant-a",
		1,
		secondCatalog,
	); err != nil {
		t.Fatalf("publish retrieval v2: %v", err)
	}

	second, err := reader.ResolveRetrievalSnapshot(ctx, "tenant-a", 2)
	if err != nil {
		t.Fatalf("resolve retrieval min v2: %v", err)
	}
	if second.GraphVersion != 2 ||
		second.Revision != 2 ||
		second.EmbeddingGeneration != "embedding-v2" {
		t.Fatalf("refreshed snapshot = %#v", second)
	}
}

func TestRetrievalDefinitionRevisionInvalidatesPublishedSnapshot(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedRetrievalGraphVersion(t, ctx, store, "tenant-a", "doc:1")
	definitions := publishRetrievalTestDefinitions(
		t,
		ctx,
		store,
		"tenant-a",
		0,
	)
	catalog := retrievalTestCatalog(
		t,
		ctx,
		store,
		"tenant-a",
		definitions.Revision,
		"embedding-v1",
	)
	if _, err := store.PublishRetrievalCatalog(
		ctx,
		"tenant-a",
		0,
		catalog,
	); err != nil {
		t.Fatalf("publish retrieval catalog: %v", err)
	}
	if _, err := store.ResolveRetrievalSnapshot(ctx, "tenant-a", 0); err != nil {
		t.Fatalf("resolve published snapshot: %v", err)
	}
	publishRetrievalTestDefinitions(t, ctx, store, "tenant-a", 1)

	if _, err := store.ResolveRetrievalSnapshot(
		ctx,
		"tenant-a",
		0,
	); !errors.Is(err, retrieval.ErrNotReady) {
		t.Fatalf("snapshot after definition change err = %v", err)
	}
}

func seedRetrievalGraphVersion(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	tenantID string,
	entityID string,
) {
	t.Helper()
	if _, err := store.Commit(
		ctx,
		tenantID,
		graph.Mutations{UpsertEntities: []graph.Entity{{
			ID:     entityID,
			Kind:   "Document",
			Fields: graph.Fields{"text": "checkout failed"},
		}}},
		CommitOptions{},
	); err != nil {
		t.Fatalf("commit graph version: %v", err)
	}
	if _, err := store.RebuildIndexes(ctx, tenantID); err != nil {
		t.Fatalf("rebuild graph indexes: %v", err)
	}
}

func publishRetrievalTestDefinitions(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	tenantID string,
	expectedRevision int64,
) RetrievalDefinitionRecord {
	t.Helper()
	record, err := store.PublishRetrievalDefinitions(
		ctx,
		tenantID,
		expectedRevision,
		[]RetrievalDefinition{{
			Name:             "documents",
			Kinds:            []string{"Document"},
			TextFields:       []string{"text"},
			EmbeddingProfile: "default",
			Enabled:          true,
		}},
	)
	if err != nil {
		t.Fatalf("publish retrieval definitions: %v", err)
	}
	return record
}

func retrievalTestCatalog(
	t *testing.T,
	ctx context.Context,
	store *TenantStore,
	tenantID string,
	definitionRevision int64,
	embeddingGeneration string,
) RetrievalCatalog {
	t.Helper()
	indexCatalog, err := store.GetIndexCatalog(ctx, tenantID)
	if err != nil {
		t.Fatalf("get graph index catalog: %v", err)
	}
	indexKey := store.indexCatalogVersionKey(tenantID, indexCatalog.Version)
	indexData, err := store.Objects.Get(ctx, indexKey)
	if err != nil {
		t.Fatalf("get immutable graph index catalog: %v", err)
	}
	segments := make([]RetrievalSegmentRef, 0, 3)
	for _, kind := range []string{
		RetrievalSegmentChunks,
		RetrievalSegmentVector,
		RetrievalSegmentLexical,
	} {
		data := []byte("PAR1" + kind + "-" + embeddingGeneration)
		segment, err := store.PutRetrievalSegment(
			ctx,
			tenantID,
			embeddingGeneration,
			indexCatalog.Version,
			kind,
			"0000",
			kind+"-test-v1",
			1,
			"",
			data,
		)
		if err != nil {
			t.Fatalf("put %s retrieval segment: %v", kind, err)
		}
		segments = append(segments, segment)
	}
	return RetrievalCatalog{
		GraphVersion:        indexCatalog.Version,
		DefinitionRevision:  definitionRevision,
		EmbeddingProfile:    "default",
		EmbeddingGeneration: embeddingGeneration,
		EmbeddingDimensions: 3,
		IndexCatalogKey:     indexKey,
		IndexCatalogHash:    objectContentHash(indexData),
		Segments:            segments,
	}
}
