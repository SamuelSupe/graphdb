package storage

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

func TestParquetRebuildLookupScanInspectAndHealth(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedIndexedGraph(t, ctx, store)
	catalog, err := store.RebuildIndexesWithOptions(ctx, "tenant-a", IndexRebuildOptions{Format: IndexFormatParquet})
	if err != nil {
		t.Fatalf("rebuild parquet: %v", err)
	}
	assertCatalogParquet(t, catalog)

	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-01"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-01" {
		t.Fatalf("match index ids=%#v ok=%v err=%v", ids, ok, err)
	}
	edges, ok, err := lookup.OutEdges(ctx, "service:api", map[string]struct{}{"runs_on": {}})
	if err != nil || !ok || len(edges) != 1 || edges[0].To != "host:app-01" {
		t.Fatalf("out edges=%#v ok=%v err=%v", edges, ok, err)
	}

	entity, ok, err := lookup.GetEntity(ctx, "host:app-01", []string{"hostname"})
	if err != nil || !ok {
		t.Fatalf("get entity ok=%v err=%v", ok, err)
	}
	if entity.Kind != "host" || entity.Fields["hostname"] != "app-01" {
		t.Fatalf("entity = %#v", entity)
	}
	if _, ok := entity.Fields["cpu"]; ok {
		t.Fatalf("projection leaked cpu field: %#v", entity.Fields)
	}

	hosts, ok, err := lookup.ListEntities(ctx, "host", []string{"region"})
	if err != nil || !ok || len(hosts) != 2 {
		t.Fatalf("list hosts len=%d ok=%v err=%v hosts=%#v", len(hosts), ok, err, hosts)
	}
	for _, host := range hosts {
		if host.Kind != "host" || len(host.Fields) != 1 || host.Fields["region"] == nil {
			t.Fatalf("projected host = %#v", host)
		}
	}

	scanned, err := store.ListEntities(ctx, "tenant-a", EntityScanOptions{Kind: "host", Limit: 10})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !scanned.IndexedRead || len(scanned.Entities) != 2 {
		t.Fatalf("scan = %#v", scanned)
	}
	scannedEdges, err := store.ListEdges(ctx, "tenant-a", EdgeScanOptions{Type: "runs_on", Limit: 10})
	if err != nil {
		t.Fatalf("edge scan: %v", err)
	}
	if !scannedEdges.IndexedRead || len(scannedEdges.Edges) != 1 {
		t.Fatalf("edge scan = %#v", scannedEdges)
	}

	inspection, err := store.InspectIndex(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	foundParquet := map[string]bool{}
	for _, object := range inspection.Objects {
		if object.Format == IndexFormatParquet {
			foundParquet[object.Codec] = true
			if !object.HashMatches || object.RowCount == 0 {
				t.Fatalf("parquet inspect object = %#v", object)
			}
		}
	}
	for _, codec := range []string{parquetSecondaryIndexCodec, parquetEdgeShardCodec, parquetEntityPageCodec} {
		if !foundParquet[codec] {
			t.Fatalf("inspection missing parquet codec %s: %#v", codec, inspection.Objects)
		}
	}
	if len(foundParquet) == 0 {
		t.Fatalf("inspection has no parquet objects: %#v", inspection.Objects)
	}

	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "ready" || len(health.Issues) != 0 {
		t.Fatalf("health = %#v", health)
	}
}

func TestEntityPageRowsDecodeOutOfOrdinalOrder(t *testing.T) {
	entity := graph.Entity{ID: "host:a", Kind: "host"}
	rows := []entityPageRow{
		{Kind: entityPageRowField, Ordinal: 1, Key: "region", Value: parquetValue{Kind: parquetValueKindString, StringValue: "r1"}},
		{Kind: entityPageRowField, Ordinal: 0, Key: "hostname", Value: parquetValue{Kind: parquetValueKindString, StringValue: "app-a"}},
		{Kind: entityPageRowSource, Ordinal: 1, EntitySource: graph.EntitySource{Source: "agent"}},
		{Kind: entityPageRowSource, Ordinal: 0, EntitySource: graph.EntitySource{Source: "manual"}},
	}
	for _, row := range rows {
		if err := applyEntityPageRow(&entity, row); err != nil {
			t.Fatalf("apply row %#v: %v", row, err)
		}
	}
	if entity.Fields["hostname"] != "app-a" || entity.Fields["region"] != "r1" {
		t.Fatalf("fields = %#v", entity.Fields)
	}
	if len(entity.Sources) != 2 || entity.Sources[0].Source != "manual" || entity.Sources[1].Source != "agent" {
		t.Fatalf("sources = %#v", entity.Sources)
	}
}

func TestParquetCatalogRefreshesAfterCommit(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	seedIndexedGraph(t, ctx, store)
	if _, err := store.RebuildIndexesWithOptions(ctx, "tenant-a", IndexRebuildOptions{Format: IndexFormatParquet}); err != nil {
		t.Fatalf("rebuild parquet: %v", err)
	}
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:app-03", Kind: "host", Fields: graph.Fields{"hostname": "app-03", "region": "r1"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(result.IndexWarnings) != 0 {
		t.Fatalf("index warnings = %#v", result.IndexWarnings)
	}
	catalog, err := store.GetIndexCatalog(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if catalog.Version != result.Manifest.Version {
		t.Fatalf("catalog version = %d, want %d", catalog.Version, result.Manifest.Version)
	}
	lookup := &PersistedIndexLookup{Store: store, TenantID: "tenant-a", Version: catalog.Version, Catalog: catalog}
	ids, ok, err := lookup.MatchFieldIndex(ctx, "host", "hostname", []any{"app-03"})
	if err != nil || !ok || len(ids) != 1 || ids[0] != "host:app-03" {
		t.Fatalf("match after commit ids=%#v ok=%v err=%v", ids, ok, err)
	}
	health, err := store.IndexHealth(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if health.Status != "ready" || len(health.Issues) != 0 {
		t.Fatalf("health = %#v", health)
	}
}

func TestParquetRebuildSkipsUnchangedObjectWrites(t *testing.T) {
	ctx := context.Background()
	objects := newCountingIndexStore(NewMemoryStore())
	store := NewTenantStore(objects, "test")
	seedIndexedGraph(t, ctx, store)

	objects.TrackIndexes = true
	if _, err := store.RebuildIndexesWithOptions(ctx, "tenant-a", IndexRebuildOptions{Format: IndexFormatParquet}); err != nil {
		t.Fatalf("first rebuild parquet: %v", err)
	}
	if writes := objects.PutCount("/indexes/parquet/"); writes == 0 {
		t.Fatal("first parquet rebuild did not write parquet objects")
	}

	objects.ResetPutCounts()
	if _, err := store.RebuildIndexesWithOptions(ctx, "tenant-a", IndexRebuildOptions{Format: IndexFormatParquet}); err != nil {
		t.Fatalf("second rebuild parquet: %v", err)
	}
	if writes := objects.PutCount("/indexes/parquet/"); writes != 0 {
		t.Fatalf("second parquet rebuild wrote %d unchanged parquet objects", writes)
	}
}

func TestParquetSchemasAvoidJSONPayloadColumns(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()
	lease, err := marshalParquetWriterLease(ctx, WriterLease{
		TenantID:  "tenant-a",
		OwnerID:   "writer-a",
		ExpiresAt: now.Add(time.Minute),
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("marshal lease: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "writer lease", lease)

	heartbeat, err := marshalParquetReaderHeartbeat(ctx, ReaderHeartbeat{
		TenantID:        "tenant-a",
		ReaderID:        "reader-a",
		InstanceID:      "reader-a",
		Mode:            "reader",
		Status:          "fresh",
		Fresh:           true,
		Consistent:      true,
		ManifestVersion: 3,
		VisibleVersion:  3,
		LastSeenAt:      now,
	})
	if err != nil {
		t.Fatalf("marshal heartbeat: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "reader heartbeat", heartbeat)

	status, err := marshalParquetCollectorStatus(ctx, CollectorStatus{
		TenantID:       "tenant-a",
		Source:         "aws",
		CollectorID:    "collector-a",
		LastBatchID:    "batch-1",
		LastCursor:     "cursor-1",
		LastVersion:    4,
		LastStartedAt:  now.Add(-time.Second),
		LastFinishedAt: now,
		AppliedTotal:   10,
		FailedTotal:    1,
	})
	if err != nil {
		t.Fatalf("marshal collector status: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "collector status", status)

	policy, err := marshalParquetSourcePolicy(ctx, sourcePolicyRecord{
		TenantID: "tenant-a",
		SourcePolicy: graph.SourcePolicy{
			DefaultPriority: 1,
			Sources: []graph.SourcePolicyItem{
				{Name: "manual", Priority: 1000, Description: "operator input"},
				{Name: "aws", Priority: 50},
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal source policy: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "source policy", policy)

	manifest, err := marshalParquetManifest(ctx, Manifest{
		TenantID:           "tenant-a",
		Version:            3,
		HeadCommitID:       "commit-3",
		SnapshotKey:        "test/tenants/tenant-a/snapshots/2.parquet",
		SnapshotCatalogKey: "test/tenants/tenant-a/snapshots/sharded/v2/catalog.parquet",
		SnapshotVersion:    2,
		UpdatedAt:          now,
		CommitSegments: []CommitSegmentRef{{
			Key:          "test/tenants/tenant-a/commits/segments/0001-0002.parquet",
			Codec:        commitSegmentCodecParquet,
			FirstVersion: 1,
			LastVersion:  2,
			Count:        2,
			ContentHash:  "hash-segment",
		}},
		CommitKeys: []string{"test/tenants/tenant-a/commits/00000000000000000003-commit-3.parquet"},
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "manifest", manifest)

	commit := graph.Commit{
		ID:        "commit-4",
		TenantID:  "tenant-a",
		Version:   4,
		CreatedAt: now,
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}, Version: 4, UpdatedAt: now,
		}}},
	}
	commitObject, err := marshalCommitObject(commit)
	if err != nil {
		t.Fatalf("marshal commit object: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "commit object", commitObject)
	commitSegment, err := marshalParquetCommitSegment(ctx, "tenant-a", []commitSegmentItem{{Key: "test/tenants/tenant-a/commits/00000000000000000004-commit-4.parquet", Commit: commit}})
	if err != nil {
		t.Fatalf("marshal commit segment: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "commit segment", commitSegment)

	quotaLimit := 100
	autoCompact := true
	config, err := marshalParquetTenantConfig(ctx, tenantConfigRecord{
		TenantID: "tenant-a",
		Config: TenantConfig{
			Quota:       TenantQuotaConfig{MaxEntitiesPerTenant: &quotaLimit},
			Maintenance: TenantMaintenanceConfig{AutoCompact: &autoCompact},
		},
	})
	if err != nil {
		t.Fatalf("marshal tenant config: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "tenant config", config)

	registry, err := marshalParquetTenantRegistry(ctx, tenantRegistry{
		TenantIDs: []string{"tenant-a", "tenant-b"},
		UpdatedAt: now.Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("marshal tenant registry: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "tenant registry", registry)

	metadata, err := marshalParquetTenantMetadata(ctx, TenantMetadata{
		TenantID:    "tenant-a",
		Status:      TenantStatusActive,
		Name:        "Tenant A",
		Description: "primary tenant",
		Labels:      map[string]string{"env": "prod"},
		Metadata:    map[string]any{"owner": "platform", "critical": true, "weight": 1.5},
		CreatedAt:   now.Add(-time.Hour),
		UpdatedAt:   now,
	})
	if err != nil {
		t.Fatalf("marshal tenant metadata: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "tenant metadata", metadata)

	indexes, err := marshalParquetIndexDefinitions(ctx, IndexDefinitionRecord{
		TenantID: "tenant-a",
		Indexes: []IndexDefinition{{
			Name:      "host.hostname",
			Kind:      "host",
			Field:     "hostname",
			Unique:    true,
			CreatedAt: now.Add(-time.Minute),
			UpdatedAt: now,
		}},
	})
	if err != nil {
		t.Fatalf("marshal index definitions: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "index definitions", indexes)

	savedQuery, err := marshalParquetSavedQuery(ctx, SavedQuery{
		TenantID:    "tenant-a",
		Name:        "host-impact",
		Description: "impact query",
		Request: query.Request{
			Op:            "impact",
			Kind:          "service",
			Where:         []query.Filter{{Field: "name", Op: "=", Value: "api"}},
			RelationTypes: []string{"runs_on"},
			Sort:          []query.SortSpec{{Field: "name", Desc: true}},
			Project:       []string{"name"},
			Aggregate:     []query.Aggregation{{Name: "count_hosts", Op: "count", Field: "id"}},
			Limit:         50,
		},
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("marshal saved query: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "saved query", savedQuery)

	entity := graph.Entity{
		ID:         "host:app-01",
		Kind:       "host",
		Fields:     graph.Fields{"hostname": "app-01", "cpu": float64(4)},
		Source:     "agent",
		ExternalID: "i-1",
		Version:    4,
		UpdatedAt:  now,
		FieldSources: map[string]graph.FieldSource{
			"hostname": {Source: "agent", Priority: 100, Version: 4, UpdatedAt: now},
		},
		Sources: []graph.EntitySource{{
			Source:     "agent",
			ExternalID: "i-1",
			Priority:   100,
			ObservedAt: now,
		}},
	}
	entityPage, err := marshalParquetEntityPage(ctx, EntityPageData{
		TenantID:  "tenant-a",
		Shard:     "ab",
		Entities:  []graph.Entity{entity},
		Version:   4,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("marshal entity page: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "entity page", entityPage)

	entityRecord, err := marshalParquetEntityRecord(ctx, EntityRecord{
		TenantID:    "tenant-a",
		ID:          entity.ID,
		Page:        "ab",
		PageHash:    "page-hash",
		PageETag:    "page-etag",
		ContentHash: "record-hash",
		Entity:      entity,
		Version:     4,
		UpdatedAt:   now,
	})
	if err != nil {
		t.Fatalf("marshal entity record: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "entity record", entityRecord)

	edge := graph.Edge{
		ID:         "edge:1",
		Type:       "runs_on",
		From:       "service:api",
		To:         "host:app-01",
		Fields:     graph.Fields{"port": float64(443)},
		Source:     "agent",
		ExternalID: "rel-1",
		Version:    4,
		UpdatedAt:  now,
		FieldSources: map[string]graph.FieldSource{
			"port": {Source: "agent", Priority: 100, Version: 4, UpdatedAt: now},
		},
		Sources: []graph.EdgeSource{{
			Source:     "agent",
			ExternalID: "rel-1",
			EdgeID:     "collector-edge-1",
			Priority:   100,
			ObservedAt: now,
		}},
	}
	edgeShard, err := marshalParquetEdgeShard(ctx, EdgeShardData{
		TenantID:     "tenant-a",
		RelationType: "runs_on",
		Shard:        "ab",
		Edges:        []graph.Edge{edge},
		Version:      4,
		UpdatedAt:    now,
	})
	if err != nil {
		t.Fatalf("marshal edge shard: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "edge shard", edgeShard)

	snapshotRecordBytes, err := marshalParquetSnapshotRecord(ctx, snapshotRecord{
		TenantID: "tenant-a",
		Snapshot: graph.Snapshot{
			Version: 4,
			CITypes: []graph.CIType{{
				Name:   "host",
				Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}},
			}, {
				Name: "service",
			}},
			RelationTypes: []graph.RelationType{{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true}},
			Entities:      []graph.Entity{entity},
			Edges:         []graph.Edge{edge},
		},
	})
	if err != nil {
		t.Fatalf("marshal snapshot record: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "snapshot record", snapshotRecordBytes)
	if _, err := decodeParquetSnapshotRecord(ctx, snapshotRecordBytes); err != nil {
		t.Fatalf("decode snapshot record: %v", err)
	}

	taskBytes, err := marshalParquetTask(ctx, Task{
		ID:                "task-1",
		TenantID:          "tenant-a",
		Type:              TaskTypeGC,
		Status:            "running",
		Phase:             "scan",
		ProgressCompleted: 1,
		ProgressTotal:     3,
		OwnerID:           "worker-1",
		Params:            map[string]any{"deadletter_max_age_seconds": float64(3600), "dry_run": true},
		Result:            map[string]any{"deleted": float64(2)},
		ResultKey:         "test/tenants/tenant-a/tasks/results/task-1.parquet",
		StartedAt:         now,
		UpdatedAt:         now,
	})
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "task", taskBytes)
	if _, err := decodeParquetTask(ctx, taskBytes); err != nil {
		t.Fatalf("decode task: %v", err)
	}

	indexTaskBytes, err := marshalParquetIndexTask(ctx, IndexTask{
		ID:                "index-task-1",
		TenantID:          "tenant-a",
		Type:              "rebuild",
		Status:            "succeeded",
		Phase:             "done",
		ProgressCompleted: 1,
		ProgressTotal:     1,
		OwnerID:           "worker-1",
		CatalogVersion:    4,
		StartedAt:         now,
		UpdatedAt:         now,
		FinishedAt:        now,
	})
	if err != nil {
		t.Fatalf("marshal index task: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "index task", indexTaskBytes)
	if _, err := decodeParquetIndexTask(ctx, indexTaskBytes); err != nil {
		t.Fatalf("decode index task: %v", err)
	}

	taskResultBytes, err := marshalParquetTaskResult(ctx, "tenant-a", "task-1", map[string]any{
		"tenant_id": "tenant-a",
		"version":   float64(4),
		"snapshot":  map[string]any{"entities": []any{"host:app-01"}},
	})
	if err != nil {
		t.Fatalf("marshal task result: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "task result", taskResultBytes)
	if _, err := decodeParquetTaskResult(ctx, taskResultBytes, "tenant-a", "task-1"); err != nil {
		t.Fatalf("decode task result: %v", err)
	}

	expectedVersion := int64(3)
	directCommitBytes, err := marshalParquetDirectCommitRecord(ctx, DirectCommitRecord{
		TenantID: "tenant-a",
		Request: DirectCommitRequest{
			ExpectedVersion: &expectedVersion,
			IdempotencyKey:  "idem-1",
			Mutations: graph.Mutations{
				UpsertEntities: []graph.Entity{entity},
				UpsertEdges:    []graph.Edge{edge},
			},
		},
		Result: CommitResult{
			Manifest: Manifest{
				LayoutVersion: CurrentObjectLayoutVersion,
				TenantID:      "tenant-a",
				Version:       4,
				HeadCommitID:  "commit-1",
				CommitKeys:    []string{"test/tenants/tenant-a/commits/00000000000000000004-commit-1.parquet"},
				UpdatedAt:     now,
			},
			ReadableVersion:   4,
			ReadAfterCommitID: "commit-1",
			DataMD5:           "md5",
			Suppressed: []graph.FieldConflict{{
				ResourceType:     "entity",
				EntityID:         "host:app-01",
				Field:            "owner",
				ExistingSource:   "manual",
				ExistingPriority: 1000,
				IncomingSource:   "agent",
				IncomingPriority: 100,
				ExistingValue:    "platform",
				IncomingValue:    "ops",
				Message:          "lower priority suppressed",
			}},
			CanonicalEntities: []graph.EntityCanonicalization{{CanonicalID: "host:app-01", IncomingID: "i-1", Kind: "host", Source: "agent", ExternalID: "i-1"}},
			CanonicalEdges:    []graph.EdgeCanonicalization{{CanonicalID: edge.ID, IncomingID: "rel-1", Type: edge.Type, From: edge.From, To: edge.To}},
			IndexWarnings:     []string{"index stale"},
		},
		StartedAt:  now,
		FinishedAt: now,
	})
	if err != nil {
		t.Fatalf("marshal direct commit record: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "direct commit record", directCommitBytes)
	if _, err := decodeParquetDirectCommitRecord(ctx, directCommitBytes); err != nil {
		t.Fatalf("decode direct commit record: %v", err)
	}

	ingestRecordBytes, err := marshalParquetIngestRecord(ctx, IngestBatchRecord{
		TenantID: "tenant-a",
		Request: IngestRequest{
			Source:         "agent",
			CollectorID:    "collector-a",
			BatchID:        "batch-1",
			IdempotencyKey: "idem-ingest-1",
			Cursor:         "cursor-1",
			Items: []IngestItem{{
				ExternalID: "host-ext-1",
				Entity:     &entity,
			}, {
				ExternalID: "edge-ext-1",
				Edge:       &edge,
			}},
		},
		Result: IngestResult{
			BatchID: "batch-1",
			Version: 4,
			Applied: 2,
			Conflicts: []IngestConflict{{
				ResourceType:     "entity",
				Index:            0,
				ExternalID:       "host-ext-1",
				EntityID:         "host:app-01",
				Field:            "owner",
				ExistingSource:   "manual",
				ExistingPriority: 1000,
				IncomingSource:   "agent",
				IncomingPriority: 100,
				ExistingValue:    "platform",
				IncomingValue:    "ops",
				Message:          "lower priority suppressed",
			}},
		},
		StartedAt:  now,
		FinishedAt: now,
	})
	if err != nil {
		t.Fatalf("marshal ingest record: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "ingest record", ingestRecordBytes)
	if _, err := decodeParquetIngestRecord(ctx, ingestRecordBytes); err != nil {
		t.Fatalf("decode ingest record: %v", err)
	}

	deadLetterBytes, err := marshalParquetDeadLetter(ctx, DeadLetter{
		ID:       "collector-a/batch-1",
		TenantID: "tenant-a",
		Source:   "agent",
		BatchID:  "batch-1",
		Request: IngestRequest{
			Source:      "agent",
			CollectorID: "collector-a",
			BatchID:     "batch-1",
			Items:       []IngestItem{{ExternalID: "host-ext-1", Entity: &entity}},
		},
		LastResult: IngestResult{
			BatchID: "batch-1",
			Version: 4,
			Failed:  1,
			Failures: []IngestFailure{{
				Index:      0,
				ExternalID: "host-ext-1",
				Error:      "boom",
			}},
		},
		Attempts:  1,
		Status:    "pending",
		Error:     "boom",
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("marshal deadletter: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "deadletter", deadLetterBytes)
	if _, err := decodeParquetDeadLetter(ctx, deadLetterBytes); err != nil {
		t.Fatalf("decode deadletter: %v", err)
	}

	catalog, err := marshalParquetIndexCatalog(ctx, IndexCatalog{
		TenantID:  "tenant-a",
		Version:   4,
		UpdatedAt: now,
		Indexes: []IndexSpec{{
			Name:           "host.hostname",
			Kind:           "host",
			Field:          "hostname",
			Type:           "field",
			Status:         "ready",
			Format:         IndexFormatParquet,
			Codec:          parquetSecondaryIndexCodec,
			RowCount:       2,
			EntryCount:     2,
			DistinctValues: 2,
			TopValues:      []IndexValueStat{{Value: "app-01", Count: 1}},
			ContentHash:    "index-hash",
			SchemaHash:     "index-schema",
			UpdatedAt:      now,
			Objects: []IndexObject{{
				Role:        "postings",
				Key:         "test/tenants/tenant-a/indexes/parquet/versions/v4/fields/host/hostname.parquet",
				Format:      IndexFormatParquet,
				Codec:       parquetSecondaryIndexCodec,
				RowCount:    2,
				ContentHash: "index-hash",
				SchemaHash:  "index-schema",
			}},
		}},
		EdgeShards: []EdgeShard{{
			RelationType: "runs_on",
			Shard:        "ab",
			Format:       IndexFormatParquet,
			Codec:        parquetEdgeShardCodec,
			RowCount:     1,
			EdgeCount:    1,
			ContentHash:  "edge-hash",
			SchemaHash:   "edge-schema",
			UpdatedAt:    now,
			Objects: []IndexObject{{
				Role:        "shard",
				Key:         "test/tenants/tenant-a/indexes/parquet/versions/v4/edges/runs_on/ab.parquet",
				Format:      IndexFormatParquet,
				Codec:       parquetEdgeShardCodec,
				RowCount:    1,
				ContentHash: "edge-hash",
				SchemaHash:  "edge-schema",
			}},
		}},
		EntityPages: []EntityPageSpec{{
			Shard:       "cd",
			Format:      IndexFormatParquet,
			Codec:       parquetEntityPageCodec,
			RowCount:    2,
			EntityCount: 2,
			ContentHash: "page-hash",
			SchemaHash:  "page-schema",
			UpdatedAt:   now,
			Objects: []IndexObject{{
				Role:        "page",
				Key:         "test/tenants/tenant-a/indexes/parquet/versions/v4/entities/pages/cd.parquet",
				Format:      IndexFormatParquet,
				Codec:       parquetEntityPageCodec,
				RowCount:    2,
				ContentHash: "page-hash",
				SchemaHash:  "page-schema",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("marshal index catalog: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "index catalog", catalog)

	snapshotSchema, err := marshalParquetSnapshotSchema(ctx, snapshotSchemaData{
		TenantID:  "tenant-a",
		Version:   4,
		UpdatedAt: now,
		CITypes: []graph.CIType{{
			Name:        "host",
			DisplayName: "Host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true, Required: true},
				"region":   {Type: "string", Enum: []any{"r1", "r2"}, Default: "r1"},
			},
			IdentityKeys: []graph.IdentityKey{{
				Name:                "hostname",
				Fields:              []string{"hostname"},
				Strategy:            "exact",
				ConfidenceThreshold: 0.9,
			}},
		}},
		RelationTypes: []graph.RelationType{{
			Name:            "runs_on",
			DisplayName:     "Runs On",
			ReverseName:     "hosts",
			FromKind:        "service",
			ToKind:          "host",
			Directed:        true,
			Cardinality:     graph.ManyToMany,
			ImpactDirection: "forward",
		}},
	})
	if err != nil {
		t.Fatalf("marshal snapshot schema: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "snapshot schema", snapshotSchema)
	decodedSnapshotSchema, err := decodeParquetSnapshotSchema(ctx, snapshotSchema)
	if err != nil {
		t.Fatalf("decode snapshot schema: %v", err)
	}
	if len(decodedSnapshotSchema.CITypes) != 1 || decodedSnapshotSchema.CITypes[0].Name != "host" ||
		len(decodedSnapshotSchema.RelationTypes) != 1 || decodedSnapshotSchema.RelationTypes[0].Name != "runs_on" {
		t.Fatalf("decoded snapshot schema = %#v", decodedSnapshotSchema)
	}

	snapshotCatalog, err := marshalParquetShardedSnapshotCatalog(ctx, ShardedSnapshotCatalog{
		TenantID: "tenant-a",
		Key:      "test/tenants/tenant-a/snapshots/sharded/v4/catalog.parquet",
		Version:  4,
		Format:   snapshotFormatParquetSharded,
		Schema: SnapshotSchemaSpec{
			Key:         "test/tenants/tenant-a/snapshots/sharded/v4/schema.parquet",
			Format:      snapshotSchemaFormatParquet,
			ContentHash: "schema-hash",
		},
		UpdatedAt: now,
		EntityPages: []SnapshotEntityPageSpec{{
			Shard:       "ab",
			Key:         "test/tenants/tenant-a/snapshots/sharded/v4/entities/ab.parquet",
			Format:      IndexFormatParquet,
			EntityCount: 2,
			ContentHash: "entity-page-hash",
		}},
		EdgeShards: []SnapshotEdgeShardSpec{{
			RelationType: "runs_on",
			Shard:        "cd",
			Key:          "test/tenants/tenant-a/snapshots/sharded/v4/edges/runs_on/cd.parquet",
			Format:       IndexFormatParquet,
			EdgeCount:    1,
			ContentHash:  "edge-shard-hash",
		}},
	})
	if err != nil {
		t.Fatalf("marshal snapshot catalog: %v", err)
	}
	assertNoJSONPayloadColumns(t, ctx, "snapshot catalog", snapshotCatalog)
}

func assertNoJSONPayloadColumns(t *testing.T, ctx context.Context, label string, data []byte) {
	t.Helper()
	table, err := pqarrow.ReadTable(ctx, bytes.NewReader(data), nil, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("read %s parquet table: %v", label, err)
	}
	defer table.Release()
	for _, field := range table.Schema().Fields() {
		if strings.HasSuffix(field.Name, "_json") || field.Name == "payload_json" {
			t.Fatalf("%s parquet schema contains JSON payload column %q", label, field.Name)
		}
	}
}

func seedIndexedGraph(t *testing.T, ctx context.Context, store *TenantStore) {
	t.Helper()
	mutations := graph.Mutations{
		UpsertCITypes: []graph.CIType{{
			Name: "host",
			Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Indexed: true},
				"region":   {Type: "string", Indexed: true},
				"cpu":      {Type: "number"},
			},
		}, {
			Name:   "service",
			Fields: map[string]graph.FieldSpec{"name": {Type: "string", Indexed: true}},
		}},
		UpsertRelationTypes: []graph.RelationType{{
			Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true,
		}},
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "region": "r1", "cpu": float64(4)}},
			{ID: "host:app-02", Kind: "host", Fields: graph.Fields{"hostname": "app-02", "region": "r1", "cpu": float64(8)}},
			{ID: "service:api", Kind: "service", Fields: graph.Fields{"name": "api"}},
		},
		UpsertEdges: []graph.Edge{{
			ID: "collector-edge-1", Type: "runs_on", From: "service:api", To: "host:app-01",
		}},
	}
	if _, err := store.Commit(ctx, "tenant-a", mutations, CommitOptions{}); err != nil {
		t.Fatalf("seed graph: %v", err)
	}
}

func assertCatalogParquet(t *testing.T, catalog IndexCatalog) {
	t.Helper()
	if len(catalog.Indexes) == 0 || len(catalog.EntityPages) == 0 || len(catalog.EdgeShards) == 0 {
		t.Fatalf("catalog missing sections: %#v", catalog)
	}
	for _, index := range catalog.Indexes {
		if index.Format != IndexFormatParquet || index.Codec != parquetSecondaryIndexCodec || len(index.Objects) == 0 || requireAnyIndexObjectKey(t, index.Objects) == "" {
			t.Fatalf("field index should be parquet-ready: %#v", index)
		}
		for _, object := range index.Objects {
			if object.Format != IndexFormatParquet || object.Codec != parquetSecondaryIndexCodec || object.SchemaHash == "" || object.ContentHash == "" {
				t.Fatalf("field index object should be parquet-ready: %#v", index)
			}
		}
	}
	for _, page := range catalog.EntityPages {
		if page.Format != IndexFormatParquet || page.Codec != parquetEntityPageCodec || len(page.Objects) != 1 || page.SchemaHash == "" {
			t.Fatalf("page spec not parquet-ready: %#v", page)
		}
	}
	for _, shard := range catalog.EdgeShards {
		if shard.Format != IndexFormatParquet || shard.Codec != parquetEdgeShardCodec || len(shard.Objects) != 1 || shard.Objects[0].Format != IndexFormatParquet {
			t.Fatalf("edge shard should be parquet-ready: %#v", shard)
		}
	}
}
