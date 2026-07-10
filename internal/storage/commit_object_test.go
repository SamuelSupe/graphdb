package storage

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestCommitObjectParquetRoundTrip(t *testing.T) {
	commit := graph.Commit{
		ID:        "commit-a",
		TenantID:  "tenant-a",
		Version:   7,
		CreatedAt: time.Now().UTC(),
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
			ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
		}}},
	}
	data, err := marshalCommitObject(commit)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !isParquetBytes(data) {
		t.Fatal("commit object is not parquet")
	}
	decoded, err := unmarshalCommitObject(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.ID != commit.ID || decoded.TenantID != commit.TenantID || decoded.Version != commit.Version {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestCommitObjectParquetRoundTripAllMutationKinds(t *testing.T) {
	now := time.Date(2026, 6, 25, 10, 11, 12, 13, time.UTC)
	commit := graph.Commit{
		ID:        "commit-all",
		TenantID:  "tenant-a",
		Version:   8,
		CreatedAt: now,
		Mutations: graph.Mutations{
			UpsertCITypes: []graph.CIType{{
				Name:        "host",
				DisplayName: "Host",
				Extends:     []string{"ci"},
				Fields: map[string]graph.FieldSpec{
					"hostname": {Type: "string", Required: true, Indexed: true, Unique: true, Enum: []any{"app-01", "app-02"}, Default: "app-01"},
					"cpu":      {Type: "number"},
					"tags":     {Type: "array", MergeStrategy: graph.FieldMergeAppendUnique},
				},
				IdentityKeys: []graph.IdentityKey{{Name: "host_identity", Fields: []string{"hostname"}, ConfidenceThreshold: 0.9, Strategy: "first"}},
			}},
			DeleteCITypes: []string{"legacy_ci"},
			UpsertRelationTypes: []graph.RelationType{{
				Name: "runs_on", DisplayName: "Runs on", ReverseName: "hosts", FromKind: "service", ToKind: "host",
				FromKinds: []string{"service", "job"}, ToKinds: []string{"host"}, Directed: true, Cardinality: graph.ManyToMany,
				ImpactDirection: "out", AllowCrossKind: true, Standard: true,
			}},
			DeleteRelationTypes: []string{"legacy_relation"},
			UpsertEntities: []graph.Entity{{
				ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01", "cpu": float64(4), "tags": []any{"blue"}}, Source: "agent", ExternalID: "i-a",
				Confidence: 0.8, SourceRank: 100, Version: 8, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
				FieldSources:    map[string]graph.FieldSource{"hostname": {Source: "agent", Priority: 100, Version: 8, UpdatedAt: now}},
				FieldWriteModes: map[string]string{"tags": graph.FieldMergeReplace},
				Sources:         []graph.EntitySource{{Source: "agent", ExternalID: "i-a", Confidence: 0.8, Priority: 100, ObservedAt: now}},
				MergedFrom:      []string{"host:old-a"}, SplitFrom: "host:split",
			}},
			DeleteEntities: []string{"host:deleted"},
			DeleteEntityRequests: []graph.EntityDeleteRequest{{
				ID: "host:low", Kind: "host", Source: "agent", ExternalID: "i-low", SourceRank: 100, Confidence: 0.4, Reason: "missing",
			}},
			MarkSourceStale: []graph.SourceStaleRequest{{
				Source: "agent", Kind: "host", ObservedExternalIDs: []string{"i-a", "i-b"}, Action: "delete", SourceRank: 100, Confidence: 0.5, Reason: "full sync",
			}},
			UpsertEdges: []graph.Edge{{
				ID: "edge:a", Type: "runs_on", From: "service:api", To: "host:a", Fields: graph.Fields{"port": float64(443)}, Source: "agent", ExternalID: "rel-a",
				Confidence: 0.7, SourceRank: 100, Version: 8, CreatedAt: now.Add(-time.Minute), UpdatedAt: now,
				FieldSources:    map[string]graph.FieldSource{"port": {Source: "agent", Priority: 100, Version: 8, UpdatedAt: now}},
				Sources:         []graph.EdgeSource{{Source: "agent", ExternalID: "rel-a", EdgeID: "collector-edge-a", Confidence: 0.7, Priority: 100, ObservedAt: now}},
				ExistenceSource: &graph.FieldSource{Source: "agent", Priority: 100, Version: 8, UpdatedAt: now},
			}},
			DeleteEdges: []string{"edge:deleted"},
			DeleteEdgeRequests: []graph.EdgeDeleteRequest{{
				Type: "runs_on", From: "service:api", To: "host:old", Source: "agent", SourceRank: 100, Confidence: 0.6, Reason: "gone",
			}},
			MergeEntities: []graph.MergeRequest{{TargetID: "host:a", SourceIDs: []string{"host:dup-a", "host:dup-b"}}},
			SplitEntities: []graph.SplitRequest{{
				SourceID: "host:compound",
				Entities: []graph.Entity{{ID: "host:part-a", Kind: "host", Fields: graph.Fields{"hostname": "part-a"}, Source: "manual", SourceRank: 1000, Version: 8, UpdatedAt: now}},
			}},
		},
	}
	normalized, _, err := normalizeCommitForParquet(commit)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	data, err := marshalCommitObject(commit)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	decoded, err := unmarshalCommitObject(data)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(decoded, normalized) {
		t.Fatalf("decoded mismatch\n got: %#v\nwant: %#v", decoded, normalized)
	}
}

func TestCommitObjectRejectsNonParquetJSON(t *testing.T) {
	commit := graph.Commit{
		ID:        "json",
		TenantID:  "tenant-a",
		Version:   1,
		Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:a", Kind: "host"}}},
	}
	data, err := marshalNonParquetJSON(commit)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	if _, err := unmarshalCommitObject(data); err == nil || !strings.Contains(err.Error(), "only parquet commits") {
		t.Fatalf("decode err = %v, want parquet-only rejection", err)
	}
}

func TestTenantStoreWritesCommitEnvelope(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"hostname": "app-01"},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if len(result.CommitKeys) != 1 {
		t.Fatalf("commit keys = %#v", result.CommitKeys)
	}
	data, err := store.Objects.Get(ctx, result.CommitKeys[0])
	if err != nil {
		t.Fatalf("get commit object: %v", err)
	}
	if !strings.HasSuffix(result.CommitKeys[0], ".parquet") || !isParquetBytes(data) {
		t.Fatalf("commit key/object = %q parquet=%v", result.CommitKeys[0], isParquetBytes(data))
	}
	commit, err := unmarshalCommitObject(data)
	if err != nil {
		t.Fatalf("decode commit object: %v", err)
	}
	if commit.ID != result.HeadCommitID {
		t.Fatalf("commit = %#v result = %#v", commit, result)
	}
	store.deleteWriteCache("tenant-a")
	loaded, manifest, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if manifest.Version != result.Version {
		t.Fatalf("manifest version = %d want %d", manifest.Version, result.Version)
	}
	if _, ok := loaded.GetEntity("host:a"); !ok {
		t.Fatal("host:a missing after envelope load")
	}
}

func TestTenantStoreArrayMergeMarkerSurvivesCommitReload(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{
		UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{
			"tags": {Type: "array", MergeStrategy: graph.FieldMergeAppendUnique},
		}}},
		UpsertEntities: []graph.Entity{{
			ID: "host:a", Kind: "host", Fields: graph.Fields{"tags": []any{"abc", "bcd"}},
		}},
	}, CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"tags": []any{"bcd", "aaa"}},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("reload after append: %v", err)
	}
	entity, ok := loaded.GetEntity("host:a")
	if !ok {
		t.Fatal("host:a missing after append reload")
	}
	if got := entity.Fields["tags"]; !reflect.DeepEqual(got, []any{"abc", "bcd", "aaa"}) {
		t.Fatalf("tags after append reload = %#v", got)
	}
	if _, ok := entity.Fields["tags!"]; ok {
		t.Fatalf("override marker leaked after append reload: %#v", entity.Fields)
	}
	if _, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:a", Kind: "host", Fields: graph.Fields{"tags!": []any{"forced"}},
	}}}, CommitOptions{}); err != nil {
		t.Fatalf("replace commit: %v", err)
	}
	store.deleteWriteCache("tenant-a")
	loaded, _, err = store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("reload after replace: %v", err)
	}
	entity, ok = loaded.GetEntity("host:a")
	if !ok {
		t.Fatal("host:a missing after replace reload")
	}
	if got := entity.Fields["tags"]; !reflect.DeepEqual(got, []any{"forced"}) {
		t.Fatalf("tags after replace reload = %#v", got)
	}
	if _, ok := entity.Fields["tags!"]; ok {
		t.Fatalf("override marker leaked after replace reload: %#v", entity.Fields)
	}
}

func TestTenantStoreWritesLargeCommitAsParquet(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	large := strings.Repeat("cmdb-large-field-", 4<<10)
	result, err := store.CommitWithReport(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: "host:large", Kind: "host", Fields: graph.Fields{"description": large},
	}}}, CommitOptions{})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	data, err := store.Objects.Get(ctx, result.CommitKeys[0])
	if err != nil {
		t.Fatalf("get commit object: %v", err)
	}
	if !strings.HasSuffix(result.CommitKeys[0], ".parquet") || !isParquetBytes(data) {
		t.Fatalf("commit key/object = %q parquet=%v", result.CommitKeys[0], isParquetBytes(data))
	}
	store.deleteWriteCache("tenant-a")
	loaded, _, err := store.Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	entity, ok := loaded.GetEntity("host:large")
	if !ok {
		t.Fatal("host:large missing")
	}
	if got := entity.Fields["description"]; got != large {
		t.Fatalf("description mismatch")
	}
}

func TestTenantStoreLoadE2ECommitSequenceFromObjects(t *testing.T) {
	ctx := context.Background()
	store := NewTenantStore(NewMemoryStore(), "test")
	tenantID := "tenant-e2e"
	host1 := "host:" + tenantID + ":001"
	host2 := "host:" + tenantID + ":002"
	service := "service:" + tenantID + ":api"
	db := "db:" + tenantID + ":main"

	if _, err := store.PutSourcePolicy(ctx, tenantID, graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
			{Name: "cloud", Priority: 50},
		},
	}); err != nil {
		t.Fatalf("source policy: %v", err)
	}
	commit := func(name string, mutations graph.Mutations) {
		t.Helper()
		if _, err := store.CommitWithReport(ctx, tenantID, mutations, CommitOptions{}); err != nil {
			t.Fatalf("%s commit: %v", name, err)
		}
		store.deleteWriteCache(tenantID)
		if _, _, err := store.Load(ctx, tenantID); err != nil {
			t.Fatalf("%s reload: %v", name, err)
		}
	}

	commit("seed", graph.Mutations{
		UpsertCITypes: []graph.CIType{
			{Name: "host", Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Required: true, Unique: true, Indexed: true},
				"region":   {Type: "string", Indexed: true},
			}},
			{Name: "service", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Required: true, Indexed: true}}},
			{Name: "database", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Required: true, Indexed: true}}},
		},
		UpsertRelationTypes: []graph.RelationType{
			{Name: "runs_on", FromKind: "service", ToKind: "host", Directed: true, Cardinality: graph.ManyToOne, ImpactDirection: "forward"},
			{Name: "depends_on", FromKind: "service", ToKind: "database", Directed: true, Cardinality: graph.ManyToMany, ImpactDirection: "forward"},
		},
		UpsertEntities: []graph.Entity{
			{ID: host1, Kind: "host", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"hostname": host1, "region": "r0"}},
			{ID: host2, Kind: "host", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"hostname": host2, "region": "r0"}},
			{ID: service, Kind: "service", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"name": "api"}},
			{ID: db, Kind: "database", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"name": "main"}},
		},
		UpsertEdges: []graph.Edge{
			{ID: "edge:" + service + ":runs-on", Type: "runs_on", From: service, To: host1, Source: "agent", SourceRank: 100},
			{ID: "edge:" + service + ":depends-on", Type: "depends_on", From: service, To: db, Source: "agent", SourceRank: 100},
		},
	})
	commit("manual-region", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: host1, Kind: "host", Source: "manual", SourceRank: 1000, Confidence: 0.9,
		Fields: graph.Fields{"hostname": host1, "region": "manual-r0"},
	}}})
	commit("suppressed-region", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: host1, Kind: "host", Source: "agent", SourceRank: 100, Confidence: 0.9,
		Fields: graph.Fields{"hostname": host1, "region": "agent-r0"},
	}}})
	manualExternalID := "manual-asset-" + tenantID
	manualID := graph.CanonicalEntityIDParts("host", "manual", manualExternalID)
	commit("canonical-entity", graph.Mutations{UpsertEntities: []graph.Entity{{
		Kind: "host", Source: "manual", ExternalID: manualExternalID,
		Fields: graph.Fields{"hostname": manualID, "region": "manual-entity"},
	}}})
	commit("suppressed-entity-delete", graph.Mutations{DeleteEntityRequests: []graph.EntityDeleteRequest{{
		ID: manualID, Source: "agent",
	}}})
	if _, err := store.Ingest(ctx, tenantID, IngestRequest{
		Source:      "cloud",
		CollectorID: "collector-stale",
		BatchID:     "stale-seed",
		Items: []IngestItem{
			{ExternalID: "stale-a-" + tenantID, Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "stale-a-" + tenantID}}},
			{ExternalID: "stale-b-" + tenantID, Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "stale-b-" + tenantID}}},
		},
	}); err != nil {
		t.Fatalf("stale seed ingest: %v", err)
	}
	store.deleteWriteCache(tenantID)
	if _, _, err := store.Load(ctx, tenantID); err != nil {
		t.Fatalf("stale seed reload: %v", err)
	}
	if _, err := store.Ingest(ctx, tenantID, IngestRequest{
		Source:      "cloud",
		CollectorID: "collector-stale",
		BatchID:     "stale-full",
		FullSync:    true,
		Items: []IngestItem{{
			ExternalID: "stale-a-" + tenantID,
			Entity:     &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "stale-a-" + tenantID}},
		}},
	}); err != nil {
		t.Fatalf("stale full ingest: %v", err)
	}
	store.deleteWriteCache(tenantID)
	if _, _, err := store.Load(ctx, tenantID); err != nil {
		t.Fatalf("stale full reload: %v", err)
	}
	commit("manual-edge", graph.Mutations{UpsertEdges: []graph.Edge{{
		ID: "manual-" + service + "-runs-on", Type: "runs_on", From: service, To: host1,
		Source: "manual", Confidence: 0.9, Fields: graph.Fields{"note": "manual"},
	}}})
	commit("suppressed-edge", graph.Mutations{UpsertEdges: []graph.Edge{{
		ID: "agent-" + service + "-runs-on", Type: "runs_on", From: service, To: host1,
		Source: "agent", Confidence: 1, Fields: graph.Fields{"note": "agent"},
	}}})
	if _, err := store.Ingest(ctx, tenantID, IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "edge-delete-001",
		IdempotencyKey: "edge-delete-001",
		Items: []IngestItem{{
			ExternalID: "manual-" + service + "-runs-on",
			DeleteEdge: &graph.EdgeDeleteRequest{
				ID:     "manual-" + service + "-runs-on",
				Reason: "collector no longer observes relation",
			},
		}},
	}); err != nil {
		t.Fatalf("suppressed edge delete ingest: %v", err)
	}
	items := make([]IngestItem, 0, 40)
	for i := 0; i < 40; i++ {
		id := fmt.Sprintf("host:%s:bulk:%03d", tenantID, i)
		items = append(items, IngestItem{
			ExternalID: id,
			Entity: &graph.Entity{
				ID: id, Kind: "host", Source: "agent", SourceRank: 100, Confidence: 0.9,
				Fields: graph.Fields{"hostname": fmt.Sprintf("bulk-%03d", i), "region": "bulk-r0"},
			},
		})
	}
	if _, err := store.Ingest(ctx, tenantID, IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "bulk-001",
		IdempotencyKey: "bulk-001",
		Cursor:         "cursor-001",
		Items:          items,
	}); err != nil {
		t.Fatalf("bulk ingest: %v", err)
	}
	store.deleteWriteCache(tenantID)
	loaded, manifest, err := store.Load(ctx, tenantID)
	if err != nil {
		t.Fatalf("final reload: %v", err)
	}
	if manifest.Version != loaded.Version {
		t.Fatalf("manifest version = %d graph = %d", manifest.Version, loaded.Version)
	}
}
