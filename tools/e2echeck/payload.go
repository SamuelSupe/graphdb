package main

import (
	"fmt"

	"graphdb/internal/graph"
	"graphdb/internal/query"
	"graphdb/internal/storage"
)

type ids struct {
	host1   string
	host2   string
	service string
	db      string
}

func testIDs(tenant string) ids {
	return ids{
		host1:   "host:" + tenant + ":001",
		host2:   "host:" + tenant + ":002",
		service: "service:" + tenant + ":api",
		db:      "db:" + tenant + ":main",
	}
}

func sourcePolicy() graph.SourcePolicy {
	return graph.SourcePolicy{
		DefaultPriority: 0,
		Sources: []graph.SourcePolicyItem{
			{Name: "manual", Priority: 1000},
			{Name: "agent", Priority: 100},
			{Name: "cloud", Priority: 50},
		},
	}
}

func seedMutations(id ids) graph.Mutations {
	return graph.Mutations{
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
			{ID: id.host1, Kind: "host", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"hostname": id.host1, "region": "r0"}},
			{ID: id.host2, Kind: "host", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"hostname": id.host2, "region": "r0"}},
			{ID: id.service, Kind: "service", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"name": "api"}},
			{ID: id.db, Kind: "database", Source: "agent", SourceRank: 100, Confidence: 0.9, Fields: graph.Fields{"name": "main"}},
		},
		UpsertEdges: []graph.Edge{
			{ID: "edge:" + id.service + ":runs-on", Type: "runs_on", From: id.service, To: id.host1, Source: "agent", SourceRank: 100},
			{ID: "edge:" + id.service + ":depends-on", Type: "depends_on", From: id.service, To: id.db, Source: "agent", SourceRank: 100},
		},
	}
}

func manualRunsOnEdge(id ids) graph.Mutations {
	return graph.Mutations{UpsertEdges: []graph.Edge{{
		ID: "manual-" + id.service + "-runs-on", Type: "runs_on", From: id.service, To: id.host1,
		Source: "manual", Confidence: 0.9, Fields: graph.Fields{"note": "manual"},
	}}}
}

func agentRunsOnEdge(id ids) graph.Mutations {
	return graph.Mutations{UpsertEdges: []graph.Edge{{
		ID: "agent-" + id.service + "-runs-on", Type: "runs_on", From: id.service, To: id.host1,
		Source: "agent", Confidence: 1, Fields: graph.Fields{"note": "agent"},
	}}}
}

func agentDeleteRunsOnEdge(id ids) storage.IngestRequest {
	return storage.IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "edge-delete-001",
		IdempotencyKey: "edge-delete-001",
		Items: []storage.IngestItem{{
			ExternalID: "manual-" + id.service + "-runs-on",
			DeleteEdge: &graph.EdgeDeleteRequest{
				ID:     "manual-" + id.service + "-runs-on",
				Reason: "collector no longer observes relation",
			},
		}},
	}
}

func updateRegionMutation(id string, source string, region string) graph.Mutations {
	priority := 100
	if source == "manual" {
		priority = 1000
	}
	return graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: id, Kind: "host", Source: source, SourceRank: priority, Confidence: 0.9,
		Fields: graph.Fields{"hostname": id, "region": region},
	}}}
}

func bulkIngest(tenant string, count int) storage.IngestRequest {
	items := make([]storage.IngestItem, 0, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("host:%s:bulk:%03d", tenant, i)
		items = append(items, storage.IngestItem{
			ExternalID: id,
			Entity: &graph.Entity{
				ID: id, Kind: "host", Source: "agent", SourceRank: 100, Confidence: 0.9,
				Fields: graph.Fields{"hostname": fmt.Sprintf("bulk-%03d", i), "region": "bulk-r0"},
			},
		})
	}
	return storage.IngestRequest{
		Source:         "agent",
		CollectorID:    "collector-a",
		BatchID:        "bulk-001",
		IdempotencyKey: "bulk-001",
		Cursor:         "cursor-001",
		Items:          items,
	}
}

func invalidIngest() storage.IngestRequest {
	return storage.IngestRequest{
		Source:         "badsrc",
		CollectorID:    "collector-a",
		BatchID:        "bad-001",
		IdempotencyKey: "bad-001",
		Items:          []storage.IngestItem{{ExternalID: "bad-item"}},
	}
}

func matchHosts(region string, limit int, sort bool) query.Request {
	request := query.Request{
		Op:       "profile",
		TargetOp: "match",
		Kind:     "host",
		Where:    []query.Filter{{Field: "region", Op: "eq", Value: region}},
		Project:  []string{"id", "hostname", "region"},
		Limit:    limit,
	}
	if sort {
		request.Sort = []query.SortSpec{{Field: "hostname"}}
	}
	return request
}
