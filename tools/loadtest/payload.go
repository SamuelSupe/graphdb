package main

import (
	"fmt"

	"graphdb/internal/graph"
	"graphdb/internal/query"
	"graphdb/internal/storage"
)

func schemaMutations() graph.Mutations {
	return graph.Mutations{
		UpsertCITypes: []graph.CIType{
			{
				Name: "host",
				Fields: map[string]graph.FieldSpec{
					"hostname": {Type: "string", Required: true, Unique: true, Indexed: true},
					"region":   {Type: "string", Indexed: true},
				},
			},
			{Name: "service", Fields: map[string]graph.FieldSpec{"name": {Type: "string", Required: true, Unique: true, Indexed: true}}},
		},
		UpsertEntities: []graph.Entity{
			{ID: "host:seed", Kind: "host", Fields: graph.Fields{"hostname": "seed-host", "region": "region-0"}},
			{ID: "service:seed", Kind: "service", Fields: graph.Fields{"name": "seed-service"}},
		},
		UpsertEdges: []graph.Edge{{ID: "edge:seed-runs-on", Type: "runs_on", From: "service:seed", To: "host:seed"}},
	}
}

func batchRequest(batch int, batchSize int) storage.IngestRequest {
	items := make([]storage.IngestItem, 0, batchSize*3)
	for i := 0; i < batchSize; i++ {
		n := batch*batchSize + i
		hostID := fmt.Sprintf("host:%06d", n)
		serviceID := fmt.Sprintf("service:%06d", n)
		items = append(items,
			storage.IngestItem{ExternalID: hostID, Entity: &graph.Entity{
				ID: hostID, Kind: "host", Confidence: 0.9, SourceRank: 10,
				Fields: graph.Fields{"hostname": fmt.Sprintf("app-%06d", n), "region": regionName(n % 8)},
			}},
			storage.IngestItem{ExternalID: serviceID, Entity: &graph.Entity{
				ID: serviceID, Kind: "service", Confidence: 0.9, SourceRank: 10,
				Fields: graph.Fields{"name": fmt.Sprintf("svc-%06d", n)},
			}},
			storage.IngestItem{ExternalID: "edge-" + serviceID, Edge: &graph.Edge{
				ID: fmt.Sprintf("edge:%06d", n), Type: "runs_on", From: serviceID, To: hostID,
			}},
		)
	}
	return storage.IngestRequest{
		Source:         "loadtest",
		CollectorID:    "collector-a",
		BatchID:        fmt.Sprintf("batch-%06d", batch),
		IdempotencyKey: fmt.Sprintf("batch-%06d", batch),
		Cursor:         fmt.Sprintf("cursor-%06d", batch),
		Items:          items,
	}
}

func matchQuery(region string) query.Request {
	return query.Request{
		Op:       "profile",
		TargetOp: "match",
		Kind:     "host",
		Where:    []query.Filter{{Field: "region", Op: "eq", Value: region}},
		Sort:     []query.SortSpec{{Field: "hostname"}},
		Project:  []string{"id", "hostname", "region"},
		Limit:    20,
	}
}

func traverseQuery(batch int) query.Request {
	return query.Request{Op: "traverse", ID: "service:seed", Direction: "out", RelationType: "runs_on", Depth: 1, Limit: 5}
}

func regionName(value int) string {
	return fmt.Sprintf("region-%d", value)
}
