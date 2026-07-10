package main

import (
	"fmt"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func schemaMutations() graph.Mutations {
	return graph.Mutations{
		UpsertCITypes: []graph.CIType{
			{Name: "environment", Fields: map[string]graph.FieldSpec{
				"name": {Type: "string", Required: true, Unique: true, Indexed: true},
			}},
			{Name: "cluster", Fields: map[string]graph.FieldSpec{
				"name":   {Type: "string", Required: true, Unique: true, Indexed: true},
				"region": {Type: "string", Indexed: true},
				"env":    {Type: "string", Indexed: true},
			}},
			{Name: "host", Fields: map[string]graph.FieldSpec{
				"hostname": {Type: "string", Required: true, Unique: true, Indexed: true},
				"region":   {Type: "string", Indexed: true},
				"env":      {Type: "string", Indexed: true},
				"app":      {Type: "string", Indexed: true},
				"owner":    {Type: "string", Indexed: true},
				"cpu":      {Type: "number", Indexed: true},
			}},
			{Name: "service", Fields: map[string]graph.FieldSpec{
				"name":   {Type: "string", Required: true, Unique: true, Indexed: true},
				"region": {Type: "string", Indexed: true},
				"env":    {Type: "string", Indexed: true},
				"app":    {Type: "string", Indexed: true},
				"tier":   {Type: "string", Indexed: true},
				"owner":  {Type: "string", Indexed: true},
			}},
			{Name: "database", Fields: map[string]graph.FieldSpec{
				"name":   {Type: "string", Required: true, Unique: true, Indexed: true},
				"engine": {Type: "string", Indexed: true},
				"region": {Type: "string", Indexed: true},
				"env":    {Type: "string", Indexed: true},
			}},
		},
		UpsertEntities: seedEntities(),
		UpsertEdges:    seedEdges(),
	}
}

func seedEntities() []graph.Entity {
	group := cmdbGroup(0)
	return group.entities
}

func seedEdges() []graph.Edge {
	group := cmdbGroup(0)
	return group.edges
}

type generatedGroup struct {
	entities []graph.Entity
	edges    []graph.Edge
}

func batchRequest(batch int64, batchSize int) storage.IngestRequest {
	items := make([]storage.IngestItem, 0, batchSize*9)
	for i := 0; i < batchSize; i++ {
		group := cmdbGroup(batch*int64(batchSize) + int64(i))
		for _, entity := range group.entities {
			copy := entity
			items = append(items, storage.IngestItem{ExternalID: entity.ExternalID, Entity: &copy})
		}
		for _, edge := range group.edges {
			copy := edge
			items = append(items, storage.IngestItem{ExternalID: edge.ExternalID, Edge: &copy})
		}
	}
	return storage.IngestRequest{
		Source:         "soak-agent",
		CollectorID:    "soak-collector",
		BatchID:        fmt.Sprintf("soak-batch-%012d", batch),
		IdempotencyKey: fmt.Sprintf("soak-batch-%012d", batch),
		Cursor:         fmt.Sprintf("cursor-%012d", batch),
		Items:          items,
	}
}

func cmdbGroup(n int64) generatedGroup {
	region := regionName(int(n % 8))
	env := envName(int(n % 3))
	app := fmt.Sprintf("app-%03d", n%500)
	owner := fmt.Sprintf("team-%02d", n%32)
	clusterID := clusterID(n)
	envID := "environment:" + env
	hostID := hostID(n)
	svcID := serviceID(n)
	dbID := databaseID(n)
	anchorService := serviceID(0)
	cpu := 2 + int(n%32)
	entities := []graph.Entity{
		sourceEntity(envID, "environment", graph.Fields{"name": env}),
		sourceEntity(clusterID, "cluster", graph.Fields{
			"name": fmt.Sprintf("%s-%s-%02d", env, region, n%64), "region": region, "env": env,
		}),
		sourceEntity(hostID, "host", graph.Fields{
			"hostname": fmt.Sprintf("host-%012d", n), "region": region, "env": env,
			"app": app, "owner": owner, "cpu": cpu,
		}),
		sourceEntity(svcID, "service", graph.Fields{
			"name": fmt.Sprintf("svc-%012d", n), "region": region, "env": env,
			"app": app, "tier": tierName(int(n % 4)), "owner": owner,
		}),
		sourceEntity(dbID, "database", graph.Fields{
			"name": fmt.Sprintf("db-%012d", n), "engine": engineName(int(n % 3)),
			"region": region, "env": env,
		}),
	}
	edges := []graph.Edge{
		sourceEdge("contains", envID, clusterID, n, "env-cluster"),
		sourceEdge("contains", clusterID, hostID, n, "cluster-host"),
		sourceEdge("runs_on", svcID, hostID, n, "service-host"),
		sourceEdge("depends_on", svcID, dbID, n, "service-db"),
		sourceEdge("connects_to", hostID, dbID, n, "host-db"),
	}
	if n > 0 {
		edges = append(edges, sourceEdge("depends_on", svcID, anchorService, n, "service-service"))
	}
	return generatedGroup{entities: entities, edges: edges}
}

func sourceEntity(id string, kind string, fields graph.Fields) graph.Entity {
	return graph.Entity{
		ID: id, Kind: kind, Source: "soak-agent", ExternalID: id,
		Confidence: 0.95, SourceRank: 100, Fields: fields,
	}
}

func sourceEdge(edgeType string, from string, to string, n int64, suffix string) graph.Edge {
	id := fmt.Sprintf("edge:%s:%012d:%s", edgeType, n, suffix)
	return graph.Edge{
		ID: id, Type: edgeType, From: from, To: to, Source: "soak-agent",
		ExternalID: id, Confidence: 0.95, SourceRank: 100,
		Fields: graph.Fields{"collector": "soak", "ordinal": n},
	}
}

func hostID(n int64) string {
	return fmt.Sprintf("host:%012d", maxInt64(n, 0))
}

func serviceID(n int64) string {
	return fmt.Sprintf("service:%012d", maxInt64(n, 0))
}

func databaseID(n int64) string {
	return fmt.Sprintf("database:%012d", maxInt64(n, 0))
}

func clusterID(n int64) string {
	return fmt.Sprintf("cluster:%02d", maxInt64(n, 0)%64)
}

func regionName(value int) string {
	return fmt.Sprintf("region-%d", value)
}

func envName(value int) string {
	return []string{"prod", "staging", "dev"}[value%3]
}

func tierName(value int) string {
	return []string{"frontend", "api", "worker", "data"}[value%4]
}

func engineName(value int) string {
	return []string{"postgres", "mysql", "redis"}[value%3]
}

func maxInt64(value int64, minimum int64) int64 {
	if value < minimum {
		return minimum
	}
	return value
}
