package main

import (
	"fmt"
	"time"

	"graphdb/internal/query"
)

type queryCase struct {
	name        string
	request     query.Request
	stream      bool
	saved       string
	scan        string
	expectPlan  string
	timeout     time.Duration
	minWrittenN int64
	minVersion  int64
	allowStale  bool
}

func savedQueries() map[string]query.Request {
	return map[string]query.Request{
		"soak-hosts-by-region": indexedHostMatch("region-0", 100),
		"soak-prod-services": {
			Op:   "match",
			Kind: "service",
			Where: []query.Filter{
				{Field: "env", Op: "eq", Value: "prod"},
				{Field: "tier", Op: "in", Value: []any{"api", "worker"}},
			},
			Sort:    []query.SortSpec{{Field: "name"}},
			Project: []string{"id", "name", "env", "tier", "owner"},
			Limit:   100,
		},
		"soak-team-services": {
			Op:   "match",
			Kind: "service",
			Where: []query.Filter{
				{Field: "owner", Op: "eq", Value: "team-00"},
				{Field: "env", Op: "in", Value: []any{"prod", "staging"}},
			},
			Project: []string{"id", "name", "env", "app", "tier", "owner"},
			Limit:   250,
		},
		"soak-service-impact": {
			Op: "impact", ID: serviceID(0), Direction: "out",
			RelationTypes: []string{"runs_on", "depends_on", "connects_to"},
			Depth:         3,
			Project:       []string{"id", "kind", "name", "hostname", "engine"},
			Limit:         100,
			TimeoutMS:     5000,
			CostLimit:     500000,
		},
		"soak-prod-containment": {
			Op: "traverse", ID: "environment:prod", Direction: "out",
			RelationTypes: []string{"contains"},
			Depth:         2,
			Path:          query.PathFilter{NodeKinds: []string{"cluster", "host"}, MaxPaths: 500},
			Project:       []string{"id", "kind", "name", "hostname", "region", "env"},
			Limit:         500,
			TimeoutMS:     5000,
			CostLimit:     500000,
		},
	}
}

func queryCases(maxWritten int64, round int64, latestVersion int64) []queryCase {
	n := maxInt64(maxWritten, 0)
	if n > 0 {
		n = round % (n + 1)
	}
	stable := int64(0)
	region := regionName(int(round % 8))
	env := envName(int(round % 3))
	return []queryCase{
		{
			name:       "profile-indexed-match",
			request:    profile(indexedHostMatch(region, 100)),
			expectPlan: "field-index",
		},
		{
			name:       "indexed-match-min-version",
			request:    indexedHostMatch(region, 100),
			minVersion: latestVersion,
		},
		{
			name:       "indexed-match-allow-stale",
			request:    indexedHostMatch(region, 100),
			allowStale: true,
		},
		{
			name: "range-aggregate-match",
			request: query.Request{
				Op:   "match",
				Kind: "host",
				Where: []query.Filter{
					{Field: "cpu", Op: "gte", Value: 8},
					{Field: "env", Op: "in", Value: []any{"prod", "staging"}},
					{Field: "owner", Op: "exists", Value: true},
					{Field: "hostname", Op: "prefix", Value: "host-"},
				},
				Sort:    []query.SortSpec{{Field: "cpu", Desc: true}, {Field: "hostname"}},
				Project: []string{"id", "hostname", "region", "env", "cpu"},
				Aggregate: []query.Aggregation{
					{Op: "count", Name: "hosts"},
					{Op: "avg", Field: "cpu", Name: "avg_cpu"},
				},
				Limit:     100,
				TimeoutMS: 5000,
				CostLimit: 500000,
			},
		},
		{
			name: "fuzzy-service-match",
			request: query.Request{
				Op: "match", Kind: "service",
				Where:     []query.Filter{{Field: "name", Op: "fuzzy", Value: fmt.Sprintf("svc-%03d", n%1000)}},
				Project:   []string{"id", "name", "app", "owner"},
				Limit:     50,
				CostLimit: 500000,
			},
		},
		{
			name: "neighbors-host-out",
			request: query.Request{
				Op: "neighbors", ID: hostID(stable), Direction: "out",
				RelationTypes: []string{"connects_to"},
				Project:       []string{"id", "kind", "name", "hostname"},
				Limit:         50,
				TimeoutMS:     5000,
			},
		},
		{
			name: "cmdb-owner-service-match",
			request: query.Request{
				Op: "match", Kind: "service",
				Where: []query.Filter{
					{Field: "owner", Op: "eq", Value: fmt.Sprintf("team-%02d", round%32)},
					{Field: "tier", Op: "in", Value: []any{"api", "worker"}},
				},
				Sort:      []query.SortSpec{{Field: "name"}},
				Project:   []string{"id", "name", "env", "app", "tier", "owner"},
				Limit:     250,
				TimeoutMS: 5000,
				CostLimit: 500000,
			},
		},
		{
			name: "cmdb-env-containment-traverse",
			request: query.Request{
				Op: "traverse", ID: "environment:" + env, Direction: "out",
				RelationTypes: []string{"contains"},
				Depth:         2,
				Path:          query.PathFilter{NodeKinds: []string{"cluster", "host"}, MaxPaths: 500},
				Project:       []string{"id", "kind", "name", "hostname", "region", "env"},
				Limit:         500,
				TimeoutMS:     5000,
				CostLimit:     500000,
			},
		},
		{
			name: "cmdb-service-to-database-path",
			request: query.Request{
				Op: "traverse", ID: serviceID(stable), Direction: "out",
				RelationTypes: []string{"depends_on", "connects_to"},
				Depth:         2,
				Path: query.PathFilter{
					NodeKinds: []string{"service", "host", "database"},
					EndKind:   "database",
					EndWhere:  []query.Filter{{Field: "engine", Op: "exists", Value: true}},
					MaxPaths:  100,
				},
				Project:   []string{"id", "kind", "name", "engine"},
				Limit:     100,
				TimeoutMS: 5000,
				CostLimit: 500000,
			},
		},
		{
			name: "path-filter-traverse",
			request: query.Request{
				Op: "traverse", ID: serviceID(stable), Direction: "out",
				RelationTypes: []string{"runs_on", "contains", "depends_on", "connects_to"},
				Depth:         3,
				Path: query.PathFilter{
					NodeKinds: []string{"service", "host", "cluster", "database"},
					MaxPaths:  50,
				},
				Limit:     50,
				TimeoutMS: 5000,
				CostLimit: 500000,
			},
		},
		{
			name: "impact-service",
			request: query.Request{
				Op: "impact", ID: serviceID(stable), Direction: "out",
				RelationTypes: []string{"runs_on", "depends_on", "connects_to"},
				Depth:         3,
				Limit:         50,
				TimeoutMS:     5000,
				CostLimit:     500000,
			},
		},
		{
			name: "shortest-service-cluster",
			request: query.Request{
				Op: "shortest_path", ID: serviceID(stable), TargetID: hostID(stable), Direction: "out",
				RelationTypes: []string{"runs_on"},
				Depth:         1,
				Limit:         1,
				TimeoutMS:     5000,
				CostLimit:     500000,
			},
		},
		{name: "stream-large-indexed-hosts", request: indexedHostMatch(env, 1000), stream: true, timeout: 2 * time.Minute},
		{name: "stream-large-indexed-hosts-min-version", request: indexedHostMatch(env, 1000), stream: true, timeout: 2 * time.Minute, minVersion: latestVersion},
		{name: "stream-all-host-page", request: allHostsMatch(1000), stream: true, timeout: 2 * time.Minute},
		{name: "scan-entities", scan: "entities", timeout: 2 * time.Minute},
		{name: "scan-entities-min-version", scan: "entities-min-version", timeout: 2 * time.Minute, minVersion: latestVersion},
		{name: "scan-entities-allow-stale", scan: "entities-allow-stale", timeout: 2 * time.Minute},
		{name: "scan-edges", scan: "edges", timeout: 2 * time.Minute},
		{name: "scan-edges-allow-stale", scan: "edges-allow-stale", timeout: 2 * time.Minute},
		{name: "export-snapshot-stream", scan: "snapshot-export", timeout: 5 * time.Minute},
		{name: "saved-host-query", saved: "soak-hosts-by-region"},
		{name: "saved-prod-services", saved: "soak-prod-services"},
		{name: "saved-team-services", saved: "soak-team-services"},
		{name: "saved-service-impact", saved: "soak-service-impact"},
		{name: "saved-prod-containment", saved: "soak-prod-containment"},
	}
}

func indexedHostMatch(value string, limit int) query.Request {
	field := "region"
	if value == "prod" || value == "staging" || value == "dev" {
		field = "env"
	}
	return query.Request{
		Op:        "match",
		Kind:      "host",
		Where:     []query.Filter{{Field: field, Op: "eq", Value: value}},
		Sort:      []query.SortSpec{{Field: "hostname"}},
		Project:   []string{"id", "hostname", "region", "env", "app", "owner"},
		Limit:     limit,
		TimeoutMS: 5000,
		CostLimit: 500000,
	}
}

func allHostsMatch(limit int) query.Request {
	return query.Request{
		Op:        "match",
		Kind:      "host",
		Project:   []string{"id", "hostname", "region", "env", "app", "owner"},
		Limit:     limit,
		TimeoutMS: 10000,
		CostLimit: 500000,
	}
}

func profile(request query.Request) query.Request {
	request.TargetOp = request.Op
	request.Op = "profile"
	return request
}
