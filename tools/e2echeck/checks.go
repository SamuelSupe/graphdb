package main

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"graphdb/internal/graph"
	"graphdb/internal/httpapi"
	"graphdb/internal/query"
	"graphdb/internal/storage"
)

type runner struct {
	cfg    config
	writer *apiClient
	reader *apiClient
	other  *apiClient
}

func (r *runner) run(ctx context.Context) error {
	id := testIDs(r.cfg.tenant)
	if err := r.checkHealth(ctx); err != nil {
		return err
	}
	if err := r.checkReaderRejectsWrites(ctx); err != nil {
		return err
	}
	if err := r.checkSourcePolicy(ctx); err != nil {
		return err
	}
	seed, err := r.commitResponse(ctx, "seed", seedMutations(id))
	if err != nil {
		return err
	}
	seedVersion := intValue(seed.json["version"])
	if err := r.checkReaderVersionContract(ctx, id.host1, seedVersion); err != nil {
		return err
	}
	if err := r.waitReaderEntity(ctx, id.host1); err != nil {
		return err
	}
	if err := r.checkTenantIsolation(ctx, id.host1); err != nil {
		return err
	}
	if err := r.checkSourcePriority(ctx, id.host1); err != nil {
		return err
	}
	if err := r.checkEntityGovernance(ctx); err != nil {
		return err
	}
	if err := r.checkEdgeGovernance(ctx, id); err != nil {
		return err
	}
	if err := r.checkGraphQueries(ctx, id); err != nil {
		return err
	}
	if err := r.checkIngestion(ctx); err != nil {
		return err
	}
	if err := r.checkIndexesAndLazyQuery(ctx); err != nil {
		return err
	}
	if err := r.checkStreamQuery(ctx); err != nil {
		return err
	}
	if err := r.checkOperationalReadiness(ctx, id); err != nil {
		return err
	}
	if err := r.checkIndexedDelete(ctx); err != nil {
		return err
	}
	if err := r.checkParquetIndexes(ctx, id); err != nil {
		return err
	}
	if err := r.checkCompactAndControl(ctx, id.host1); err != nil {
		return err
	}
	return nil
}

func (r *runner) checkHealth(ctx context.Context) error {
	writer, err := r.writer.do(ctx, http.MethodGet, "/v1/health", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if mode := stringValue(writer.json["mode"]); mode != "writer" && mode != "all" {
		return fmt.Errorf("writer mode = %q", mode)
	}
	reader, err := r.reader.do(ctx, http.MethodGet, "/v1/health", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if mode := stringValue(reader.json["mode"]); mode != "reader" {
		return fmt.Errorf("reader mode = %q", mode)
	}
	pass("health writer=%s reader=%s", stringValue(writer.json["mode"]), stringValue(reader.json["mode"]))
	return nil
}

func (r *runner) checkReaderRejectsWrites(ctx context.Context) error {
	_, err := r.reader.do(ctx, http.MethodPost, "/v1/commits", httpapi.CommitRequest{}, http.StatusMethodNotAllowed)
	if err != nil {
		return err
	}
	pass("reader rejects writes")
	return nil
}

func (r *runner) checkSourcePolicy(ctx context.Context) error {
	if _, err := r.writer.do(ctx, http.MethodPut, "/v1/source-policy", sourcePolicy(), http.StatusOK); err != nil {
		return err
	}
	resp, err := r.reader.do(ctx, http.MethodGet, "/v1/source-policy", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !boolValue(resp.json["configured"]) {
		return fmt.Errorf("source policy not configured on reader")
	}
	pass("source policy configured")
	return nil
}

func (r *runner) commit(ctx context.Context, name string, mutations any) error {
	_, err := r.commitResponse(ctx, name, mutations)
	return err
}

func (r *runner) commitResponse(ctx context.Context, name string, mutations any) (apiResponse, error) {
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/commits", httpapi.CommitRequest{Mutations: mustMutations(mutations)}, http.StatusOK)
	if err == nil {
		pass("commit %s", name)
	}
	return resp, err
}

func (r *runner) checkReaderVersionContract(ctx context.Context, id string, version int) error {
	if version <= 0 {
		return fmt.Errorf("seed commit did not return version: %d", version)
	}
	entity, err := r.reader.do(ctx, http.MethodGet, fmt.Sprintf("%s?min_version=%d", entityPath(id), version), nil, http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(entity.json["version"]) < version {
		return fmt.Errorf("reader entity version = %d, want >= %d: %s", intValue(entity.json["version"]), version, string(entity.body))
	}
	queryResp, err := r.reader.doWithHeaders(ctx, http.MethodPost, "/v1/query", query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "eq", Value: id}},
		Limit: 1,
	}, map[string]string{"X-GraphDB-Min-Version": fmt.Sprintf("%d", version)}, http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(queryResp.json["version"]) < version || len(arrayValue(queryResp.json["results"])) != 1 {
		return fmt.Errorf("reader min-version query = %s", string(queryResp.body))
	}
	if _, err := r.reader.doWithHeaders(ctx, http.MethodGet, "/v1/entities?kind=host&limit=1", nil, map[string]string{"X-GraphDB-Allow-Stale": "true"}, http.StatusOK); err != nil {
		return err
	}
	impossible, err := r.reader.do(ctx, http.MethodGet, fmt.Sprintf("%s?min_version=%d", entityPath(id), version+100000), nil, http.StatusServiceUnavailable)
	if err != nil {
		return err
	}
	if impossible.headers.Get("Retry-After") == "" ||
		stringValue(impossible.json["code"]) != "reader_not_fresh" ||
		!boolValue(impossible.json["retryable"]) ||
		intValue(mapValue(impossible.json["detail"])["required_version"]) != version+100000 {
		return fmt.Errorf("reader_not_fresh contract = headers=%v body=%s", impossible.headers, string(impossible.body))
	}
	pass("reader min_version allow_stale and reader_not_fresh contract")
	return nil
}

func (r *runner) waitReaderEntity(ctx context.Context, id string) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := r.reader.do(ctx, http.MethodGet, entityPath(id), nil, http.StatusOK, http.StatusNotFound)
		if err != nil {
			return err
		}
		if resp.status == http.StatusOK {
			pass("reader observed entity %s", id)
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("reader did not observe %s", id)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (r *runner) checkTenantIsolation(ctx context.Context, id string) error {
	if _, err := r.other.do(ctx, http.MethodGet, entityPath(id), nil, http.StatusNotFound); err != nil {
		return err
	}
	pass("tenant isolation")
	return nil
}

func (r *runner) checkSourcePriority(ctx context.Context, hostID string) error {
	if err := r.commit(ctx, "manual-region", updateRegionMutation(hostID, "manual", "manual-r0")); err != nil {
		return err
	}
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/commits", httpapi.CommitRequest{Mutations: updateRegionMutation(hostID, "agent", "agent-r0")}, http.StatusOK)
	if err != nil {
		return err
	}
	if len(arrayValue(resp.json["suppressed"])) == 0 {
		return fmt.Errorf("expected suppressed conflict")
	}
	entity, err := r.writer.do(ctx, http.MethodGet, entityPath(hostID), nil, http.StatusOK)
	if err != nil {
		return err
	}
	region := nestedString(entity.json, "entity", "fields", "region")
	if region != "manual-r0" {
		return fmt.Errorf("source priority region = %q", region)
	}
	pass("source priority suppression")
	return nil
}

func (r *runner) checkEntityGovernance(ctx context.Context) error {
	externalID := "manual-asset-" + r.cfg.tenant
	canonicalID := graph.CanonicalEntityIDParts("host", "manual", externalID)
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/commits", httpapi.CommitRequest{Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{
		Kind: "host", Source: "manual", ExternalID: externalID, Fields: graph.Fields{"hostname": canonicalID, "region": "manual-entity"},
	}}}}, http.StatusOK)
	if err != nil {
		return err
	}
	if stringValue(mapValue(arrayValue(resp.json["canonical_entities"])[0])["canonical_id"]) != canonicalID {
		return fmt.Errorf("entity canonical response = %s", string(resp.body))
	}
	deleteResp, err := r.writer.do(ctx, http.MethodPost, "/v1/commits", httpapi.CommitRequest{Mutations: graph.Mutations{DeleteEntityRequests: []graph.EntityDeleteRequest{{
		ID: canonicalID, Source: "agent",
	}}}}, http.StatusOK)
	if err != nil {
		return err
	}
	if len(arrayValue(deleteResp.json["suppressed"])) != 1 {
		return fmt.Errorf("expected source-aware entity delete suppression: %s", string(deleteResp.body))
	}
	if _, err := r.writer.do(ctx, http.MethodGet, entityPath(canonicalID), nil, http.StatusOK); err != nil {
		return err
	}
	staleA := "stale-a-" + r.cfg.tenant
	staleB := "stale-b-" + r.cfg.tenant
	seed := storage.IngestRequest{
		Source:      "cloud",
		CollectorID: "collector-stale",
		BatchID:     "stale-seed",
		Items: []storage.IngestItem{
			{ExternalID: staleA, Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "stale-a-" + r.cfg.tenant}}},
			{ExternalID: staleB, Entity: &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "stale-b-" + r.cfg.tenant}}},
		},
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/ingest/batches", seed, http.StatusOK); err != nil {
		return err
	}
	full := storage.IngestRequest{
		Source:      "cloud",
		CollectorID: "collector-stale",
		BatchID:     "stale-full",
		FullSync:    true,
		Items: []storage.IngestItem{{
			ExternalID: staleA,
			Entity:     &graph.Entity{Kind: "host", Fields: graph.Fields{"hostname": "stale-a-" + r.cfg.tenant}},
		}},
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/ingest/batches", full, http.StatusOK); err != nil {
		return err
	}
	staleID := graph.CanonicalEntityIDParts("host", "cloud", staleB)
	staleEntity, err := r.writer.do(ctx, http.MethodGet, entityPath(staleID), nil, http.StatusOK)
	if err != nil {
		return err
	}
	sources := arrayValue(mapValue(staleEntity.json["entity"])["sources"])
	if len(sources) != 1 || !boolValue(mapValue(sources[0])["stale"]) {
		return fmt.Errorf("stale sources = %v", sources)
	}
	lag, err := r.reader.do(ctx, http.MethodGet, "/v1/control/reader-lag", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(lag.json["manifest_version"]) < intValue(lag.json["visible_version"]) {
		return fmt.Errorf("reader lag response = %s", string(lag.body))
	}
	pass("entity canonical identity source-aware delete stale and reader lag")
	return nil
}

func (r *runner) checkEdgeGovernance(ctx context.Context, id ids) error {
	if err := r.commit(ctx, "manual-edge", manualRunsOnEdge(id)); err != nil {
		return err
	}
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/commits", httpapi.CommitRequest{Mutations: agentRunsOnEdge(id)}, http.StatusOK)
	if err != nil {
		return err
	}
	suppressed := arrayValue(resp.json["suppressed"])
	edgeID := graph.CanonicalEdgeIDParts("runs_on", id.service, id.host1)
	if len(suppressed) == 0 {
		return fmt.Errorf("expected edge field suppression")
	}
	first := mapValue(suppressed[0])
	if stringValue(first["resource_type"]) != "edge" || stringValue(first["canonical_id"]) != edgeID {
		return fmt.Errorf("edge suppression = %v", first)
	}
	neighbors, err := r.writer.do(ctx, http.MethodPost, "/v1/query", query.Request{Op: "neighbors", ID: id.service, Direction: "out", RelationType: "runs_on", Limit: 10}, http.StatusOK)
	if err != nil {
		return err
	}
	results := arrayValue(neighbors.json["results"])
	if len(results) != 1 {
		return fmt.Errorf("runs_on neighbors = %d, want canonical single edge", len(results))
	}
	edge := mapValue(mapValue(results[0])["edge"])
	if stringValue(edge["id"]) != edgeID {
		return fmt.Errorf("neighbor edge id = %q, want %q", stringValue(edge["id"]), edgeID)
	}
	deleteResp, err := r.writer.do(ctx, http.MethodPost, "/v1/ingest/batches", agentDeleteRunsOnEdge(id), http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(deleteResp.json["failed"]) != 0 || intValue(deleteResp.json["suppressed"]) != 1 {
		return fmt.Errorf("edge delete ingest result = %s", string(deleteResp.body))
	}
	afterDelete, err := r.writer.do(ctx, http.MethodPost, "/v1/query", query.Request{Op: "neighbors", ID: id.service, Direction: "out", RelationType: "runs_on", Limit: 10}, http.StatusOK)
	if err != nil {
		return err
	}
	if len(arrayValue(afterDelete.json["results"])) != 1 {
		return fmt.Errorf("suppressed edge delete removed edge: %s", string(afterDelete.body))
	}
	pass("edge canonical identity and source-aware delete")
	return nil
}

func (r *runner) checkGraphQueries(ctx context.Context, id ids) error {
	queries := []struct {
		name string
		body query.Request
	}{
		{"match", matchHosts("manual-r0", 5, false)},
		{"neighbors", query.Request{Op: "neighbors", ID: id.service, Direction: "out", RelationType: "runs_on", Limit: 5}},
		{"traverse", query.Request{Op: "traverse", ID: id.service, Direction: "out", Depth: 1, Limit: 5}},
		{"shortest_path", query.Request{Op: "shortest_path", ID: id.service, TargetID: id.db, Direction: "out", RelationType: "depends_on", Depth: 3, Limit: 5}},
	}
	for _, item := range queries {
		resp, err := r.writer.do(ctx, http.MethodPost, "/v1/query", item.body, http.StatusOK)
		if err != nil {
			return err
		}
		if len(arrayValue(resp.json["results"])) == 0 {
			return fmt.Errorf("%s returned no results", item.name)
		}
		pass("query %s", item.name)
	}
	return nil
}

func (r *runner) checkIngestion(ctx context.Context) error {
	body := bulkIngest(r.cfg.tenant, 40)
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/ingest/batches", body, http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(resp.json["applied"]) != 40 || intValue(resp.json["failed"]) != 0 {
		return fmt.Errorf("ingest result = %s", string(resp.body))
	}
	replay, err := r.writer.do(ctx, http.MethodPost, "/v1/ingest/batches", body, http.StatusOK)
	if err != nil {
		return err
	}
	if !boolValue(replay.json["skipped"]) {
		return fmt.Errorf("idempotent replay was not skipped")
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/ingest/batches", invalidIngest(), http.StatusMultiStatus); err != nil {
		return err
	}
	dead, err := r.writer.do(ctx, http.MethodGet, "/v1/ingest/deadletters/badsrc", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if len(arrayValue(dead.json["deadletters"])) == 0 {
		return fmt.Errorf("deadletter not recorded")
	}
	status, err := r.writer.do(ctx, http.MethodGet, "/v1/ingest/collectors/agent/collector-a", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(status.json["applied_total"]) < 40 {
		return fmt.Errorf("collector applied_total = %d", intValue(status.json["applied_total"]))
	}
	pass("ingest idempotency and deadletter")
	return nil
}

func (r *runner) checkIndexesAndLazyQuery(ctx context.Context) error {
	if err := r.checkIndexDDL(ctx); err != nil {
		return err
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/indexes/rebuild", nil, http.StatusOK); err != nil {
		return err
	}
	health, err := r.writer.do(ctx, http.MethodGet, "/v1/indexes/health", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if stringValue(health.json["status"]) != "ready" {
		return fmt.Errorf("index health = %s", string(health.body))
	}
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/query", matchHosts("bulk-r0", 5, false), http.StatusOK)
	if err != nil {
		return err
	}
	if scanned := intValue(mapValue(resp.json["stats"])["scanned"]); scanned > 6 {
		return fmt.Errorf("lazy query scanned %d, want <= 6", scanned)
	}
	sorted, err := r.writer.do(ctx, http.MethodPost, "/v1/query", matchHosts("bulk-r0", 5, true), http.StatusOK)
	if err != nil {
		return err
	}
	plan := mapValue(sorted.json["plan"])
	if intValue(plan["estimated_cost"]) <= intValue(plan["estimated_rows"]) {
		return fmt.Errorf("sorted query cost not inflated: %v", plan)
	}
	pass("indexes and lazy query")
	return nil
}

func (r *runner) checkIndexDDL(ctx context.Context) error {
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/indexes", storage.IndexDefinition{Kind: "host", Field: "owner", Name: "host.owner-ddl"}, http.StatusAccepted)
	if err != nil {
		return err
	}
	taskID := nestedString(resp.json, "task", "id")
	if taskID == "" {
		return fmt.Errorf("create index response missing task: %s", string(resp.body))
	}
	if err := r.waitIndexTask(ctx, taskID); err != nil {
		return err
	}
	definitions, err := r.writer.do(ctx, http.MethodGet, "/v1/indexes/definitions", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if len(arrayValue(definitions.json["indexes"])) == 0 {
		return fmt.Errorf("index definitions empty after create: %s", string(definitions.body))
	}
	drop, err := r.writer.do(ctx, http.MethodDelete, "/v1/indexes/definitions/host.owner-ddl", nil, http.StatusAccepted)
	if err != nil {
		return err
	}
	taskID = nestedString(drop.json, "task", "id")
	if taskID == "" {
		return fmt.Errorf("drop index response missing task: %s", string(drop.body))
	}
	if err := r.waitIndexTask(ctx, taskID); err != nil {
		return err
	}
	pass("index create drop background rebuild")
	return nil
}

func (r *runner) waitIndexTask(ctx context.Context, taskID string) error {
	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := r.writer.do(ctx, http.MethodGet, "/v1/indexes/tasks/"+taskID, nil, http.StatusOK)
		if err != nil {
			return err
		}
		status := stringValue(resp.json["status"])
		if status == "succeeded" {
			return nil
		}
		if status == "failed" {
			return fmt.Errorf("index task failed: %s", string(resp.body))
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("index task %s did not finish: %s", taskID, string(resp.body))
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (r *runner) checkStreamQuery(ctx context.Context) error {
	items, err := r.reader.ndjson(ctx, "/v1/query/stream", matchHosts("bulk-r0", 5, false))
	if err != nil {
		return err
	}
	if len(items) < 3 {
		return fmt.Errorf("stream returned %d rows", len(items))
	}
	done := items[len(items)-1]
	stats := mapValue(done["stats"])
	if !boolValue(done["done"]) || intValue(stats["scanned"]) > 6 {
		return fmt.Errorf("stream done = %v", done)
	}
	pass("reader stream lazy pagination")
	return nil
}

func (r *runner) checkOperationalReadiness(ctx context.Context, id ids) error {
	queryResp, err := r.reader.do(ctx, http.MethodPost, "/v1/query", matchHosts("bulk-r0", 5, false), http.StatusOK)
	if err != nil {
		return err
	}
	if queryResp.headers.Get("X-GraphDB-Query-ID") == "" {
		return fmt.Errorf("query response missing X-GraphDB-Query-ID")
	}
	if _, err := r.reader.do(ctx, http.MethodPost, "/v1/query", query.Request{
		Op:    "match",
		Kind:  "host",
		Where: []query.Filter{{Field: "hostname", Op: "prefix", Value: "bulk-"}},
		Limit: 5,
	}, http.StatusOK); err != nil {
		return err
	}
	running, err := r.reader.do(ctx, http.MethodGet, "/v1/queries/running", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if len(arrayValue(running.json["queries"])) != 0 {
		return fmt.Errorf("running query list should be empty after request completed: %s", string(running.body))
	}
	usage, err := r.writer.do(ctx, http.MethodGet, "/v1/tenant-usage", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if intValue(usage.json["object_count"]) == 0 || intValue(usage.json["total_bytes"]) == 0 {
		return fmt.Errorf("tenant usage missing objects/bytes: %s", string(usage.body))
	}
	if !usageHasCategory(usage.json, "commits") || !usageHasCategory(usage.json, "indexes") {
		return fmt.Errorf("tenant usage missing commit/index category: %s", string(usage.body))
	}
	readiness, err := r.reader.do(ctx, http.MethodGet, "/v1/control/reader-fleet-readiness?min_ready=1&max_staleness_ms=30000", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !boolValue(readiness.json["ready"]) || intValue(readiness.json["ready_readers"]) < 1 {
		return fmt.Errorf("reader fleet readiness not ready: %s", string(readiness.body))
	}
	gate, err := r.reader.do(ctx, http.MethodGet, "/v1/control/reader-traffic-gate", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !boolValue(gate.json["serve_traffic"]) {
		return fmt.Errorf("reader traffic gate is not serving: %s", string(gate.body))
	}
	pass("tenant usage reader fleet readiness traffic gate and running query control")
	return nil
}

func usageHasCategory(body map[string]any, name string) bool {
	for _, item := range arrayValue(body["categories"]) {
		if stringValue(mapValue(item)["name"]) == name {
			return true
		}
	}
	return false
}

func (r *runner) checkParquetIndexes(ctx context.Context, id ids) error {
	catalog, err := r.writer.do(ctx, http.MethodPost, "/v1/indexes/rebuild", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !catalogContainsFormat(catalog.json, "parquet") {
		return fmt.Errorf("parquet catalog missing format/objects: %s", string(catalog.body))
	}
	health, err := r.reader.do(ctx, http.MethodGet, "/v1/indexes/health", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if stringValue(health.json["status"]) != "ready" {
		return fmt.Errorf("parquet health = %s", string(health.body))
	}
	resp, err := r.reader.do(ctx, http.MethodPost, "/v1/query", matchHosts("bulk-r0", 5, false), http.StatusOK)
	if err != nil {
		return err
	}
	if scanned := intValue(mapValue(resp.json["stats"])["scanned"]); scanned > 6 {
		return fmt.Errorf("parquet lazy query scanned %d, want <= 6", scanned)
	}
	neighbors, err := r.reader.do(ctx, http.MethodPost, "/v1/query", query.Request{Op: "neighbors", ID: id.service, Direction: "out", RelationType: "runs_on", Limit: 10}, http.StatusOK)
	if err != nil {
		return err
	}
	if !strings.Contains(string(neighbors.body), id.host1) {
		return fmt.Errorf("parquet neighbors missing %s: %s", id.host1, string(neighbors.body))
	}
	entities, err := r.reader.do(ctx, http.MethodGet, "/v1/entities?kind=host&limit=3", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !boolValue(entities.json["indexed_read"]) || len(arrayValue(entities.json["entities"])) == 0 {
		return fmt.Errorf("parquet entity scan = %s", string(entities.body))
	}
	edges, err := r.reader.do(ctx, http.MethodGet, "/v1/edges?type=runs_on&limit=3", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !boolValue(edges.json["indexed_read"]) || len(arrayValue(edges.json["edges"])) == 0 {
		return fmt.Errorf("parquet edge scan = %s", string(edges.body))
	}
	export, err := r.reader.do(ctx, http.MethodGet, "/v1/export/snapshot/stream?inline=true", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !strings.Contains(string(export.body), `"stream":"snapshot"`) || !strings.Contains(string(export.body), id.host1) {
		return fmt.Errorf("parquet snapshot stream missing header or host %s", id.host1)
	}
	pass("parquet rebuild reader query scan export")
	return nil
}

func catalogContainsFormat(catalog map[string]any, format string) bool {
	for _, index := range arrayValue(catalog["indexes"]) {
		item := mapValue(index)
		if stringValue(item["format"]) == format && len(arrayValue(item["objects"])) > 0 {
			return true
		}
	}
	return false
}

func (r *runner) checkIndexedDelete(ctx context.Context) error {
	id := "host:" + r.cfg.tenant + ":delete-check"
	if err := r.commit(ctx, "delete-check-seed", graph.Mutations{UpsertEntities: []graph.Entity{{
		ID: id, Kind: "host", Source: "agent", Fields: graph.Fields{"hostname": id, "region": "delete-check"},
	}}}); err != nil {
		return err
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/indexes/rebuild", nil, http.StatusOK); err != nil {
		return err
	}
	resp, err := r.writer.do(ctx, http.MethodPost, "/v1/commits", httpapi.CommitRequest{Mutations: graph.Mutations{DeleteEntities: []string{id}}}, http.StatusOK)
	if err != nil {
		return err
	}
	if warnings := arrayValue(resp.json["index_warnings"]); len(warnings) != 0 {
		return fmt.Errorf("indexed delete returned warnings: %v", warnings)
	}
	if _, err := r.writer.do(ctx, http.MethodGet, entityPath(id), nil, http.StatusNotFound); err != nil {
		return err
	}
	health, err := r.writer.do(ctx, http.MethodGet, "/v1/indexes/health", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if stringValue(health.json["status"]) != "ready" {
		return fmt.Errorf("index health after indexed delete = %s", string(health.body))
	}
	pass("indexed delete keeps persisted index healthy")
	return nil
}

func (r *runner) checkCompactAndControl(ctx context.Context, id string) error {
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/compact", nil, http.StatusOK); err != nil {
		return err
	}
	if err := r.waitReaderEntity(ctx, id); err != nil {
		return err
	}
	if _, err := r.writer.do(ctx, http.MethodGet, "/v1/control/writer-lease", nil, http.StatusOK); err != nil {
		return err
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/control/recover", nil, http.StatusOK); err != nil {
		return err
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/control/cleanup-commits", nil, http.StatusOK); err != nil {
		return err
	}
	if _, err := r.writer.do(ctx, http.MethodPost, "/v1/control/gc", map[string]any{"keep_snapshots": 1, "cleanup_index_orphans": true}, http.StatusOK); err != nil {
		return err
	}
	pass("compact and control")
	return nil
}
