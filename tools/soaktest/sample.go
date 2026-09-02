package main

import (
	"context"
)

type sampleResult struct {
	readerReady bool
}

func sampleState(ctx context.Context, admin *apiClient, reader *apiClient, metrics *registry, events *eventWriter, includeReader bool) sampleResult {
	result := sampleResult{}
	if resp, err := admin.tenantUsage(ctx, metrics); err != nil {
		events.emit("usage_sample_error", map[string]any{"error": err.Error()})
	} else {
		events.emit("usage_sample", usageFields(resp.json))
	}
	metrics.emit(events)
	if resp, err := admin.indexCatalog(ctx, metrics); err != nil {
		events.emit("index_catalog_sample_error", map[string]any{"error": err.Error(), "status": resp.status})
	} else if resp.status == 200 {
		events.emit("index_catalog_sample", indexCatalogFields(resp.json))
	}
	if !includeReader {
		events.emit("reader_checks_skipped", map[string]any{"reason": "reader_paused"})
		return result
	}
	if resp, err := admin.indexHealth(ctx, metrics); err != nil {
		events.emit("index_health_sample_error", map[string]any{"error": err.Error(), "status": resp.status})
	} else {
		events.emit("index_health_sample", indexHealthFields(resp.json))
	}
	if resp, err := admin.readerFreshness(ctx, metrics); err != nil {
		events.emit("reader_freshness_error", map[string]any{"error": err.Error(), "status": resp.status})
	} else {
		events.emit("reader_freshness", map[string]any{
			"status":          resp.json["status"],
			"fresh":           resp.json["fresh"],
			"consistent":      resp.json["consistent"],
			"visible_version": int64Value(resp.json["visible_version"]),
			"version_lag":     int64Value(resp.json["version_lag"]),
			"lag_ms":          int64Value(resp.json["lag_ms"]),
		})
	}
	fleetReady := false
	if resp, err := admin.fleetReadiness(ctx, metrics); err != nil {
		events.emit("reader_fleet_error", map[string]any{"error": err.Error(), "status": resp.status})
	} else {
		fleetReady = boolValue(resp.json["ready"])
		events.emit("reader_fleet", map[string]any{
			"ready":          resp.json["ready"],
			"target_version": int64Value(resp.json["target_version"]),
			"total_readers":  intValue(resp.json["total_readers"]),
			"ready_readers":  intValue(resp.json["ready_readers"]),
			"stale_readers":  intValue(resp.json["stale_readers"]),
		})
	}
	result.readerReady = fleetReady
	return result
}

func usageFields(body map[string]any) map[string]any {
	fields := map[string]any{
		"manifest_version":   int64Value(body["manifest_version"]),
		"snapshot_version":   int64Value(body["snapshot_version"]),
		"commit_tail_length": intValue(body["commit_tail_length"]),
		"object_count":       intValue(body["object_count"]),
		"total_bytes":        int64Value(body["total_bytes"]),
	}
	categories := map[string]any{}
	if values, ok := body["categories"].([]any); ok {
		for _, value := range values {
			category, ok := value.(map[string]any)
			if !ok {
				continue
			}
			name, _ := category["name"].(string)
			if name == "" {
				continue
			}
			categories[name+"_objects"] = intValue(category["object_count"])
			categories[name+"_bytes"] = int64Value(category["bytes"])
		}
	}
	fields["categories"] = categories
	return fields
}

func indexCatalogFields(body map[string]any) map[string]any {
	indexes, _ := body["indexes"].([]any)
	edgeShards, _ := body["edge_shards"].([]any)
	entityPages, _ := body["entity_pages"].([]any)
	return map[string]any{
		"version":       int64Value(body["version"]),
		"index_count":   len(indexes),
		"edge_shards":   len(edgeShards),
		"entity_pages":  len(entityPages),
		"index_entries": sumObjects(indexes, "entry_count"),
		"edge_rows":     sumObjects(edgeShards, "edge_count"),
		"entity_rows":   sumObjects(entityPages, "entity_count"),
	}
}

func indexHealthFields(body map[string]any) map[string]any {
	issues, _ := body["issues"].([]any)
	return map[string]any{
		"status":           body["status"],
		"manifest_version": int64Value(body["manifest_version"]),
		"catalog_version":  int64Value(body["catalog_version"]),
		"issue_count":      len(issues),
	}
}

func sumObjects(values []any, field string) int {
	total := 0
	for _, value := range values {
		object, ok := value.(map[string]any)
		if !ok {
			continue
		}
		total += intValue(object[field])
	}
	return total
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case int64:
		return int(typed)
	case float64:
		return int(typed)
	default:
		return 0
	}
}

func int64Value(value any) int64 {
	switch typed := value.(type) {
	case int64:
		return typed
	case int:
		return int64(typed)
	case float64:
		return int64(typed)
	default:
		return 0
	}
}
