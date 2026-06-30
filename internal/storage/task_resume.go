package storage

import (
	"context"
	"errors"
)

func (s *TenantStore) loadTaskResultByKey(ctx context.Context, key string) (map[string]any, bool, error) {
	tenantID, taskID, ok := s.taskResultIdentityFromKey(key)
	if !ok {
		return nil, false, nil
	}
	data, err := s.Objects.Get(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	result, err := decodeParquetTaskResult(ctx, data, tenantID, taskID)
	if err != nil {
		return nil, false, err
	}
	return result, true, nil
}

func exportTaskSummary(result map[string]any, resultKey string, resumed bool) map[string]any {
	summary := map[string]any{
		"tenant_id":  result["tenant_id"],
		"version":    result["version"],
		"result_key": resultKey,
	}
	if resumed {
		summary["resumed"] = true
	}
	snapshot, ok := result["snapshot"].(map[string]any)
	if !ok {
		return summary
	}
	summary["ci_types"] = lenAnySlice(snapshot["ci_types"])
	summary["relation_types"] = lenAnySlice(snapshot["relation_types"])
	summary["entities"] = lenAnySlice(snapshot["entities"])
	summary["edges"] = lenAnySlice(snapshot["edges"])
	return summary
}

func lenAnySlice(value any) int {
	switch typed := value.(type) {
	case []any:
		return len(typed)
	default:
		return 0
	}
}
