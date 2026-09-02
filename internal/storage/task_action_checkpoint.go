package storage

import (
	"context"
	"time"
)

const taskCheckpointActionsKey = "actions"

type taskActionUpdate struct {
	ID           string
	Status       string
	Input        map[string]any
	Output       map[string]any
	Verification map[string]any
	Err          error
}

func (s *TenantStore) updateTaskActionProgress(ctx context.Context, task Task, phase string, completed int, total int, action taskActionUpdate, extra map[string]any) error {
	writeCtx, cancel := s.taskPersistenceContext(ctx)
	defer cancel()
	current := s.taskStateOrLocal(writeCtx, task)
	checkpoint := taskActionCheckpoint(current.Checkpoint, action)
	for key, value := range extra {
		checkpoint[key] = value
	}
	checkpoint["phase"] = phase
	return s.updateTaskProgress(writeCtx, current, phase, completed, total, checkpoint)
}

func taskActionCheckpoint(existing map[string]any, update taskActionUpdate) map[string]any {
	out := map[string]any{}
	if update.ID == "" {
		return out
	}
	actions := taskCheckpointActions(existing)
	replaced := false
	next := taskActionMap(update)
	for i, action := range actions {
		if actionID(action) == update.ID {
			actions[i] = mergeActionMap(action, next)
			replaced = true
			break
		}
	}
	if !replaced {
		actions = append(actions, next)
	}
	completed := 0
	for _, action := range actions {
		if stringValue(action["status"]) == "completed" {
			completed++
		}
	}
	out[taskCheckpointActionsKey] = actions
	out["current_action"] = update.ID
	out["current_action_status"] = update.Status
	out["action_completed"] = completed
	out["action_total"] = len(actions)
	return out
}

func taskActionMap(update taskActionUpdate) map[string]any {
	status := update.Status
	if status == "" {
		status = "running"
	}
	out := map[string]any{
		"id":         update.ID,
		"status":     status,
		"updated_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
	if len(update.Input) > 0 {
		out["input"] = update.Input
	}
	if len(update.Output) > 0 {
		out["output"] = update.Output
	}
	if len(update.Verification) > 0 {
		out["verification"] = update.Verification
	}
	if update.Err != nil {
		out["status"] = "failed"
		out["error"] = update.Err.Error()
	}
	return out
}

func mergeActionMap(existing map[string]any, next map[string]any) map[string]any {
	out := make(map[string]any, len(existing)+len(next))
	for key, value := range existing {
		out[key] = value
	}
	for key, value := range next {
		out[key] = value
	}
	return out
}

func taskCheckpointActions(checkpoint map[string]any) []map[string]any {
	if len(checkpoint) == 0 {
		return nil
	}
	raw, ok := checkpoint[taskCheckpointActionsKey]
	if !ok {
		return nil
	}
	switch typed := raw.(type) {
	case []map[string]any:
		return cloneActionMaps(typed)
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, value := range typed {
			if action, ok := value.(map[string]any); ok {
				out = append(out, cloneTaskActionMap(action))
			}
		}
		return out
	default:
		return nil
	}
}

func taskActionCompleted(task Task, id string) bool {
	return taskActionStatus(task, id) == "completed"
}

func taskActionStatus(task Task, id string) string {
	for _, action := range taskCheckpointActions(task.Checkpoint) {
		if actionID(action) == id {
			return stringValue(action["status"])
		}
	}
	return ""
}

func actionID(action map[string]any) string {
	return stringValue(action["id"])
}

func cloneActionMaps(actions []map[string]any) []map[string]any {
	out := make([]map[string]any, 0, len(actions))
	for _, action := range actions {
		out = append(out, cloneTaskActionMap(action))
	}
	return out
}

func cloneTaskActionMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}
