package storage

const taskResumeCheckpointParam = "_resume_checkpoint"

func taskInitialCheckpoint(params map[string]any) map[string]any {
	raw, ok := params[taskResumeCheckpointParam]
	if !ok {
		return nil
	}
	delete(params, taskResumeCheckpointParam)
	checkpoint, ok := raw.(map[string]any)
	if !ok || len(checkpoint) == 0 {
		return nil
	}
	return cloneTaskParams(checkpoint)
}

func taskCheckpointString(task Task, key string) string {
	if len(task.Checkpoint) == 0 {
		return ""
	}
	value, ok := task.Checkpoint[key]
	if !ok {
		return ""
	}
	return stringValue(value)
}

func taskCheckpointBool(task Task, key string) bool {
	if len(task.Checkpoint) == 0 {
		return false
	}
	value, ok := task.Checkpoint[key]
	if !ok {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return typed == "true" || typed == "1"
	default:
		return false
	}
}

func taskCheckpointInt64(task Task, key string) int64 {
	if len(task.Checkpoint) == 0 {
		return 0
	}
	value, ok := task.Checkpoint[key]
	if !ok {
		return 0
	}
	return taskCheckpointNumber(value)
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return ""
	}
}

func taskCheckpointNumber(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	default:
		return 0
	}
}
