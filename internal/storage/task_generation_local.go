package storage

import (
	"context"
	"fmt"
)

const taskIngestGenerationCheckpoint = "local_ingest_generation"

type taskIngestGenerationKey struct{}

type taskIngestGeneration struct {
	store      *TenantStore
	tenantID   string
	generation int64
}

func taskUsesIngest(taskType string) bool {
	return taskType == TaskTypeBulkImport || taskType == TaskTypeReplayDeadLetter
}

// Called under the tenant lock. Checkpoints from before generation tracking can
// resume only in a tenant that has never been replaced.
func (s *TenantStore) prepareTaskIngestCheckpoint(ctx context.Context, tenantID, taskType string, checkpoint map[string]any, resuming bool) (map[string]any, error) {
	if s.localFileStore() == nil || !taskUsesIngest(taskType) {
		return checkpoint, nil
	}
	current, err := s.localIngestGeneration(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	expected := current
	if resuming {
		expected = 1
		if value, exists := checkpoint[taskIngestGenerationCheckpoint]; exists {
			expected = taskCheckpointNumber(value)
		}
	}
	if expected != current {
		return nil, fmt.Errorf("%w: task tenant generation changed from %d to %d", ErrConflict, expected, current)
	}
	if checkpoint == nil {
		checkpoint = make(map[string]any)
	}
	checkpoint[taskIngestGenerationCheckpoint] = current
	return checkpoint, nil
}

func (s *TenantStore) taskIngestContext(ctx context.Context, task Task) (context.Context, error) {
	if s.localFileStore() == nil || !taskUsesIngest(task.Type) {
		return ctx, nil
	}
	generation := int64(1)
	if value, exists := task.Checkpoint[taskIngestGenerationCheckpoint]; exists {
		generation = taskCheckpointNumber(value)
	}
	ctx = context.WithValue(ctx, taskIngestGenerationKey{}, taskIngestGeneration{s, task.TenantID, generation})
	return ctx, s.validateTaskIngestGeneration(ctx, task.TenantID)
}

func (s *TenantStore) validateTaskIngestGeneration(ctx context.Context, tenantID string) error {
	expected, ok := ctx.Value(taskIngestGenerationKey{}).(taskIngestGeneration)
	if !ok || expected.store != s || expected.tenantID != tenantID {
		return nil
	}
	current, err := s.localIngestGeneration(ctx, tenantID)
	if err != nil {
		return err
	}
	if expected.generation != current {
		return fmt.Errorf("%w: task tenant generation changed from %d to %d", ErrConflict, expected.generation, current)
	}
	return nil
}
