package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultTaskExecutionLimit = 4
	defaultTaskQueueLimit     = 128
	defaultTaskTenantStripes  = 64
)

var ErrMaintenanceBusy = errors.New("maintenance execution capacity is busy")

var ErrTaskServiceClosed = errors.New("task service is shutting down")

func newTaskTenantSlots(count int) []chan struct{} {
	if count < 1 {
		count = 1
	}
	slots := make([]chan struct{}, count)
	for i := range slots {
		slots[i] = make(chan struct{}, 1)
	}
	return slots
}

func taskActiveKey(tenantID string, taskType string) string {
	return tenantID + "\x00" + taskType
}

// TryAcquireMaintenance shares the bounded task execution pool with
// synchronous maintenance endpoints. It deliberately does not use the writer
// admission lock because compaction performs most work against immutable state
// and should not block foreground commits for its entire duration.
func (s *TenantStore) TryAcquireMaintenance(tenantID string) (func(), error) {
	_, release, err := s.TryAcquireMaintenanceContext(context.Background(), tenantID)
	return release, err
}

// TryAcquireMaintenanceContext lets a sequential maintenance operation release
// its execution slots while draining WAL, then reacquire them before proceeding.
// The returned context and release function belong to that one operation.
func (s *TenantStore) TryAcquireMaintenanceContext(ctx context.Context, tenantID string) (context.Context, func(), error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return ctx, nil, err
	}
	tenantSlot := s.taskTenantSlot(tenantID)
	select {
	case tenantSlot <- struct{}{}:
	default:
		return ctx, nil, ErrMaintenanceBusy
	}
	select {
	case s.taskExecutionSlots <- struct{}{}:
		admission := &taskExecutionAdmission{store: s, tenant: tenantSlot, execution: s.taskExecutionSlots, held: true}
		return context.WithValue(ctx, taskIngestAdmissionKey{}, admission), admission.release, nil
	default:
		releaseTaskSlot(tenantSlot)
		return ctx, nil, ErrMaintenanceBusy
	}
}

func (s *TenantStore) admitTask(ctx context.Context, task Task) (Task, bool, error) {
	return s.admitTaskMode(ctx, task, false)
}

func (s *TenantStore) admitTaskMode(ctx context.Context, task Task, replace bool) (Task, bool, error) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if s.taskClosing {
		return Task{}, false, ErrTaskServiceClosed
	}
	key := taskActiveKey(task.TenantID, task.Type)
	if active, ok := s.taskActive[key]; ok && !taskTerminal(active.Status) && !replace {
		if !sameTaskParams(active.Params, task.Params) {
			return Task{}, false, fmt.Errorf("%w: tenant %q already has %s task %q with different parameters", ErrConflict, task.TenantID, task.Type, active.ID)
		}
		return active, true, nil
	}
	select {
	case s.taskQueueSlots <- struct{}{}:
		s.taskActive[key] = task
		s.taskWorkers.Add(1)
		return Task{}, false, nil
	default:
		return Task{}, false, fmt.Errorf("task queue is full")
	}
}

func sameTaskParams(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	left, leftErr := json.Marshal(a)
	right, rightErr := json.Marshal(b)
	return leftErr == nil && rightErr == nil && string(left) == string(right)
}

func (s *TenantStore) releaseTaskAdmission(task Task) {
	s.taskMu.Lock()
	key := taskActiveKey(task.TenantID, task.Type)
	if active, ok := s.taskActive[key]; ok && active.ID == task.ID {
		delete(s.taskActive, key)
	}
	s.taskMu.Unlock()
	select {
	case <-s.taskQueueSlots:
	default:
	}
}

func (s *TenantStore) runTaskAdmitted(ctx context.Context, cancel context.CancelFunc, task Task) {
	defer cancel()
	defer s.releaseTaskAdmission(task)
	defer s.unregisterTaskCancel(task.TenantID, task.ID)
	if task.Type == TaskTypeTenantBackup && stringTaskParam(task.Params, "destination") == "object" {
		// Network waits must not occupy the pool that compaction needs to
		// relieve WAL backpressure. Bound captures and uploads together.
		if !acquireTaskSlot(ctx, s.taskBackupSlots) {
			s.persistQueuedTaskCancellation(ctx, task)
			return
		}
		defer releaseTaskSlot(s.taskBackupSlots)
	}
	if taskRetainsDataDuringWALWait(task.Type) {
		// Acquire before the tenant/execution slots so waiting for memory cannot
		// prevent compact from relieving another task's WAL backpressure.
		if !acquireTaskSlot(ctx, s.taskResidentSlots) {
			s.persistQueuedTaskCancellation(ctx, task)
			return
		}
		defer releaseTaskSlot(s.taskResidentSlots)
	}
	admission := &taskExecutionAdmission{store: s, tenant: s.taskTenantSlot(task.TenantID), execution: s.taskExecutionSlots}
	if !admission.acquire(ctx) {
		s.persistQueuedTaskCancellation(ctx, task)
		return
	}
	defer admission.release()
	ctx = context.WithValue(ctx, taskIngestAdmissionKey{}, admission)
	s.runTask(ctx, cancel, task)
}

func taskRetainsDataDuringWALWait(taskType string) bool {
	switch taskType {
	case TaskTypeBulkImport, TaskTypeReplayDeadLetter, TaskTypeTenantRestore, TaskTypeTenantRestoreDrill:
		return true
	default:
		return false
	}
}

type taskIngestAdmissionKey struct{}

func (s *TenantStore) taskAdmissionFromContext(ctx context.Context) (*taskExecutionAdmission, bool) {
	if ctx == nil {
		return nil, false
	}
	admission, ok := ctx.Value(taskIngestAdmissionKey{}).(*taskExecutionAdmission)
	if !ok || admission == nil {
		return nil, false
	}
	if admission.store != nil && admission.store != s {
		return nil, false
	}
	return admission, true
}

// Used by task execution paths to yield/reacquire bounded execution slots.
// Protected by mu so concurrent/deferred paths cannot race on held state.
type taskExecutionAdmission struct {
	mu                sync.Mutex
	store             *TenantStore
	tenant, execution chan struct{}
	held              bool
	gen               uint64
}

func (a *taskExecutionAdmission) acquire(ctx context.Context) bool {
	a.mu.Lock()
	if a.held {
		a.mu.Unlock()
		return true
	}
	gen := a.gen
	a.mu.Unlock()

	if a.tenant != nil && !acquireTaskSlot(ctx, a.tenant) {
		return false
	}
	if !acquireTaskSlot(ctx, a.execution) {
		if a.tenant != nil {
			releaseTaskSlot(a.tenant)
		}
		return false
	}

	a.mu.Lock()
	if a.held || a.gen != gen {
		held := a.held
		a.mu.Unlock()
		releaseTaskSlot(a.execution)
		if a.tenant != nil {
			releaseTaskSlot(a.tenant)
		}
		return held
	}
	a.held = true
	a.mu.Unlock()
	return true
}

func (a *taskExecutionAdmission) release() {
	a.mu.Lock()
	a.gen++
	if !a.held {
		a.mu.Unlock()
		return
	}
	a.held = false
	a.mu.Unlock()

	releaseTaskSlot(a.execution)
	if a.tenant != nil {
		releaseTaskSlot(a.tenant)
	}
}

func (s *TenantStore) persistQueuedTaskCancellation(ctx context.Context, task Task) {
	writeCtx, cancel := s.taskFinalizationContext(ctx)
	defer cancel()
	current := s.taskStateOrLocal(writeCtx, task)
	if taskTerminal(current.Status) {
		return
	}
	now := time.Now().UTC()
	current.Status = TaskStatusCanceled
	current.Phase = TaskStatusCanceled
	current.Error = TaskStatusCanceled
	current.UpdatedAt = now
	current.FinishedAt = now
	s.trySaveTask(writeCtx, current)
}

func (s *TenantStore) taskTenantSlot(tenantID string) chan struct{} {
	return s.taskTenantSlots[taskTenantStripe(
		tenantID,
		len(s.taskTenantSlots),
	)]
}

func taskTenantStripe(tenantID string, stripes int) int {
	var hash uint32 = 2166136261
	for i := 0; i < len(tenantID); i++ {
		hash ^= uint32(tenantID[i])
		hash *= 16777619
	}
	return int(hash % uint32(stripes))
}

func acquireTaskSlot(ctx context.Context, slot chan struct{}) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case slot <- struct{}{}:
		if ctx.Err() != nil {
			releaseTaskSlot(slot)
			return false
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func releaseTaskSlot(slot chan struct{}) {
	<-slot
}
