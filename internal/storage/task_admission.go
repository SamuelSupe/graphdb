package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
		admission := &taskExecutionAdmission{tenant: tenantSlot, execution: s.taskExecutionSlots, held: true}
		return context.WithValue(ctx, taskIngestAdmissionKey{}, admission), admission.release, nil
	default:
		releaseTaskSlot(tenantSlot)
		return ctx, nil, ErrMaintenanceBusy
	}
}

func (s *TenantStore) admitTask(ctx context.Context, task Task) (Task, bool, error) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if s.taskClosing {
		return Task{}, false, ErrTaskServiceClosed
	}
	key := taskActiveKey(task.TenantID, task.Type)
	if active, ok := s.taskActive[key]; ok {
		current, err := s.getTaskObject(ctx, active.TenantID, active.ID)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return Task{}, false, err
		}
		if err == nil && taskTerminal(current.Status) {
			delete(s.taskActive, key)
		} else {
			if !sameTaskParams(active.Params, task.Params) {
				return Task{}, false, fmt.Errorf("%w: tenant %q already has %s task %q with different parameters", ErrConflict, task.TenantID, task.Type, active.ID)
			}
			if err == nil {
				active = current
			}
			return active, true, nil
		}
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
	defer s.releaseTaskAdmission(task)
	defer s.unregisterTaskCancel(task.TenantID, task.ID)
	stopWatch := s.watchTaskCancellation(task, cancel)
	defer stopWatch()
	if taskRetainsDataDuringWALWait(task.Type) {
		// Acquire before the tenant/execution slots so waiting for memory cannot
		// prevent compact from relieving another task's WAL backpressure.
		if !acquireTaskSlot(ctx, s.taskResidentSlots) {
			s.persistQueuedTaskCancellation(ctx, task)
			return
		}
		defer releaseTaskSlot(s.taskResidentSlots)
	}
	admission := &taskExecutionAdmission{tenant: s.taskTenantSlot(task.TenantID), execution: s.taskExecutionSlots}
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

// Used only by the task's sequential execution path. WAL waits release these
// slots, then reacquire them before resuming work.
type taskExecutionAdmission struct {
	tenant, execution chan struct{}
	held              bool
}

func (a *taskExecutionAdmission) acquire(ctx context.Context) bool {
	if !acquireTaskSlot(ctx, a.tenant) {
		return false
	}
	if !acquireTaskSlot(ctx, a.execution) {
		releaseTaskSlot(a.tenant)
		return false
	}
	a.held = true
	return true
}

func (a *taskExecutionAdmission) release() {
	if a.held {
		releaseTaskSlot(a.execution)
		releaseTaskSlot(a.tenant)
		a.held = false
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

func (s *TenantStore) reserveQueuedTask() bool {
	select {
	case s.taskQueueSlots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *TenantStore) admitIndexTaskWorker() error {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if s.taskClosing {
		return ErrTaskServiceClosed
	}
	select {
	case s.taskQueueSlots <- struct{}{}:
		s.taskWorkers.Add(1)
		return nil
	default:
		return fmt.Errorf("task queue is full")
	}
}

func (s *TenantStore) releaseQueuedTask() {
	select {
	case <-s.taskQueueSlots:
	default:
	}
}

func (s *TenantStore) taskTenantSlot(tenantID string) chan struct{} {
	return s.taskTenantSlots[taskTenantStripe(
		tenantID,
		len(s.taskTenantSlots),
	)]
}

func (s *TenantStore) indexTaskStartSlot(tenantID string) chan struct{} {
	return s.indexTaskStartSlots[taskTenantStripe(
		tenantID,
		len(s.indexTaskStartSlots),
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
