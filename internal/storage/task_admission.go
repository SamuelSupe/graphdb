package storage

import (
	"context"
	"fmt"
	"time"
)

const (
	defaultTaskExecutionLimit = 4
	defaultTaskQueueLimit     = 128
	defaultTaskTenantStripes  = 64
)

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

func (s *TenantStore) admitTask(task Task) (Task, bool, error) {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	key := taskActiveKey(task.TenantID, task.Type)
	if active, ok := s.taskActive[key]; ok {
		return active, true, nil
	}
	select {
	case s.taskQueueSlots <- struct{}{}:
		s.taskActive[key] = task
		return Task{}, false, nil
	default:
		return Task{}, false, fmt.Errorf("task queue is full")
	}
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
	if !acquireTaskSlot(ctx, s.taskTenantSlot(task.TenantID)) {
		s.persistQueuedTaskCancellation(ctx, task)
		return
	}
	defer releaseTaskSlot(s.taskTenantSlot(task.TenantID))
	if !acquireTaskSlot(ctx, s.taskExecutionSlots) {
		s.persistQueuedTaskCancellation(ctx, task)
		return
	}
	defer releaseTaskSlot(s.taskExecutionSlots)
	s.runTask(ctx, cancel, task)
}

func (s *TenantStore) persistQueuedTaskCancellation(ctx context.Context, task Task) {
	writeCtx := context.WithoutCancel(ctx)
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
