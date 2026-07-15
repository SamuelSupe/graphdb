package storage

import (
	"context"
	"sync"
)

// Bound foreground priority so sustained writes cannot starve queued maintenance.
const foregroundTenantLockBurst = 8

type tenantLockWaiter struct {
	ready      chan struct{}
	foreground bool
	granted    bool
}

type tenantLock struct {
	held             bool
	refs             int
	foregroundGrants int
	foreground       []*tenantLockWaiter
	background       []*tenantLockWaiter
}

func (s *TenantStore) lockTenant(tenantID string) func() {
	unlock, err := s.lockTenantContext(context.Background(), tenantID, false)
	if err != nil {
		panic(err)
	}
	return unlock
}

func (s *TenantStore) lockTenantForeground(ctx context.Context, tenantID string) (func(), error) {
	return s.lockTenantContext(ctx, tenantID, true)
}

func (s *TenantStore) lockTenantMaintenance(ctx context.Context, tenantID string) (func(), error) {
	return s.lockTenantContext(ctx, tenantID, false)
}

func (s *TenantStore) lockTenantContext(ctx context.Context, tenantID string, foreground bool) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.lockMu.Lock()
	lock := s.tenantLocks[tenantID]
	if lock == nil {
		lock = &tenantLock{}
		s.tenantLocks[tenantID] = lock
	}
	lock.refs++
	if !lock.held {
		lock.held = true
		if foreground {
			lock.foregroundGrants = 1
		} else {
			lock.foregroundGrants = 0
		}
		s.lockMu.Unlock()
		return s.tenantUnlock(tenantID, lock), nil
	}

	waiter := &tenantLockWaiter{ready: make(chan struct{}), foreground: foreground}
	if foreground {
		lock.foreground = append(lock.foreground, waiter)
	} else {
		lock.background = append(lock.background, waiter)
	}
	s.lockMu.Unlock()

	select {
	case <-waiter.ready:
		// Cancellation can race with ownership handoff; release a granted lock
		// instead of returning it to an already canceled operation.
		if err := ctx.Err(); err != nil {
			s.lockMu.Lock()
			s.releaseTenantLockLocked(tenantID, lock)
			s.lockMu.Unlock()
			return nil, err
		}
		return s.tenantUnlock(tenantID, lock), nil
	case <-ctx.Done():
		s.lockMu.Lock()
		if waiter.granted {
			s.releaseTenantLockLocked(tenantID, lock)
		} else {
			s.removeTenantLockWaiterLocked(lock, waiter)
			lock.refs--
			s.deleteIdleTenantLockLocked(tenantID, lock)
		}
		s.lockMu.Unlock()
		return nil, ctx.Err()
	}
}

func (s *TenantStore) tenantUnlock(tenantID string, lock *tenantLock) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.lockMu.Lock()
			s.releaseTenantLockLocked(tenantID, lock)
			s.lockMu.Unlock()
		})
	}
}

func (s *TenantStore) releaseTenantLockLocked(tenantID string, lock *tenantLock) {
	lock.refs--
	next := nextTenantLockWaiter(lock)
	if next == nil {
		lock.held = false
		lock.foregroundGrants = 0
		s.deleteIdleTenantLockLocked(tenantID, lock)
		return
	}
	next.granted = true
	close(next.ready)
}

func nextTenantLockWaiter(lock *tenantLock) *tenantLockWaiter {
	if len(lock.foreground) > 0 && (len(lock.background) == 0 || lock.foregroundGrants < foregroundTenantLockBurst) {
		next := lock.foreground[0]
		lock.foreground = lock.foreground[1:]
		lock.foregroundGrants++
		return next
	}
	if len(lock.background) > 0 {
		next := lock.background[0]
		lock.background = lock.background[1:]
		lock.foregroundGrants = 0
		return next
	}
	if len(lock.foreground) > 0 {
		next := lock.foreground[0]
		lock.foreground = lock.foreground[1:]
		lock.foregroundGrants++
		return next
	}
	return nil
}

func (s *TenantStore) removeTenantLockWaiterLocked(lock *tenantLock, waiter *tenantLockWaiter) {
	queue := &lock.background
	if waiter.foreground {
		queue = &lock.foreground
	}
	for i, current := range *queue {
		if current != waiter {
			continue
		}
		copy((*queue)[i:], (*queue)[i+1:])
		*queue = (*queue)[:len(*queue)-1]
		return
	}
}

func (s *TenantStore) deleteIdleTenantLockLocked(tenantID string, lock *tenantLock) {
	if lock.refs == 0 && !lock.held && s.tenantLocks[tenantID] == lock {
		delete(s.tenantLocks, tenantID)
	}
}
