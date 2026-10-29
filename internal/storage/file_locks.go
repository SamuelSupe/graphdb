package storage

import (
	"context"

	"golang.org/x/sync/semaphore"
)

type fileObjectLock struct {
	gate *semaphore.Weighted
	refs int
}

func (s *FileStore) lockObject(key string) func() {
	unlock, _ := s.lockObjectContext(context.Background(), key)
	return unlock
}

func (s *FileStore) lockObjectContext(ctx context.Context, key string) (func(), error) {
	s.lockMu.Lock()
	if s.objectLocks == nil {
		s.objectLocks = map[string]*fileObjectLock{}
	}
	lock := s.objectLocks[key]
	if lock == nil {
		lock = &fileObjectLock{gate: semaphore.NewWeighted(1)}
		s.objectLocks[key] = lock
	}
	lock.refs++
	s.lockMu.Unlock()

	releaseRef := func() {
		s.lockMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.objectLocks, key)
		}
		s.lockMu.Unlock()
	}
	if err := lock.gate.Acquire(ctx, 1); err != nil {
		releaseRef()
		return nil, err
	}
	return func() {
		lock.gate.Release(1)
		releaseRef()
	}, nil
}
