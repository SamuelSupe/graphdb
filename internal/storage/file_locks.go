package storage

import "sync"

type fileObjectLock struct {
	mu   sync.Mutex
	refs int
}

func (s *FileStore) lockObject(key string) func() {
	s.lockMu.Lock()
	if s.objectLocks == nil {
		s.objectLocks = map[string]*fileObjectLock{}
	}
	lock := s.objectLocks[key]
	if lock == nil {
		lock = &fileObjectLock{}
		s.objectLocks[key] = lock
	}
	lock.refs++
	s.lockMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()

		s.lockMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(s.objectLocks, key)
		}
		s.lockMu.Unlock()
	}
}
