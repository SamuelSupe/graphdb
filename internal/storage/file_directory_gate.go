package storage

import (
	"context"
	"strings"
	"sync"
)

// A directory change freezes just its subtree while preparing retained files.
// Waiters release the global IO permit so the final rename cannot deadlock.
func (s *FileStore) lockDirectoryIOForKey(ctx context.Context, key string) (func(), error) {
	for {
		unlock, err := s.lockDirectoryIO(ctx)
		if err != nil || s.runtime == nil {
			return unlock, err
		}
		r := s.runtime
		r.mu.Lock()
		wait := s.directoryChangeLocked(key)
		r.mu.Unlock()
		if wait == nil {
			return unlock, nil
		}
		unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (s *FileStore) directoryChangeLocked(key string) <-chan struct{} {
	for prefix, done := range s.runtime.directoryChanges {
		if strings.HasPrefix(key, prefix) || strings.HasPrefix(prefix, key) {
			return done
		}
	}
	return nil
}

func (s *FileStore) beginDirectoryChange(ctx context.Context, key string) (func(), error) {
	release, err := s.beginLifecycleOperation(ctx)
	if err != nil || s.runtime == nil {
		return release, err
	}
	prefix := strings.TrimSuffix(key, "/") + "/"
	r := s.runtime
	for {
		unlock, err := s.lockDirectoryIOWeight(ctx, directoryIOCapacity)
		if err != nil {
			release()
			return nil, err
		}
		r.mu.Lock()
		wait := s.directoryChangeLocked(prefix)
		if wait == nil {
			if r.directoryChanges == nil {
				r.directoryChanges = make(map[string]chan struct{})
			}
			done := make(chan struct{})
			r.directoryChanges[prefix] = done
			r.mu.Unlock()
			unlock()
			return sync.OnceFunc(func() {
				r.mu.Lock()
				delete(r.directoryChanges, prefix)
				close(done)
				r.mu.Unlock()
				release()
			}), nil
		}
		r.mu.Unlock()
		unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
}
