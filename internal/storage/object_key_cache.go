package storage

import (
	"context"
	"errors"
	"strings"
)

const maxObjectKeyCacheEntries = 100_000

func (s *TenantStore) objectKeyMayExist(ctx context.Context, key string) (bool, error) {
	s.lockMu.Lock()
	if _, ok := s.objectKeyCache[key]; ok {
		s.lockMu.Unlock()
		return true, nil
	}
	s.lockMu.Unlock()

	meta, err := objectMeta(ctx, s.Objects, key)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		// The caller will perform the authoritative Get and return its richer
		// error. Failing open here avoids turning a transient HEAD limitation
		// into a false cache miss.
		return true, nil
	}
	if !meta.Exists {
		return false, nil
	}
	s.markObjectKeyCached(key)
	return true, nil
}

func (s *TenantStore) markObjectKeyCached(key string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if _, exists := s.objectKeyCache[key]; !exists && len(s.objectKeyCache) >= maxObjectKeyCacheEntries {
		s.resetObjectKeyCacheLocked()
	}
	s.objectKeyCache[key] = struct{}{}
}

func (s *TenantStore) resetObjectKeyCacheLocked() {
	s.objectKeyCache = map[string]struct{}{}
}

func (s *TenantStore) clearObjectKeyPrefix(prefix string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	for key := range s.objectKeyCache {
		if strings.HasPrefix(key, prefix) {
			delete(s.objectKeyCache, key)
		}
	}
}
