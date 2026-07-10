package storage

import (
	"context"
	"path"
	"strings"
)

const (
	maxObjectKeyCacheEntries    = 100_000
	maxObjectPrefixCacheEntries = 4_096
)

func (s *TenantStore) objectKeyMayExist(ctx context.Context, key string) (bool, error) {
	prefix := path.Dir(key) + "/"
	s.lockMu.Lock()
	if _, ok := s.objectKeyCache[key]; ok {
		s.lockMu.Unlock()
		return true, nil
	}
	if _, ok := s.objectPrefixCache[prefix]; ok {
		s.lockMu.Unlock()
		return false, nil
	}
	s.lockMu.Unlock()

	objects, err := s.Objects.List(ctx, prefix)
	if err != nil {
		return true, nil
	}
	found := false
	for _, object := range objects {
		if object.Key == key {
			found = true
			break
		}
	}
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if _, ok := s.objectKeyCache[key]; ok {
		return true, nil
	}
	// A very large prefix is cheaper and safer to query again than to retain
	// forever. Do not mark it complete unless all of its keys fit the bound.
	if len(objects) > maxObjectKeyCacheEntries {
		return found, nil
	}
	if len(s.objectPrefixCache) >= maxObjectPrefixCacheEntries ||
		len(s.objectKeyCache)+len(objects) > maxObjectKeyCacheEntries {
		s.resetObjectKeyCacheLocked()
	}
	s.objectPrefixCache[prefix] = struct{}{}
	for _, object := range objects {
		if strings.HasSuffix(object.Key, ".parquet") {
			s.objectKeyCache[object.Key] = struct{}{}
		}
	}
	_, ok := s.objectKeyCache[key]
	return ok, nil
}

func (s *TenantStore) markObjectKeyCached(key string) {
	prefix := path.Dir(key) + "/"
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	if _, exists := s.objectKeyCache[key]; !exists && len(s.objectKeyCache) >= maxObjectKeyCacheEntries {
		s.resetObjectKeyCacheLocked()
	}
	if _, exists := s.objectPrefixCache[prefix]; !exists && len(s.objectPrefixCache) >= maxObjectPrefixCacheEntries {
		s.resetObjectKeyCacheLocked()
	}
	s.objectKeyCache[key] = struct{}{}
	s.objectPrefixCache[prefix] = struct{}{}
}

func (s *TenantStore) resetObjectKeyCacheLocked() {
	s.objectKeyCache = map[string]struct{}{}
	s.objectPrefixCache = map[string]struct{}{}
}

func (s *TenantStore) clearObjectKeyPrefix(prefix string) {
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
	for key := range s.objectKeyCache {
		if strings.HasPrefix(key, prefix) {
			delete(s.objectKeyCache, key)
		}
	}
	for cachedPrefix := range s.objectPrefixCache {
		if strings.HasPrefix(cachedPrefix, prefix) || strings.HasPrefix(prefix, cachedPrefix) {
			delete(s.objectPrefixCache, cachedPrefix)
		}
	}
}
