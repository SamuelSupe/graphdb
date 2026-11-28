package storage

import (
	"context"
	"errors"
	"strings"
)

const maxObjectKeyCacheEntries = 100_000

func (s *TenantStore) objectKeyMayExist(ctx context.Context, key string) (bool, error) {
	s.objectKeyMu.Lock()
	_, ok := s.objectKeyCache[key]
	s.objectKeyMu.Unlock()
	if ok {
		return true, nil
	}

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
	s.objectKeyMu.Lock()
	defer s.objectKeyMu.Unlock()
	if s.objectKeyCache == nil {
		s.objectKeyCache = map[string]struct{}{}
	}
	if _, exists := s.objectKeyCache[key]; !exists && len(s.objectKeyCache) >= maxObjectKeyCacheEntries {
		s.resetObjectKeyCacheLocked()
	}
	if _, exists := s.objectKeyCache[key]; !exists {
		s.objectKeyCache[key] = struct{}{}
		s.indexObjectKeyLocked(key)
	}
}

func (s *TenantStore) resetObjectKeyCache() {
	s.objectKeyMu.Lock()
	defer s.objectKeyMu.Unlock()
	s.resetObjectKeyCacheLocked()
}

func (s *TenantStore) resetObjectKeyCacheLocked() {
	s.objectKeyCache = map[string]struct{}{}
	s.objectKeyPrefixes = map[string]map[string]struct{}{}
	s.objectKeyIndexed = 0
}

func (s *TenantStore) objectKeyBucket(key string) string {
	tenantsPrefix := "tenants/"
	if s.Prefix != "" {
		tenantsPrefix = s.Prefix + "/tenants/"
	}
	if !strings.HasPrefix(key, tenantsPrefix) {
		return ""
	}
	rest := key[len(tenantsPrefix):]
	idx := strings.IndexByte(rest, '/')
	if idx <= 0 {
		return ""
	}
	return key[:len(tenantsPrefix)+idx+1]
}

func (s *TenantStore) indexObjectKeyLocked(key string) {
	if s.objectKeyPrefixes == nil {
		s.objectKeyPrefixes = map[string]map[string]struct{}{}
	}
	bucket := s.objectKeyBucket(key)
	keys := s.objectKeyPrefixes[bucket]
	if keys == nil {
		keys = map[string]struct{}{}
		s.objectKeyPrefixes[bucket] = keys
	}
	if _, exists := keys[key]; !exists {
		keys[key] = struct{}{}
		s.objectKeyIndexed++
	}
}

func (s *TenantStore) deleteObjectKeyCached(key string) {
	s.objectKeyMu.Lock()
	defer s.objectKeyMu.Unlock()
	if _, exists := s.objectKeyCache[key]; !exists {
		return
	}
	delete(s.objectKeyCache, key)
	bucket := s.objectKeyBucket(key)
	if keys, ok := s.objectKeyPrefixes[bucket]; ok {
		if _, exists := keys[key]; exists {
			delete(keys, key)
			s.objectKeyIndexed--
			if len(keys) == 0 {
				delete(s.objectKeyPrefixes, bucket)
			}
		}
	}
}

func (s *TenantStore) clearObjectKeyPrefix(prefix string) {
	s.objectKeyMu.Lock()
	defer s.objectKeyMu.Unlock()
	if prefix == "" {
		s.resetObjectKeyCacheLocked()
		return
	}
	if s.objectKeyIndexed == len(s.objectKeyCache) && s.objectKeyBucket(prefix) == prefix {
		if keys, ok := s.objectKeyPrefixes[prefix]; ok {
			for key := range keys {
				delete(s.objectKeyCache, key)
			}
			s.objectKeyIndexed -= len(keys)
			delete(s.objectKeyPrefixes, prefix)
		}
		return
	}
	for key := range s.objectKeyCache {
		if strings.HasPrefix(key, prefix) {
			delete(s.objectKeyCache, key)
			bucket := s.objectKeyBucket(key)
			if keys, ok := s.objectKeyPrefixes[bucket]; ok {
				if _, exists := keys[key]; exists {
					delete(keys, key)
					s.objectKeyIndexed--
					if len(keys) == 0 {
						delete(s.objectKeyPrefixes, bucket)
					}
				}
			}
		}
	}
}
