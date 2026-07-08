package storage

import (
	"context"
	"path"
	"strings"
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
	s.lockMu.Lock()
	defer s.lockMu.Unlock()
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
	s.objectKeyCache[key] = struct{}{}
	s.objectPrefixCache[prefix] = struct{}{}
}
