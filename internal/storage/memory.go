package storage

import (
	"context"
	"sort"
	"strings"
	"sync"
)

type MemoryStore struct {
	mu         sync.RWMutex
	objects    map[string][]byte
	etags      map[string]string
	listeners  map[uint64]func(string)
	listenerID uint64
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: map[string][]byte{}, etags: map[string]string{}}
}

func (s *MemoryStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

func (s *MemoryStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, ObjectMeta{Key: key}, ErrNotFound
	}
	return append([]byte(nil), data...), ObjectMeta{Key: key, ETag: s.etags[key], Exists: true}, nil
}

func (s *MemoryStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.objects[key]; !ok {
		return ObjectMeta{Key: key}, ErrNotFound
	}
	return ObjectMeta{Key: key, ETag: s.etags[key], Exists: true}, nil
}

func (s *MemoryStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *MemoryStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	changed := false
	defer func() {
		if changed {
			s.changed(key)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.objects[key]
	currentETag := s.etags[key]
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key, ETag: currentETag, Exists: exists}, err
	}
	if err := checkCondition(condition, currentETag, exists); err != nil {
		return ObjectMeta{Key: key, ETag: currentETag, Exists: exists}, err
	}
	changed = true
	s.objects[key] = append([]byte(nil), data...)
	s.etags[key] = sha256Hex(data)
	return ObjectMeta{Key: key, ETag: s.etags[key], Exists: true}, nil
}

func (s *MemoryStore) Delete(ctx context.Context, key string) error {
	return s.DeleteConditional(ctx, key, PutCondition{})
}

func (s *MemoryStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	changed := false
	defer func() {
		if changed {
			s.changed(key)
		}
	}()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	exists := false
	currentETag := ""
	if _, ok := s.objects[key]; ok {
		exists = true
		currentETag = s.etags[key]
	}
	if err := checkCondition(condition, currentETag, exists); err != nil {
		return err
	}
	changed = true
	delete(s.objects, key)
	delete(s.etags, key)
	return nil
}

func (s *MemoryStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]ObjectInfo, 0)
	for key, data := range s.objects {
		if err := objectContextErr(ctx); err != nil {
			return nil, err
		}
		if strings.HasPrefix(key, prefix) {
			items = append(items, ObjectInfo{Key: key, Size: int64(len(data)), ETag: s.etags[key]})
		}
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})
	return items, nil
}

func (s *MemoryStore) OnChange(listener func(string)) func() {
	s.mu.Lock()
	s.listenerID++
	id := s.listenerID
	if s.listeners == nil {
		s.listeners = make(map[uint64]func(string))
	}
	s.listeners[id] = listener
	s.mu.Unlock()
	return sync.OnceFunc(func() { s.mu.Lock(); delete(s.listeners, id); s.mu.Unlock() })
}

func (s *MemoryStore) changed(key string) {
	s.mu.RLock()
	listeners := make([]func(string), 0, len(s.listeners))
	for _, listener := range s.listeners {
		listeners = append(listeners, listener)
	}
	s.mu.RUnlock()
	for _, listener := range listeners {
		listener(key)
	}
}
