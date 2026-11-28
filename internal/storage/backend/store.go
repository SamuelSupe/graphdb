// Package backend defines the unified object storage backend contract,
// conditional write/delete invariants, and in-memory backend implementation.
package backend

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrNotFound                     = errors.New("object not found")
	ErrConflict                     = errors.New("object write conflict")
	ErrLeaseHeld                    = errors.New("writer lease is held")
	ErrConditionalWriteUnsupported  = errors.New("conditional write unsupported")
	ErrConditionalDeleteUnsupported = errors.New("conditional delete unsupported")
	ErrTenantDisabled               = errors.New("tenant disabled")
	ErrTenantDeleted                = errors.New("tenant deleted")
)

type ObjectInfo struct {
	Key  string
	Size int64
	ETag string
}

type ObjectMeta struct {
	Key    string
	ETag   string
	Exists bool
}

type BeforeImage struct {
	Data []byte
	Meta ObjectMeta
}

type PutCondition struct {
	before      *BeforeImage
	IfMatch     string
	IfNoneMatch bool
}

// WithBeforeImage attaches an optional pre-read image for rollback or fence
// verification without exposing internal fields across package boundaries.
func (c PutCondition) WithBeforeImage(data []byte, meta ObjectMeta) PutCondition {
	c.before = &BeforeImage{Data: data, Meta: meta}
	return c
}

// BeforeImage returns the attached before-image, if any.
func (c PutCondition) BeforeImage() *BeforeImage {
	return c.before
}

// ClearBeforeImage returns a copy of c without the attached before-image.
func (c PutCondition) ClearBeforeImage() PutCondition {
	c.before = nil
	return c
}

type ObjectStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error)
	Put(ctx context.Context, key string, data []byte) error
	PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error)
	Delete(ctx context.Context, key string) error
	DeleteConditional(ctx context.Context, key string, condition PutCondition) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

func ContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func ValidateKey(key string) error {
	if key == "" {
		return fmt.Errorf("object key is required")
	}
	return nil
}

func CheckCondition(condition PutCondition, currentETag string, exists bool) error {
	if condition.IfNoneMatch && exists {
		return ErrConflict
	}
	if condition.IfMatch != "" {
		if !exists || currentETag != condition.IfMatch {
			return ErrConflict
		}
	}
	return nil
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// MemoryStore is an in-memory ObjectStore implementing identical conditional
// ETag and change-listener semantics as local FileStore.
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
	if err := ContextErr(ctx); err != nil {
		return nil, err
	}
	if err := ValidateKey(key); err != nil {
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
	if err := ContextErr(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	if err := ValidateKey(key); err != nil {
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
	if err := ContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := ValidateKey(key); err != nil {
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
	if err := ContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := ValidateKey(key); err != nil {
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
	if err := ContextErr(ctx); err != nil {
		return ObjectMeta{Key: key, ETag: currentETag, Exists: exists}, err
	}
	if err := CheckCondition(condition, currentETag, exists); err != nil {
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
	if err := ContextErr(ctx); err != nil {
		return err
	}
	if err := ValidateKey(key); err != nil {
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
	if err := ContextErr(ctx); err != nil {
		return err
	}
	exists := false
	currentETag := ""
	if _, ok := s.objects[key]; ok {
		exists = true
		currentETag = s.etags[key]
	}
	if err := CheckCondition(condition, currentETag, exists); err != nil {
		return err
	}
	changed = true
	delete(s.objects, key)
	delete(s.etags, key)
	return nil
}

func (s *MemoryStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := ContextErr(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	items := make([]ObjectInfo, 0)
	for key, data := range s.objects {
		if err := ContextErr(ctx); err != nil {
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
