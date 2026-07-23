package storage

import (
	"context"
	"errors"
	"sync"
)

const singleWriterLockStripes = 64

// SingleWriterObjectStore preserves GGraphDB's conditional-write API for a
// provider that only offers atomic create-if-absent. Its read-check-write path
// is safe only when every writer is in this process; it is not a distributed
// CAS implementation.
type SingleWriterObjectStore struct {
	Inner ObjectStore
	locks [singleWriterLockStripes]sync.Mutex
}

func NewSingleWriterObjectStore(inner ObjectStore) *SingleWriterObjectStore {
	return &SingleWriterObjectStore{Inner: inner}
}

func (s *SingleWriterObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	return s.Inner.Get(ctx, key)
}

func (s *SingleWriterObjectStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	return s.Inner.GetWithMeta(ctx, key)
}

func (s *SingleWriterObjectStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	return objectMeta(ctx, s.Inner, key)
}

func (s *SingleWriterObjectStore) Put(ctx context.Context, key string, data []byte) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	unlock := s.lockKey(key)
	defer unlock()
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	return s.Inner.Put(ctx, key, data)
}

func (s *SingleWriterObjectStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateNativeCondition(condition); err != nil {
		return ObjectMeta{Key: key}, err
	}
	unlock := s.lockKey(key)
	defer unlock()
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if condition.IfNoneMatch {
		return s.Inner.PutConditional(ctx, key, data, condition)
	}
	if condition.IfMatch != "" {
		current, err := objectMeta(ctx, s.Inner, key)
		if errors.Is(err, ErrNotFound) {
			return ObjectMeta{Key: key}, ErrConflict
		}
		if err != nil {
			return ObjectMeta{Key: key}, err
		}
		if current.ETag == "" || current.ETag != condition.IfMatch {
			return current, ErrConflict
		}
	}
	if err := s.Inner.Put(ctx, key, data); err != nil {
		return ObjectMeta{Key: key}, err
	}
	return objectMeta(ctx, s.Inner, key)
}

func (s *SingleWriterObjectStore) Delete(ctx context.Context, key string) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	unlock := s.lockKey(key)
	defer unlock()
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	return s.Inner.Delete(ctx, key)
}

func (s *SingleWriterObjectStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	if err := validateNativeCondition(condition); err != nil {
		return err
	}
	unlock := s.lockKey(key)
	defer unlock()
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if condition.IfNoneMatch {
		if _, err := objectMeta(ctx, s.Inner, key); errors.Is(err, ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return ErrConflict
	}
	if condition.IfMatch != "" {
		current, err := objectMeta(ctx, s.Inner, key)
		if errors.Is(err, ErrNotFound) {
			return ErrConflict
		}
		if err != nil {
			return err
		}
		if current.ETag == "" || current.ETag != condition.IfMatch {
			return ErrConflict
		}
	}
	return s.Inner.Delete(ctx, key)
}

func (s *SingleWriterObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return s.Inner.List(ctx, prefix)
}

func (s *SingleWriterObjectStore) lockKey(key string) func() {
	var hash uint32 = 2166136261
	for i := 0; i < len(key); i++ {
		hash ^= uint32(key[i])
		hash *= 16777619
	}
	lock := &s.locks[int(hash)%len(s.locks)]
	lock.Lock()
	return lock.Unlock
}

var _ ObjectStore = (*SingleWriterObjectStore)(nil)
