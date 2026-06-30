package storage

import (
	"context"
	"time"
)

type DelayedReadObjectStore struct {
	Inner ObjectStore
	Delay time.Duration
}

func NewDelayedReadObjectStore(inner ObjectStore, delay time.Duration) ObjectStore {
	if delay <= 0 {
		return inner
	}
	return &DelayedReadObjectStore{Inner: inner, Delay: delay}
}

func (s *DelayedReadObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.Inner.Get(ctx, key)
}

func (s *DelayedReadObjectStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if err := s.wait(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return s.Inner.GetWithMeta(ctx, key)
}

func (s *DelayedReadObjectStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if err := s.wait(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if head, ok := s.Inner.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err := s.Inner.GetWithMeta(ctx, key)
	return meta, err
}

func (s *DelayedReadObjectStore) Put(ctx context.Context, key string, data []byte) error {
	return s.Inner.Put(ctx, key, data)
}

func (s *DelayedReadObjectStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	return s.Inner.PutConditional(ctx, key, data, condition)
}

func (s *DelayedReadObjectStore) Delete(ctx context.Context, key string) error {
	return s.Inner.Delete(ctx, key)
}

func (s *DelayedReadObjectStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	return s.Inner.DeleteConditional(ctx, key, condition)
}

func (s *DelayedReadObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return s.Inner.List(ctx, prefix)
}

func (s *DelayedReadObjectStore) wait(ctx context.Context) error {
	if s.Delay <= 0 {
		return nil
	}
	timer := time.NewTimer(s.Delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
