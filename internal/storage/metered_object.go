package storage

import (
	"context"
	"errors"
	"time"
)

type ObjectOperationObserver interface {
	RecordObjectStoreOperation(operation string, status string, duration time.Duration)
}

type MeteredObjectStore struct {
	Inner       ObjectStore
	Pressure    *WritePressure
	Observer    ObjectOperationObserver
	LatencyOnly bool
}

func NewMeteredObjectStore(inner ObjectStore, pressure *WritePressure, observer ObjectOperationObserver) *MeteredObjectStore {
	return &MeteredObjectStore{Inner: inner, Pressure: pressure, Observer: observer}
}

func (s *MeteredObjectStore) Get(ctx context.Context, key string) (data []byte, err error) {
	done := s.start("get")
	defer func() { done(err) }()
	return s.Inner.Get(ctx, key)
}

func (s *MeteredObjectStore) GetWithMeta(ctx context.Context, key string) (data []byte, meta ObjectMeta, err error) {
	done := s.start("get_with_meta")
	defer func() { done(err) }()
	return s.Inner.GetWithMeta(ctx, key)
}

func (s *MeteredObjectStore) Head(ctx context.Context, key string) (meta ObjectMeta, err error) {
	done := s.start("head")
	defer func() { done(err) }()
	if head, ok := s.Inner.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err = s.Inner.GetWithMeta(ctx, key)
	return meta, err
}

func (s *MeteredObjectStore) Put(ctx context.Context, key string, data []byte) (err error) {
	done := s.start("put")
	defer func() { done(err) }()
	return s.Inner.Put(ctx, key, data)
}

func (s *MeteredObjectStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (meta ObjectMeta, err error) {
	done := s.start("put_conditional")
	defer func() { done(err) }()
	return s.Inner.PutConditional(ctx, key, data, condition)
}

func (s *MeteredObjectStore) Delete(ctx context.Context, key string) (err error) {
	done := s.start("delete")
	defer func() { done(err) }()
	return s.Inner.Delete(ctx, key)
}

func (s *MeteredObjectStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) (err error) {
	done := s.start("delete_conditional")
	defer func() { done(err) }()
	return s.Inner.DeleteConditional(ctx, key, condition)
}

func (s *MeteredObjectStore) List(ctx context.Context, prefix string) (items []ObjectInfo, err error) {
	done := s.start("list")
	defer func() { done(err) }()
	return s.Inner.List(ctx, prefix)
}

func (s *MeteredObjectStore) start(operation string) func(error) {
	start := time.Now()
	return func(err error) {
		duration := time.Since(start)
		if s.Pressure != nil {
			s.Pressure.RecordObjectOperation(duration, err)
		}
		if s.Observer != nil {
			s.Observer.RecordObjectStoreOperation(operation, objectOperationStatus(err), duration)
		}
	}
}

func objectOperationStatus(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	default:
		return "error"
	}
}
