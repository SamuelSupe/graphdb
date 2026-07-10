package storage

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
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

type objectStoreUnwrapper interface {
	UnwrapObjectStore() ObjectStore
}

func NewMeteredObjectStore(inner ObjectStore, pressure *WritePressure, observer ObjectOperationObserver) *MeteredObjectStore {
	return &MeteredObjectStore{Inner: inner, Pressure: pressure, Observer: observer}
}

func FindMeteredObjectStore(objects ObjectStore) *MeteredObjectStore {
	for objects != nil {
		if metered, ok := objects.(*MeteredObjectStore); ok {
			return metered
		}
		unwrapper, ok := objects.(objectStoreUnwrapper)
		if !ok {
			return nil
		}
		next := unwrapper.UnwrapObjectStore()
		if next == objects {
			return nil
		}
		objects = next
	}
	return nil
}

func (s *MeteredObjectStore) Get(ctx context.Context, key string) (data []byte, err error) {
	ctx, done := s.start(ctx, "get", key, -1)
	defer func() { done(err) }()
	return s.Inner.Get(ctx, key)
}

func (s *MeteredObjectStore) GetWithMeta(ctx context.Context, key string) (data []byte, meta ObjectMeta, err error) {
	ctx, done := s.start(ctx, "get_with_meta", key, -1)
	defer func() { done(err) }()
	return s.Inner.GetWithMeta(ctx, key)
}

func (s *MeteredObjectStore) Head(ctx context.Context, key string) (meta ObjectMeta, err error) {
	ctx, done := s.start(ctx, "head", key, -1)
	defer func() { done(err) }()
	if head, ok := s.Inner.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err = s.Inner.GetWithMeta(ctx, key)
	return meta, err
}

func (s *MeteredObjectStore) Put(ctx context.Context, key string, data []byte) (err error) {
	ctx, done := s.start(ctx, "put", key, len(data))
	defer func() { done(err) }()
	return s.Inner.Put(ctx, key, data)
}

func (s *MeteredObjectStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (meta ObjectMeta, err error) {
	ctx, done := s.start(ctx, "put_conditional", key, len(data))
	defer func() { done(err) }()
	return s.Inner.PutConditional(ctx, key, data, condition)
}

func (s *MeteredObjectStore) Delete(ctx context.Context, key string) (err error) {
	ctx, done := s.start(ctx, "delete", key, -1)
	defer func() { done(err) }()
	return s.Inner.Delete(ctx, key)
}

func (s *MeteredObjectStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) (err error) {
	ctx, done := s.start(ctx, "delete_conditional", key, -1)
	defer func() { done(err) }()
	return s.Inner.DeleteConditional(ctx, key, condition)
}

func (s *MeteredObjectStore) List(ctx context.Context, prefix string) (items []ObjectInfo, err error) {
	ctx, done := s.start(ctx, "list", prefix, -1)
	defer func() { done(err) }()
	return s.Inner.List(ctx, prefix)
}

func (s *MeteredObjectStore) start(ctx context.Context, operation string, key string, bytes int) (context.Context, func(error)) {
	start := time.Now()
	attrs := []attribute.KeyValue{
		attribute.String("graphdb.object.operation", operation),
		attribute.String("graphdb.object.kind", objectKeyKind(key)),
	}
	if bytes >= 0 {
		attrs = append(attrs, attribute.Int("graphdb.object.bytes", bytes))
	}
	ctx, span := startStorageSpan(ctx, "graphdb.object_store."+operation, attrs...)
	return ctx, func(err error) {
		duration := time.Since(start)
		if s.Pressure != nil {
			s.Pressure.RecordObjectOperation(duration, err)
		}
		if s.Observer != nil {
			s.Observer.RecordObjectStoreOperation(operation, objectOperationStatus(err), duration)
		}
		span.SetAttributes(attribute.String("graphdb.object.status", objectOperationStatus(err)))
		endStorageSpan(span, err)
	}
}

func objectOperationStatus(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, ErrConditionalDeleteUnsupported):
		return "unsupported"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	default:
		return "error"
	}
}
