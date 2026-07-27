package storage

import (
	"context"
	"sync"
)

type ReadProtectionConfig struct {
	MaxConcurrent int
	Singleflight  bool
}

type ReadProtectedObjectStore struct {
	Inner        ObjectStore
	limit        chan struct{}
	singleflight bool

	mu    sync.Mutex
	reads map[string]*readObjectCall
}

type readObjectCall struct {
	done     chan struct{}
	data     []byte
	meta     ObjectMeta
	err      error
	canceled bool
}

func NewReadProtectedObjectStore(inner ObjectStore, config ReadProtectionConfig) *ReadProtectedObjectStore {
	store := &ReadProtectedObjectStore{Inner: inner, singleflight: config.Singleflight}
	if config.MaxConcurrent > 0 {
		store.limit = make(chan struct{}, config.MaxConcurrent)
	}
	if config.Singleflight {
		store.reads = map[string]*readObjectCall{}
	}
	return store
}

func (s *ReadProtectedObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := s.getWithMeta(ctx, key)
	return data, err
}

func (s *ReadProtectedObjectStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	return s.getWithMeta(ctx, key)
}

func (s *ReadProtectedObjectStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	release, err := s.acquireRead(ctx)
	if err != nil {
		return ObjectMeta{Key: key}, err
	}
	defer release()
	if head, ok := s.Inner.(objectHeadStore); ok {
		return head.Head(ctx, key)
	}
	_, meta, err := s.Inner.GetWithMeta(ctx, key)
	return meta, err
}

func (s *ReadProtectedObjectStore) Put(ctx context.Context, key string, data []byte) error {
	return s.Inner.Put(ctx, key, data)
}

func (s *ReadProtectedObjectStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	return s.Inner.PutConditional(ctx, key, data, condition)
}

func (s *ReadProtectedObjectStore) Delete(ctx context.Context, key string) error {
	return s.Inner.Delete(ctx, key)
}

func (s *ReadProtectedObjectStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	return s.Inner.DeleteConditional(ctx, key, condition)
}

func (s *ReadProtectedObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	release, err := s.acquireRead(ctx)
	if err != nil {
		return nil, err
	}
	defer release()
	return s.Inner.List(ctx, prefix)
}

func (s *ReadProtectedObjectStore) getWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if !s.singleflight {
		return s.loadWithMeta(ctx, key)
	}
	for {
		call, owner, err := s.beginRead(ctx, key)
		if err != nil {
			return nil, ObjectMeta{Key: key}, err
		}
		if !owner {
			select {
			case <-call.done:
				if call.canceled && ctx.Err() == nil {
					continue
				}
				return cloneReadCall(call)
			case <-ctx.Done():
				return nil, ObjectMeta{Key: key}, ctx.Err()
			}
		}
		call.data, call.meta, call.err = s.loadWithMeta(ctx, key)
		call.canceled = loadCanceledByContext(ctx, call.err)
		s.finishRead(key, call)
		return cloneReadCall(call)
	}
}

func (s *ReadProtectedObjectStore) loadWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	release, err := s.acquireRead(ctx)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	defer release()
	return s.Inner.GetWithMeta(ctx, key)
}

func (s *ReadProtectedObjectStore) acquireRead(ctx context.Context) (func(), error) {
	if s.limit == nil {
		return func() {}, nil
	}
	select {
	case s.limit <- struct{}{}:
		return func() { <-s.limit }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *ReadProtectedObjectStore) beginRead(ctx context.Context, key string) (*readObjectCall, bool, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if call := s.reads[key]; call != nil {
		return call, false, nil
	}
	call := &readObjectCall{done: make(chan struct{})}
	s.reads[key] = call
	return call, true, nil
}

func (s *ReadProtectedObjectStore) finishRead(key string, call *readObjectCall) {
	s.mu.Lock()
	if s.reads[key] == call {
		delete(s.reads, key)
	}
	s.mu.Unlock()
	close(call.done)
}

func cloneReadCall(call *readObjectCall) ([]byte, ObjectMeta, error) {
	data := append([]byte(nil), call.data...)
	return data, call.meta, call.err
}
