package storage

import (
	"context"
	"strings"
	"sync"
)

type afterPutObjectStore struct {
	ObjectStore
	mu     sync.Mutex
	suffix string
	after  func()
	fired  bool
}

func (s *afterPutObjectStore) PutConditional(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	meta, err := s.ObjectStore.PutConditional(ctx, key, data, condition)
	if err != nil {
		return meta, err
	}
	s.mu.Lock()
	fire := !s.fired && strings.HasSuffix(key, s.suffix)
	if fire {
		s.fired = true
	}
	s.mu.Unlock()
	if fire {
		s.after()
	}
	return meta, nil
}
