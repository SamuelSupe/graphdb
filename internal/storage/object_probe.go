package storage

import (
	"context"
	"fmt"
	"time"
)

type ObjectStoreStatus struct {
	Available bool      `json:"available"`
	CheckedAt time.Time `json:"checked_at"`
	LastError string    `json:"last_error,omitempty"`
}

type objectStoreProber interface {
	Probe(context.Context) error
}

type objectStoreProbeCall struct {
	done     chan struct{}
	status   ObjectStoreStatus
	canceled bool
}

func (s *TenantStore) ObjectStoreStatus(ctx context.Context) ObjectStoreStatus {
	for {
		s.objectProbeMu.Lock()
		if active := s.objectProbeActive; active != nil {
			s.objectProbeMu.Unlock()
			select {
			case <-active.done:
				if active.canceled && ctx.Err() == nil {
					continue
				}
				return active.status
			case <-ctx.Done():
				return ObjectStoreStatus{
					Available: false,
					CheckedAt: time.Now().UTC(),
					LastError: ctx.Err().Error(),
				}
			}
		}
		active := &objectStoreProbeCall{done: make(chan struct{})}
		s.objectProbeActive = active
		s.objectProbeMu.Unlock()

		status := ObjectStoreStatus{Available: true, CheckedAt: time.Now().UTC()}
		err := probeObjectStore(ctx, s.Objects, s.Prefix)
		if err != nil {
			status.Available = false
			status.LastError = err.Error()
		}
		s.objectProbeMu.Lock()
		active.status = status
		active.canceled = loadCanceledByContext(ctx, err)
		if s.objectProbeActive == active {
			s.objectProbeActive = nil
		}
		close(active.done)
		s.objectProbeMu.Unlock()
		return status
	}
}

func probeObjectStore(ctx context.Context, objects ObjectStore, prefix string) error {
	if objects == nil {
		return fmt.Errorf("object store is not configured")
	}
	switch store := objects.(type) {
	case *WriterObjectCache:
		return probeObjectStore(ctx, store.Inner, prefix)
	case *MeteredObjectStore:
		started := time.Now()
		err := probeObjectStore(ctx, store.Inner, prefix)
		if store.Observer != nil {
			store.Observer.RecordObjectStoreOperation("probe", objectOperationStatus(err), time.Since(started))
		}
		return err
	case *ReadProtectedObjectStore:
		return probeObjectStore(ctx, store.Inner, prefix)
	case *DelayedReadObjectStore:
		if err := store.wait(ctx); err != nil {
			return err
		}
		return probeObjectStore(ctx, store.Inner, prefix)
	case *SingleWriterObjectStore:
		return probeObjectStore(ctx, store.Inner, prefix)
	}
	if prober, ok := objects.(objectStoreProber); ok {
		return prober.Probe(ctx)
	}
	if unwrapper, ok := objects.(objectStoreUnwrapper); ok {
		next := unwrapper.UnwrapObjectStore()
		if next != nil && next != objects {
			return probeObjectStore(ctx, next, prefix)
		}
	}
	_, _, err := listObjectPage(ctx, objects, prefix, "", 1)
	return err
}
