package storage

import (
	"context"
	"sync"
)

type coordinatedWriteContextMemoKey struct{}

type coordinatedWriteContextMemo struct {
	mu       sync.Mutex
	store    *TenantStore
	tenantID string
	snapshot WriteContextSnapshot
	head     CoordinationHead
	loaded   bool
}

func withFreshCoordinatedWriteContextMemo(ctx context.Context) context.Context {
	return context.WithValue(
		ctx,
		coordinatedWriteContextMemoKey{},
		&coordinatedWriteContextMemo{},
	)
}

func coordinatedWriteContextMemoFrom(
	ctx context.Context,
) *coordinatedWriteContextMemo {
	memo, _ := ctx.Value(
		coordinatedWriteContextMemoKey{},
	).(*coordinatedWriteContextMemo)
	return memo
}

func (memo *coordinatedWriteContextMemo) load(
	ctx context.Context,
	store *TenantStore,
	tenantID string,
) (WriteContextSnapshot, CoordinationHead, error) {
	memo.mu.Lock()
	defer memo.mu.Unlock()
	if memo.loaded && memo.store == store && memo.tenantID == tenantID {
		return memo.snapshot, memo.head, nil
	}
	snapshot, head, err := store.loadCoordinatedWriteContextFresh(
		ctx,
		tenantID,
	)
	if err == nil && !memo.loaded {
		memo.store = store
		memo.tenantID = tenantID
		memo.snapshot = snapshot
		memo.head = head
		memo.loaded = true
	}
	return snapshot, head, err
}
