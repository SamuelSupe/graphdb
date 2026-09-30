package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"
)

type replicatedContextKey struct{}

type replicatedExecution struct {
	id       string
	at       time.Time
	mu       sync.Mutex
	sequence map[string]uint64
}

// ReplicatedContext fixes business timestamps and generated identities to the
// leader's log entry. Wall-clock deadlines and resource measurements stay local.
func ReplicatedContext(ctx context.Context, id string, at time.Time) context.Context {
	return context.WithValue(ctx, replicatedContextKey{}, &replicatedExecution{id: id, at: at, sequence: make(map[string]uint64)})
}

func IsReplicatedContext(ctx context.Context) bool {
	_, ok := ctx.Value(replicatedContextKey{}).(*replicatedExecution)
	return ok
}

func mutationTime(ctx context.Context) time.Time {
	if execution, ok := ctx.Value(replicatedContextKey{}).(*replicatedExecution); ok {
		return execution.at
	}
	return time.Now().UTC()
}

func mutationID(ctx context.Context, purpose string) (string, error) {
	if execution, ok := ctx.Value(replicatedContextKey{}).(*replicatedExecution); ok {
		execution.mu.Lock()
		execution.sequence[purpose]++
		sequence := execution.sequence[purpose]
		execution.mu.Unlock()
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d", execution.id, purpose, sequence)))
		return fmt.Sprintf("%x", sum[:12]), nil
	}
	return newCommitID()
}

func artifactTime(at []time.Time) time.Time {
	if len(at) > 0 {
		return at[0]
	}
	return time.Now().UTC()
}
