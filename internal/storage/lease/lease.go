// Package lease encapsulates WriterLease contracts, fence matching, and
// bounded lease caching with atomic mismatch eviction for shared backends.
package lease

import (
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/backend"
)

type WriterLease struct {
	TenantID   string    `json:"tenant_id"`
	OwnerID    string    `json:"owner_id"`
	FenceToken string    `json:"fence_token,omitempty"`
	FenceEpoch int64     `json:"fence_epoch,omitempty"`
	ExpiresAt  time.Time `json:"expires_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

type FenceRef struct {
	OwnerID string
	Token   string
	Epoch   int64
}

func MatchesFence(l WriterLease, expected FenceRef) bool {
	return l.OwnerID == expected.OwnerID &&
		l.FenceToken == expected.Token &&
		l.FenceEpoch == expected.Epoch &&
		expected.Token != "" &&
		expected.Epoch > 0
}

func UsableForOwner(l WriterLease, ownerID string) bool {
	if l.OwnerID != ownerID {
		return false
	}
	return l.FenceToken != "" && l.FenceEpoch > 0
}

type Entry struct {
	Lease WriterLease
	Meta  backend.ObjectMeta
}
