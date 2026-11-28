package storage

import (
	"context"

	"github.com/SamuelSupe/graphdb/v2/internal/storage/backend"
)

var (
	ErrNotFound                     = backend.ErrNotFound
	ErrConflict                     = backend.ErrConflict
	ErrLeaseHeld                    = backend.ErrLeaseHeld
	ErrConditionalWriteUnsupported  = backend.ErrConditionalWriteUnsupported
	ErrConditionalDeleteUnsupported = backend.ErrConditionalDeleteUnsupported
	ErrTenantDisabled               = backend.ErrTenantDisabled
	ErrTenantDeleted                = backend.ErrTenantDeleted
)

type (
	ObjectInfo   = backend.ObjectInfo
	ObjectMeta   = backend.ObjectMeta
	PutCondition = backend.PutCondition
	ObjectStore  = backend.ObjectStore
)

func objectContextErr(ctx context.Context) error {
	return backend.ContextErr(ctx)
}

func validateObjectKey(key string) error {
	return backend.ValidateKey(key)
}
