package storage

import (
	"context"
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("object not found")
var ErrConflict = errors.New("object write conflict")
var ErrLeaseHeld = errors.New("writer lease is held")
var ErrConditionalDeleteUnsupported = errors.New("conditional delete unsupported")
var ErrObjectStoreUnavailable = errors.New("object store unavailable")
var ErrTenantDisabled = errors.New("tenant disabled")
var ErrTenantDeleted = errors.New("tenant deleted")

type ObjectInfo struct {
	Key  string
	Size int64
	ETag string
}

type ObjectMeta struct {
	Key    string
	ETag   string
	Exists bool
}

type PutCondition struct {
	IfMatch     string
	IfNoneMatch bool
}

type ObjectStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error)
	Put(ctx context.Context, key string, data []byte) error
	PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error)
	Delete(ctx context.Context, key string) error
	DeleteConditional(ctx context.Context, key string, condition PutCondition) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

func objectContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func validateObjectKey(key string) error {
	if key == "" {
		return fmt.Errorf("object key is required")
	}
	return nil
}
