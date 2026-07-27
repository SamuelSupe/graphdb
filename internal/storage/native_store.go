package storage

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type nativeStoreOptions struct {
	endpoint  string
	bucket    string
	region    string
	accessKey string
	secretKey string
	pathStyle bool
}

func newNativeStoreOptions(endpoint, bucket, region, accessKey, secretKey string, options S3Options) (nativeStoreOptions, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nativeStoreOptions{}, fmt.Errorf("object storage endpoint must include scheme and host")
	}
	if strings.TrimSpace(bucket) == "" {
		return nativeStoreOptions{}, fmt.Errorf("object storage bucket is required")
	}
	if strings.TrimSpace(region) == "" {
		return nativeStoreOptions{}, fmt.Errorf("object storage region is required")
	}
	if strings.TrimSpace(accessKey) == "" || strings.TrimSpace(secretKey) == "" {
		return nativeStoreOptions{}, fmt.Errorf("object storage access key and secret key are required")
	}
	return nativeStoreOptions{
		endpoint:  endpoint,
		bucket:    strings.TrimSpace(bucket),
		region:    strings.TrimSpace(region),
		accessKey: strings.TrimSpace(accessKey),
		secretKey: strings.TrimSpace(secretKey),
		pathStyle: options.PathStyle,
	}, nil
}

type nativeObjectClient interface {
	Get(ctx context.Context, key string) ([]byte, ObjectMeta, error)
	Head(ctx context.Context, key string) (ObjectMeta, error)
	Put(ctx context.Context, key string, data []byte, createOnly bool) (ObjectMeta, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
	ListPage(ctx context.Context, prefix string, after string, limit int) ([]ObjectInfo, string, error)
}

type nativeObjectProber interface {
	Probe(context.Context) error
}

// NativeObjectStore maps native provider create-only support to GGraphDB's
// object-store contract. It intentionally does not emulate ETag CAS: that
// translation belongs to SingleWriterObjectStore, where the single-writer
// topology is explicit.
type NativeObjectStore struct {
	client nativeObjectClient
}

func (s *NativeObjectStore) Probe(ctx context.Context) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if prober, ok := s.client.(nativeObjectProber); ok {
		return prober.Probe(nativeContext(ctx))
	}
	_, err := s.client.List(nativeContext(ctx), "")
	return err
}

func newNativeObjectStore(client nativeObjectClient) *NativeObjectStore {
	return &NativeObjectStore{client: client}
}

func (s *NativeObjectStore) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := s.GetWithMeta(ctx, key)
	return data, err
}

func (s *NativeObjectStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return s.client.Get(nativeContext(ctx), key)
}

func (s *NativeObjectStore) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	return s.client.Head(nativeContext(ctx), key)
}

func (s *NativeObjectStore) Put(ctx context.Context, key string, data []byte) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	_, err := s.client.Put(nativeContext(ctx), key, data, false)
	return err
}

func (s *NativeObjectStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateNativeCondition(condition); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if condition.IfMatch != "" {
		return ObjectMeta{Key: key}, ErrConditionalWriteUnsupported
	}
	return s.client.Put(nativeContext(ctx), key, data, condition.IfNoneMatch)
}

func (s *NativeObjectStore) Delete(ctx context.Context, key string) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	return s.client.Delete(nativeContext(ctx), key)
}

func (s *NativeObjectStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	if err := validateNativeCondition(condition); err != nil {
		return err
	}
	if condition.IfMatch != "" {
		return ErrConditionalDeleteUnsupported
	}
	if condition.IfNoneMatch {
		if _, err := s.Head(ctx, key); errors.Is(err, ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return ErrConflict
	}
	return s.client.Delete(nativeContext(ctx), key)
}

func (s *NativeObjectStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	return s.client.List(nativeContext(ctx), prefix)
}

func (s *NativeObjectStore) ListPage(
	ctx context.Context,
	prefix string,
	after string,
	limit int,
) ([]ObjectInfo, string, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, "", err
	}
	return s.client.ListPage(nativeContext(ctx), prefix, after, limit)
}

func collectNativeObjectPages(
	ctx context.Context,
	prefix string,
	listPage func(context.Context, string, string, int) ([]ObjectInfo, string, error),
) ([]ObjectInfo, error) {
	items := make([]ObjectInfo, 0)
	after := ""
	for {
		page, next, err := listPage(ctx, prefix, after, 1000)
		if err != nil {
			return nil, err
		}
		items = append(items, page...)
		if next == "" {
			return items, nil
		}
		if next <= after {
			return nil, fmt.Errorf(
				"native object list cursor did not advance for prefix %q",
				prefix,
			)
		}
		after = next
	}
}

func nativeObjectPageLimit(limit int) int {
	if limit <= 0 || limit > 1000 {
		return 1000
	}
	return limit
}

func validateNativeCondition(condition PutCondition) error {
	if condition.IfNoneMatch && condition.IfMatch != "" {
		return fmt.Errorf("cannot combine If-None-Match and If-Match")
	}
	return nil
}

func nativeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func nativeStringRef(value string) *string {
	return &value
}

func nativeStringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
