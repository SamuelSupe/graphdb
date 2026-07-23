package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
)

type aliyunOSSClient struct {
	client *oss.Client
	bucket string
}

func (c *aliyunOSSClient) Probe(ctx context.Context) error {
	_, err := c.client.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
		Bucket:  nativeStringRef(c.bucket),
		MaxKeys: 1,
	})
	return normalizeAliyunOSSError(err, false)
}

func NewAliyunOSSStore(endpoint, bucket, region, accessKey, secretKey string, options S3Options) (*NativeObjectStore, error) {
	config, err := newNativeStoreOptions(endpoint, bucket, region, accessKey, secretKey, options)
	if err != nil {
		return nil, err
	}
	client := oss.NewClient(oss.NewConfig().
		WithRegion(config.region).
		WithEndpoint(config.endpoint).
		WithCredentialsProvider(credentials.NewStaticCredentialsProvider(config.accessKey, config.secretKey)).
		WithUsePathStyle(config.pathStyle).
		WithHttpClient(&http.Client{Timeout: defaultS3RequestTimeout, Transport: newS3Transport()}))
	return newNativeObjectStore(&aliyunOSSClient{client: client, bucket: config.bucket}), nil
}

func (c *aliyunOSSClient) Get(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	result, err := c.client.GetObject(ctx, &oss.GetObjectRequest{Bucket: nativeStringRef(c.bucket), Key: nativeStringRef(key)})
	if err != nil {
		return nil, ObjectMeta{Key: key}, normalizeAliyunOSSError(err, false)
	}
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return data, ObjectMeta{Key: key, ETag: cleanETag(nativeStringValue(result.ETag)), Exists: true}, nil
}

func (c *aliyunOSSClient) Head(ctx context.Context, key string) (ObjectMeta, error) {
	result, err := c.client.HeadObject(ctx, &oss.HeadObjectRequest{Bucket: nativeStringRef(c.bucket), Key: nativeStringRef(key)})
	if err != nil {
		return ObjectMeta{Key: key}, normalizeAliyunOSSError(err, false)
	}
	return ObjectMeta{Key: key, ETag: cleanETag(nativeStringValue(result.ETag)), Exists: true}, nil
}

func (c *aliyunOSSClient) Put(ctx context.Context, key string, data []byte, createOnly bool) (ObjectMeta, error) {
	request := &oss.PutObjectRequest{
		Bucket: nativeStringRef(c.bucket),
		Key:    nativeStringRef(key),
		Body:   bytes.NewReader(data),
	}
	if createOnly {
		request.ForbidOverwrite = nativeStringRef("true")
	}
	result, err := c.client.PutObject(ctx, request)
	if err != nil {
		return ObjectMeta{Key: key}, normalizeAliyunOSSError(err, createOnly)
	}
	meta := ObjectMeta{Key: key, ETag: cleanETag(nativeStringValue(result.ETag)), Exists: true}
	if meta.ETag != "" {
		return meta, nil
	}
	return c.Head(ctx, key)
}

func (c *aliyunOSSClient) Delete(ctx context.Context, key string) error {
	_, err := c.client.DeleteObject(ctx, &oss.DeleteObjectRequest{Bucket: nativeStringRef(c.bucket), Key: nativeStringRef(key)})
	if err == nil {
		return nil
	}
	err = normalizeAliyunOSSError(err, false)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (c *aliyunOSSClient) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items := make([]ObjectInfo, 0)
	var token string
	for {
		result, err := c.client.ListObjectsV2(ctx, &oss.ListObjectsV2Request{
			Bucket:            nativeStringRef(c.bucket),
			Prefix:            nativeStringRef(prefix),
			ContinuationToken: nativeStringRef(token),
			MaxKeys:           1000,
		})
		if err != nil {
			return nil, normalizeAliyunOSSError(err, false)
		}
		for _, object := range result.Contents {
			items = append(items, ObjectInfo{Key: nativeStringValue(object.Key), Size: object.Size, ETag: cleanETag(nativeStringValue(object.ETag))})
		}
		if !result.IsTruncated {
			break
		}
		next := nativeStringValue(result.NextContinuationToken)
		if next == "" || next == token {
			return nil, fmt.Errorf("oss list response for prefix %q was truncated without a new continuation token", prefix)
		}
		token = next
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil
}

func normalizeAliyunOSSError(err error, createOnly bool) error {
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
		return err
	}
	var serviceErr *oss.ServiceError
	if errors.As(err, &serviceErr) {
		if serviceErr.StatusCode == http.StatusNotFound {
			return ErrNotFound
		}
		if createOnly && (serviceErr.StatusCode == http.StatusConflict || serviceErr.StatusCode == http.StatusPreconditionFailed) {
			return ErrConflict
		}
	}
	return fmt.Errorf("aliyun oss: %w", err)
}

var _ nativeObjectClient = (*aliyunOSSClient)(nil)
