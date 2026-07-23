package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"

	cos "github.com/tencentyun/cos-go-sdk-v5"
)

type tencentCOSClient struct {
	client *cos.Client
}

func (c *tencentCOSClient) Probe(ctx context.Context) error {
	_, response, err := c.client.Bucket.Get(ctx, &cos.BucketGetOptions{MaxKeys: 1})
	if response != nil {
		closeCOSResponse(response)
	}
	return normalizeTencentCOSError(err, false)
}

func NewTencentCOSStore(endpoint, bucket, region, accessKey, secretKey string, options S3Options) (*NativeObjectStore, error) {
	config, err := newNativeStoreOptions(endpoint, bucket, region, accessKey, secretKey, options)
	if err != nil {
		return nil, err
	}
	if config.pathStyle {
		return nil, fmt.Errorf("tencent cos native provider does not support S3_PATH_STYLE=true")
	}
	bucketURL, err := tencentCOSBucketURL(config)
	if err != nil {
		return nil, fmt.Errorf("parse tencent cos endpoint: %w", err)
	}
	transport := &cos.AuthorizationTransport{
		SecretID:  config.accessKey,
		SecretKey: config.secretKey,
		Transport: newS3Transport(),
	}
	client := cos.NewClient(&cos.BaseURL{BucketURL: bucketURL}, &http.Client{
		Timeout:   defaultS3RequestTimeout,
		Transport: transport,
	})
	return newNativeObjectStore(&tencentCOSClient{client: client}), nil
}

func tencentCOSBucketURL(config nativeStoreOptions) (*url.URL, error) {
	bucketURL, err := url.Parse(config.endpoint)
	if err != nil {
		return nil, err
	}
	host := strings.ToLower(bucketURL.Hostname())
	if strings.HasSuffix(host, ".myqcloud.com") || strings.HasSuffix(host, ".tencentcos.cn") {
		bucketPrefix := strings.ToLower(config.bucket) + "."
		if !strings.HasPrefix(host, bucketPrefix) {
			bucketURL.Host = config.bucket + "." + bucketURL.Host
		}
	}
	return bucketURL, nil
}

func (c *tencentCOSClient) Get(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	response, err := c.client.Object.Get(ctx, key, nil)
	if err != nil {
		return nil, ObjectMeta{Key: key}, normalizeTencentCOSError(err, false)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return data, ObjectMeta{Key: key, ETag: cleanETag(response.Header.Get("ETag")), Exists: true}, nil
}

func (c *tencentCOSClient) Head(ctx context.Context, key string) (ObjectMeta, error) {
	response, err := c.client.Object.Head(ctx, key, nil)
	if err != nil {
		return ObjectMeta{Key: key}, normalizeTencentCOSError(err, false)
	}
	defer closeCOSResponse(response)
	return ObjectMeta{Key: key, ETag: cleanETag(response.Header.Get("ETag")), Exists: true}, nil
}

func (c *tencentCOSClient) Put(ctx context.Context, key string, data []byte, createOnly bool) (ObjectMeta, error) {
	var options *cos.ObjectPutOptions
	if createOnly {
		headers := http.Header{}
		headers.Set("x-cos-forbid-overwrite", "true")
		options = &cos.ObjectPutOptions{ObjectPutHeaderOptions: &cos.ObjectPutHeaderOptions{XOptionHeader: &headers}}
	}
	response, err := c.client.Object.Put(ctx, key, bytes.NewReader(data), options)
	if err != nil {
		return ObjectMeta{Key: key}, normalizeTencentCOSError(err, createOnly)
	}
	defer closeCOSResponse(response)
	meta := ObjectMeta{Key: key, ETag: cleanETag(response.Header.Get("ETag")), Exists: true}
	if meta.ETag != "" {
		return meta, nil
	}
	return c.Head(ctx, key)
}

func (c *tencentCOSClient) Delete(ctx context.Context, key string) error {
	response, err := c.client.Object.Delete(ctx, key)
	if response != nil {
		defer closeCOSResponse(response)
	}
	if err == nil {
		return nil
	}
	err = normalizeTencentCOSError(err, false)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (c *tencentCOSClient) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items := make([]ObjectInfo, 0)
	var marker string
	for {
		result, response, err := c.client.Bucket.Get(ctx, &cos.BucketGetOptions{Prefix: prefix, Marker: marker, MaxKeys: 1000})
		if response != nil {
			closeCOSResponse(response)
		}
		if err != nil {
			return nil, normalizeTencentCOSError(err, false)
		}
		for _, object := range result.Contents {
			items = append(items, ObjectInfo{Key: object.Key, Size: object.Size, ETag: cleanETag(object.ETag)})
		}
		if !result.IsTruncated {
			break
		}
		if result.NextMarker == "" || result.NextMarker == marker {
			return nil, fmt.Errorf("cos list response for prefix %q was truncated without a new marker", prefix)
		}
		marker = result.NextMarker
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	return items, nil
}

func normalizeTencentCOSError(err error, createOnly bool) error {
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
		return err
	}
	if serviceErr, ok := cos.IsCOSError(err); ok {
		if serviceErr.Response != nil {
			switch serviceErr.Response.StatusCode {
			case http.StatusNotFound:
				return ErrNotFound
			case http.StatusConflict, http.StatusPreconditionFailed:
				if createOnly {
					return ErrConflict
				}
			}
		}
		if createOnly && strings.EqualFold(serviceErr.Code, "FileAlreadyExists") {
			return ErrConflict
		}
	}
	return fmt.Errorf("tencent cos: %w", err)
}

func closeCOSResponse(response *cos.Response) {
	if response == nil || response.Response == nil || response.Body == nil {
		return
	}
	drainAndClose(response.Body)
}

var _ nativeObjectClient = (*tencentCOSClient)(nil)
