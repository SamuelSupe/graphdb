package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
)

const huaweiOBSMaxRetryCount = 0

type huaweiOBSClient struct {
	client    *obs.ObsClient
	endpoint  string
	bucket    string
	region    string
	accessKey string
	secretKey string
	pathStyle bool

	requestHTTPClient *http.Client
	probeHTTPClient   *http.Client
}

func (c *huaweiOBSClient) Probe(ctx context.Context) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	client, err := c.newProbeClient(ctx)
	if err != nil {
		return fmt.Errorf("create huawei obs probe client: %w", err)
	}
	defer client.Close()
	_, err = client.ListObjects(&obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{MaxKeys: 1},
		Bucket:        c.bucket,
	})
	if err != nil {
		return normalizeHuaweiOBSError(err, false)
	}
	return objectContextErr(ctx)
}

func (c *huaweiOBSClient) newProbeClient(ctx context.Context) (*obs.ObsClient, error) {
	if c.probeHTTPClient != nil {
		return obs.New(
			c.accessKey,
			c.secretKey,
			c.endpoint,
			obs.WithPathStyle(c.pathStyle),
			obs.WithRegion(c.region),
			obs.WithRequestContext(ctx),
			obs.WithMaxRetryCount(0),
			obs.WithHttpClient(c.probeHTTPClient),
		)
	}
	return obs.New(
		c.accessKey,
		c.secretKey,
		c.endpoint,
		obs.WithPathStyle(c.pathStyle),
		obs.WithRegion(c.region),
		obs.WithRequestContext(ctx),
		obs.WithConnectTimeout(5),
		obs.WithSocketTimeout(5),
		obs.WithHeaderTimeout(5),
		obs.WithMaxRetryCount(0),
	)
}

func NewHuaweiOBSStore(endpoint, bucket, region, accessKey, secretKey string, options S3Options) (*NativeObjectStore, error) {
	config, err := newNativeStoreOptions(endpoint, bucket, region, accessKey, secretKey, options)
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Timeout:   defaultS3RequestTimeout,
		Transport: newS3Transport(),
	}
	client, err := obs.New(
		config.accessKey,
		config.secretKey,
		config.endpoint,
		obs.WithPathStyle(config.pathStyle),
		obs.WithRegion(config.region),
		// The SDK retry backoff does not observe the request context. Retrying at
		// this layer can therefore retain canceled requests until every backoff
		// completes; callers already have bounded, retryable object operations.
		obs.WithMaxRetryCount(huaweiOBSMaxRetryCount),
		obs.WithHttpClient(httpClient),
	)
	if err != nil {
		return nil, fmt.Errorf("create huawei obs client: %w", err)
	}
	return newNativeObjectStore(&huaweiOBSClient{
		client:            client,
		endpoint:          config.endpoint,
		bucket:            config.bucket,
		region:            config.region,
		accessKey:         config.accessKey,
		secretKey:         config.secretKey,
		pathStyle:         config.pathStyle,
		requestHTTPClient: httpClient,
	}), nil
}

func (c *huaweiOBSClient) requestClient(ctx context.Context) (*obs.ObsClient, error) {
	if c.requestHTTPClient == nil {
		if c.client == nil {
			return nil, fmt.Errorf("huawei obs client is not configured")
		}
		return c.client, nil
	}
	return obs.New(
		c.accessKey,
		c.secretKey,
		c.endpoint,
		obs.WithPathStyle(c.pathStyle),
		obs.WithRegion(c.region),
		obs.WithRequestContext(ctx),
		obs.WithMaxRetryCount(huaweiOBSMaxRetryCount),
		obs.WithHttpClient(c.requestHTTPClient),
	)
}

func (c *huaweiOBSClient) Get(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	client, err := c.requestClient(ctx)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	result, err := client.GetObject(&obs.GetObjectInput{GetObjectMetadataInput: obs.GetObjectMetadataInput{Bucket: c.bucket, Key: key}})
	if err != nil {
		return nil, ObjectMeta{Key: key}, normalizeHuaweiOBSError(err, false)
	}
	defer result.Body.Close()
	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	if err := objectContextErr(ctx); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return data, ObjectMeta{Key: key, ETag: cleanETag(result.ETag), Exists: true}, nil
}

func (c *huaweiOBSClient) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	client, err := c.requestClient(ctx)
	if err != nil {
		return ObjectMeta{Key: key}, err
	}
	result, err := client.HeadObject(&obs.HeadObjectInput{Bucket: c.bucket, Key: key})
	if err != nil {
		return ObjectMeta{Key: key}, normalizeHuaweiOBSError(err, false)
	}
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	return ObjectMeta{Key: key, ETag: cleanETag(http.Header(result.ResponseHeaders).Get("ETag")), Exists: true}, nil
}

func (c *huaweiOBSClient) Put(ctx context.Context, key string, data []byte, createOnly bool) (ObjectMeta, error) {
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	client, err := c.requestClient(ctx)
	if err != nil {
		return ObjectMeta{Key: key}, err
	}
	input := &obs.PutObjectInput{
		PutObjectBasicInput: obs.PutObjectBasicInput{
			ObjectOperationInput: obs.ObjectOperationInput{Bucket: c.bucket, Key: key},
			ContentLength:        int64(len(data)),
		},
		Body: bytes.NewReader(data),
	}
	var (
		result *obs.PutObjectOutput
		putErr error
	)
	if createOnly {
		result, putErr = client.PutObject(input, obs.WithCustomHeader("x-obs-forbid-overwrite", "true"))
	} else {
		result, putErr = client.PutObject(input)
	}
	if putErr != nil {
		return ObjectMeta{Key: key}, normalizeHuaweiOBSError(putErr, createOnly)
	}
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	meta := ObjectMeta{Key: key, ETag: cleanETag(result.ETag), Exists: true}
	if meta.ETag != "" {
		return meta, nil
	}
	return c.Head(ctx, key)
}

func (c *huaweiOBSClient) Delete(ctx context.Context, key string) error {
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	client, err := c.requestClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.DeleteObject(&obs.DeleteObjectInput{Bucket: c.bucket, Key: key})
	if err == nil {
		return objectContextErr(ctx)
	}
	err = normalizeHuaweiOBSError(err, false)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	return err
}

func (c *huaweiOBSClient) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return collectNativeObjectPages(ctx, prefix, c.ListPage)
}

func (c *huaweiOBSClient) ListPage(
	ctx context.Context,
	prefix string,
	after string,
	limit int,
) ([]ObjectInfo, string, error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, "", err
	}
	client, err := c.requestClient(ctx)
	if err != nil {
		return nil, "", err
	}
	result, err := client.ListObjects(&obs.ListObjectsInput{
		ListObjsInput: obs.ListObjsInput{
			Prefix:  prefix,
			MaxKeys: nativeObjectPageLimit(limit),
		},
		Bucket: c.bucket,
		Marker: after,
	})
	if err != nil {
		return nil, "", normalizeHuaweiOBSError(err, false)
	}
	items := make([]ObjectInfo, 0, len(result.Contents))
	for _, object := range result.Contents {
		items = append(items, ObjectInfo{
			Key: object.Key, Size: object.Size, ETag: cleanETag(object.ETag),
		})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	if !result.IsTruncated {
		return items, "", objectContextErr(ctx)
	}
	if len(items) == 0 {
		return nil, "", fmt.Errorf(
			"obs list page for prefix %q was truncated without objects", prefix,
		)
	}
	return items, items[len(items)-1].Key, objectContextErr(ctx)
}

func normalizeHuaweiOBSError(err error, createOnly bool) error {
	if err == nil || errors.Is(err, ErrNotFound) || errors.Is(err, ErrConflict) {
		return err
	}
	switch huaweiOBSStatusCode(err) {
	case http.StatusNotFound:
		return ErrNotFound
	case http.StatusConflict, http.StatusPreconditionFailed:
		if createOnly {
			return ErrConflict
		}
	}
	return fmt.Errorf("huawei obs: %w", err)
}

func huaweiOBSStatusCode(err error) int {
	var serviceErr obs.ObsError
	if errors.As(err, &serviceErr) {
		return statusCodeFromOBS(serviceErr)
	}
	var serviceErrPtr *obs.ObsError
	if errors.As(err, &serviceErrPtr) && serviceErrPtr != nil {
		return statusCodeFromOBS(*serviceErrPtr)
	}
	return 0
}

func statusCodeFromOBS(err obs.ObsError) int {
	if err.StatusCode != 0 {
		return err.StatusCode
	}
	status := strings.TrimSpace(err.Status)
	switch {
	case strings.HasPrefix(status, "404"):
		return http.StatusNotFound
	case strings.HasPrefix(status, "409"):
		return http.StatusConflict
	case strings.HasPrefix(status, "412"):
		return http.StatusPreconditionFailed
	default:
		return 0
	}
}

var _ nativeObjectClient = (*huaweiOBSClient)(nil)
