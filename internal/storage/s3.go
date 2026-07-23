package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type S3Store struct {
	endpoint  *url.URL
	bucket    string
	region    string
	accessKey string
	secretKey string
	pathStyle bool
	client    *http.Client
}

type S3Options struct {
	PathStyle bool
}

const (
	defaultS3RequestTimeout = 2 * time.Minute
	defaultS3MaxAttempts    = 3
	defaultS3MaxConns       = 64
	s3DeleteBatchLimit      = 1000
)

func NewS3Store(endpoint, bucket, region, accessKey, secretKey string) (*S3Store, error) {
	return NewS3StoreWithOptions(endpoint, bucket, region, accessKey, secretKey, S3Options{})
}

func NewS3StoreWithOptions(endpoint, bucket, region, accessKey, secretKey string, options S3Options) (*S3Store, error) {
	parsed, err := url.Parse(strings.TrimRight(endpoint, "/"))
	if err != nil {
		return nil, err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("S3 endpoint must include scheme and host")
	}
	if bucket == "" {
		return nil, fmt.Errorf("S3 bucket is required")
	}
	if region == "" {
		region = "us-east-1"
	}
	if accessKey == "" || secretKey == "" {
		return nil, fmt.Errorf("S3 access key and secret key are required")
	}
	return &S3Store{
		endpoint:  parsed,
		bucket:    bucket,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		pathStyle: options.PathStyle,
		client:    &http.Client{Timeout: defaultS3RequestTimeout, Transport: newS3Transport()},
	}, nil
}

func newS3Transport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          defaultS3MaxConns * 2,
		MaxIdleConnsPerHost:   defaultS3MaxConns,
		MaxConnsPerHost:       defaultS3MaxConns,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	data, _, err := s.GetWithMeta(ctx, key)
	return data, err
}

func (s *S3Store) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	if err := validateObjectKey(key); err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	resp, err := s.do(ctx, http.MethodGet, key, nil, nil)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, ObjectMeta{Key: key}, ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, ObjectMeta{Key: key}, readS3Error(resp)
	}
	data, err := io.ReadAll(resp.Body)
	return data, ObjectMeta{Key: key, ETag: cleanETag(resp.Header.Get("ETag")), Exists: true}, err
}

func (s *S3Store) Head(ctx context.Context, key string) (ObjectMeta, error) {
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	resp, err := s.do(ctx, http.MethodHead, key, nil, nil)
	if err != nil {
		return ObjectMeta{Key: key}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return ObjectMeta{Key: key}, ErrNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ObjectMeta{Key: key}, readS3Error(resp)
	}
	return ObjectMeta{Key: key, ETag: cleanETag(resp.Header.Get("ETag")), Exists: true}, nil
}

func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *S3Store) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (ObjectMeta, error) {
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	headers := http.Header{}
	if condition.IfNoneMatch {
		headers.Set("If-None-Match", "*")
	}
	if condition.IfMatch != "" {
		headers.Set("If-Match", quoteETag(condition.IfMatch))
	}
	resp, err := s.doWithHeaders(ctx, http.MethodPut, key, nil, data, headers)
	if err != nil {
		return ObjectMeta{}, err
	}
	defer drainAndClose(resp.Body)
	if isConditionalPutConflict(resp.StatusCode, condition) {
		return ObjectMeta{Key: key}, ErrConflict
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ObjectMeta{}, readS3Error(resp)
	}
	meta := ObjectMeta{Key: key, ETag: cleanETag(resp.Header.Get("ETag")), Exists: true}
	if meta.ETag == "" && requiresReturnedETag(condition) {
		fetched, err := s.Head(ctx, key)
		if err != nil {
			return ObjectMeta{}, err
		}
		if fetched.ETag == "" {
			return ObjectMeta{}, fmt.Errorf("s3 conditional put %q completed without returned etag", key)
		}
		meta = fetched
	}
	return meta, nil
}

func requiresReturnedETag(condition PutCondition) bool {
	return condition.IfNoneMatch || condition.IfMatch != ""
}

func isConditionalPutConflict(status int, condition PutCondition) bool {
	if status == http.StatusPreconditionFailed {
		return true
	}
	return status == http.StatusConflict && requiresReturnedETag(condition)
}

func (s *S3Store) Delete(ctx context.Context, key string) error {
	return s.DeleteConditional(ctx, key, PutCondition{})
}

func (s *S3Store) DeleteConditional(ctx context.Context, key string, condition PutCondition) error {
	if err := validateObjectKey(key); err != nil {
		return err
	}
	if condition.IfNoneMatch {
		if _, err := s.Head(ctx, key); errors.Is(err, ErrNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		return ErrConflict
	}
	headers := http.Header{}
	if condition.IfMatch != "" {
		headers.Set("If-Match", quoteETag(condition.IfMatch))
	}
	resp, err := s.doWithHeaders(ctx, http.MethodDelete, key, nil, nil, headers)
	if err != nil {
		return err
	}
	defer drainAndClose(resp.Body)
	if resp.StatusCode == http.StatusPreconditionFailed || (resp.StatusCode == http.StatusConflict && condition.IfMatch != "") {
		return ErrConflict
	}
	if resp.StatusCode == http.StatusNotFound {
		if condition.IfMatch != "" {
			return ErrConflict
		}
		return nil
	}
	if resp.StatusCode == http.StatusNoContent {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return readS3Error(resp)
	}
	return nil
}

func (s *S3Store) DeleteBatch(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := validateObjectKey(key); err != nil {
			return err
		}
	}
	for start := 0; start < len(keys); start += s3DeleteBatchLimit {
		end := min(start+s3DeleteBatchLimit, len(keys))
		if err := s.deleteBatch(ctx, keys[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *S3Store) deleteBatch(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	request := deleteObjectsRequest{
		XMLNS: "http://s3.amazonaws.com/doc/2006-03-01/",
		Quiet: true,
	}
	request.Objects = make([]deleteObjectIdentifier, 0, len(keys))
	for _, key := range keys {
		request.Objects = append(request.Objects, deleteObjectIdentifier{Key: key})
	}
	body, err := xml.Marshal(request)
	if err != nil {
		return err
	}
	sum := md5.Sum(body)
	headers := http.Header{}
	headers.Set("Content-MD5", base64.StdEncoding.EncodeToString(sum[:]))
	headers.Set("Content-Type", "application/xml")
	query := url.Values{"delete": {""}}
	resp, err := s.doWithHeaders(ctx, http.MethodPost, "", query, body, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return readS3Error(resp)
	}
	var result deleteObjectsResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(result.Errors) > 0 {
		item := result.Errors[0]
		return fmt.Errorf(
			"s3 batch delete failed for %d objects; first error key=%q code=%q message=%q",
			len(result.Errors), item.Key, item.Code, item.Message,
		)
	}
	return nil
}

func (s *S3Store) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items := make([]ObjectInfo, 0)
	var token string
	for {
		query := url.Values{}
		query.Set("list-type", "2")
		query.Set("prefix", prefix)
		if token != "" {
			query.Set("continuation-token", token)
		}
		resp, err := s.do(ctx, http.MethodGet, "", query, nil)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err := readS3Error(resp)
			resp.Body.Close()
			return nil, err
		}
		var result listBucketResult
		decodeErr := xml.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		if decodeErr != nil {
			return nil, decodeErr
		}
		for _, object := range result.Contents {
			items = append(items, ObjectInfo{Key: object.Key, Size: object.Size, ETag: cleanETag(object.ETag)})
		}
		if !result.IsTruncated {
			break
		}
		if result.NextContinuationToken == "" {
			return nil, fmt.Errorf("s3 list response for prefix %q was truncated without continuation token", prefix)
		}
		if result.NextContinuationToken == token {
			return nil, fmt.Errorf("s3 list response for prefix %q repeated continuation token %q", prefix, token)
		}
		token = result.NextContinuationToken
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})
	return items, nil
}

func (s *S3Store) ListPage(ctx context.Context, prefix string, after string, limit int) ([]ObjectInfo, string, error) {
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	query := url.Values{}
	query.Set("list-type", "2")
	query.Set("prefix", prefix)
	query.Set("max-keys", strconv.Itoa(limit))
	if after != "" {
		query.Set("start-after", after)
	}
	resp, err := s.do(ctx, http.MethodGet, "", query, nil)
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := readS3Error(resp)
		resp.Body.Close()
		return nil, "", err
	}
	var result listBucketResult
	decodeErr := xml.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	if decodeErr != nil {
		return nil, "", decodeErr
	}
	items := make([]ObjectInfo, 0, len(result.Contents))
	for _, object := range result.Contents {
		items = append(items, ObjectInfo{Key: object.Key, Size: object.Size, ETag: cleanETag(object.ETag)})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Key < items[j].Key })
	if !result.IsTruncated {
		return items, "", nil
	}
	if len(items) == 0 {
		return nil, "", fmt.Errorf("s3 list page for prefix %q was truncated without objects", prefix)
	}
	return items, items[len(items)-1].Key, nil
}

func (s *S3Store) Probe(ctx context.Context) error {
	_, _, err := s.ListPage(ctx, "", "", 1)
	return err
}

func (s *S3Store) do(ctx context.Context, method, key string, query url.Values, body []byte) (*http.Response, error) {
	return s.doWithHeaders(ctx, method, key, query, body, nil)
}

func (s *S3Store) doWithHeaders(ctx context.Context, method, key string, query url.Values, body []byte, headers http.Header) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if query == nil {
		query = url.Values{}
	}
	u := s.requestURL(key, query)

	payloadHash := sha256Hex(body)
	now := time.Now().UTC()
	amzDate := now.Format("20060102T150405Z")
	date := now.Format("20060102")
	for attempt := 0; attempt < defaultS3MaxAttempts; attempt++ {
		var reader io.Reader
		if body != nil {
			reader = bytes.NewReader(body)
		}
		req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
		if err != nil {
			return nil, err
		}
		req.Header.Set("X-Amz-Content-Sha256", payloadHash)
		req.Header.Set("X-Amz-Date", amzDate)
		if body != nil {
			req.Header.Set("Content-Type", "application/octet-stream")
		}
		for key, values := range headers {
			req.Header.Del(key)
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		req.Header.Set("Authorization", s.authorization(method, u.EscapedPath(), u.RawQuery, req.URL.Host, payloadHash, amzDate, date))
		resp, err := s.client.Do(req)
		if err == nil {
			return resp, nil
		}
		if !retryableS3TransportError(ctx, err) {
			return nil, err
		}
		if attempt+1 < defaultS3MaxAttempts {
			if delayErr := retryDelay(ctx, attempt); delayErr != nil {
				return nil, delayErr
			}
			continue
		}
		return nil, fmt.Errorf("%w: %v", ErrObjectStoreUnavailable, err)
	}
	return nil, fmt.Errorf("%w: request exhausted retries", ErrObjectStoreUnavailable)
}

func (s *S3Store) requestURL(key string, query url.Values) url.URL {
	u := *s.endpoint
	if s.pathStyle {
		u.Path = s.pathStylePath(key)
	} else {
		u.Host = s.bucket + "." + u.Host
		u.Path = endpointPath(s.endpoint.Path, key)
	}
	u.RawQuery = canonicalQuery(query)
	return u
}

func (s *S3Store) pathStylePath(key string) string {
	return endpointPath(s.bucketPath(), key)
}

func retryableS3TransportError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	return true
}

func (s *S3Store) bucketPath() string {
	base := strings.TrimRight(s.endpoint.Path, "/")
	if base == "" {
		return "/" + s.bucket
	}
	return base + "/" + s.bucket
}

func endpointPath(base, key string) string {
	base = strings.TrimRight(base, "/")
	if key == "" {
		if base == "" {
			return "/"
		}
		return base
	}
	if base == "" {
		return "/" + key
	}
	return base + "/" + key
}

func readS3Error(resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_, _ = io.Copy(io.Discard, resp.Body)
	err := fmt.Errorf("s3 request failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	if resp.StatusCode >= 500 {
		return fmt.Errorf("%w: %v", ErrObjectStoreUnavailable, err)
	}
	return err
}

func drainAndClose(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, body)
	_ = body.Close()
}

type listBucketResult struct {
	IsTruncated           bool   `xml:"IsTruncated"`
	NextContinuationToken string `xml:"NextContinuationToken"`
	Contents              []struct {
		Key  string `xml:"Key"`
		Size int64  `xml:"Size"`
		ETag string `xml:"ETag"`
	} `xml:"Contents"`
}

type deleteObjectsRequest struct {
	XMLName xml.Name                 `xml:"Delete"`
	XMLNS   string                   `xml:"xmlns,attr,omitempty"`
	Objects []deleteObjectIdentifier `xml:"Object"`
	Quiet   bool                     `xml:"Quiet"`
}

type deleteObjectIdentifier struct {
	Key string `xml:"Key"`
}

type deleteObjectsResult struct {
	Errors []struct {
		Key     string `xml:"Key"`
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	} `xml:"Error"`
}
