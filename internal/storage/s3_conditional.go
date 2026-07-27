package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
)

var errConditionalWriteRetry = errors.New("conditional write can be retried")

func (s *S3Store) putConditionalResolved(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	headers := http.Header{}
	if condition.IfNoneMatch {
		headers.Set("If-None-Match", "*")
	}
	if condition.IfMatch != "" {
		headers.Set("If-Match", quoteETag(condition.IfMatch))
	}
	for attempt := 0; attempt < defaultS3MaxAttempts; attempt++ {
		resp, err := s.doWithHeadersOnce(ctx, http.MethodPut, key, nil, data, headers)
		if err == nil && retryableS3Status(resp.StatusCode) {
			err = readS3Error(resp)
			resp.Body.Close()
		}
		if err != nil {
			meta, resolveErr := s.resolveConditionalPut(ctx, key, data, condition)
			switch {
			case resolveErr == nil:
				return meta, nil
			case errors.Is(resolveErr, ErrConflict):
				return ObjectMeta{Key: key}, ErrConflict
			case !errors.Is(resolveErr, errConditionalWriteRetry):
				return ObjectMeta{Key: key}, errors.Join(
					err,
					fmt.Errorf("resolve conditional put %q: %w", key, resolveErr),
				)
			case attempt+1 >= defaultS3MaxAttempts:
				return ObjectMeta{Key: key}, err
			}
			if err := retryDelay(ctx, attempt); err != nil {
				return ObjectMeta{Key: key}, err
			}
			continue
		}
		defer drainAndClose(resp.Body)
		if isConditionalPutConflict(resp.StatusCode, condition) {
			return ObjectMeta{Key: key}, ErrConflict
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return ObjectMeta{}, readS3Error(resp)
		}
		meta := ObjectMeta{
			Key: key, ETag: cleanETag(resp.Header.Get("ETag")), Exists: true,
		}
		return s.ensureConditionalPutETag(ctx, key, meta)
	}
	return ObjectMeta{Key: key}, fmt.Errorf(
		"%w: conditional put %q exhausted retries", ErrObjectStoreUnavailable, key,
	)
}

func (s *S3Store) resolveConditionalPut(
	ctx context.Context,
	key string,
	data []byte,
	condition PutCondition,
) (ObjectMeta, error) {
	current, meta, err := s.GetWithMeta(ctx, key)
	if errors.Is(err, ErrNotFound) {
		if condition.IfNoneMatch {
			return ObjectMeta{Key: key}, errConditionalWriteRetry
		}
		return ObjectMeta{Key: key}, ErrConflict
	}
	if err != nil {
		return ObjectMeta{Key: key}, err
	}
	if bytes.Equal(current, data) {
		return s.ensureConditionalPutETag(ctx, key, meta)
	}
	if condition.IfMatch != "" &&
		cleanETag(meta.ETag) == cleanETag(condition.IfMatch) {
		return ObjectMeta{Key: key}, errConditionalWriteRetry
	}
	return ObjectMeta{Key: key}, ErrConflict
}

func (s *S3Store) ensureConditionalPutETag(
	ctx context.Context,
	key string,
	meta ObjectMeta,
) (ObjectMeta, error) {
	if meta.ETag == "" {
		var err error
		meta, err = s.Head(ctx, key)
		if err != nil {
			return ObjectMeta{Key: key}, err
		}
	}
	if meta.ETag == "" {
		return ObjectMeta{Key: key}, fmt.Errorf(
			"s3 conditional put %q completed without returned etag", key,
		)
	}
	meta.Key = key
	meta.Exists = true
	return meta, nil
}

func (s *S3Store) deleteConditionalResolved(
	ctx context.Context,
	key string,
	expectedETag string,
) error {
	headers := http.Header{"If-Match": {quoteETag(expectedETag)}}
	for attempt := 0; attempt < defaultS3MaxAttempts; attempt++ {
		resp, err := s.doWithHeadersOnce(
			ctx, http.MethodDelete, key, nil, nil, headers,
		)
		if err == nil && retryableS3Status(resp.StatusCode) {
			err = readS3Error(resp)
			resp.Body.Close()
		}
		if err != nil {
			resolveErr := s.resolveConditionalDelete(ctx, key, expectedETag)
			switch {
			case resolveErr == nil:
				return nil
			case errors.Is(resolveErr, ErrConflict):
				return ErrConflict
			case !errors.Is(resolveErr, errConditionalWriteRetry):
				return errors.Join(
					err,
					fmt.Errorf("resolve conditional delete %q: %w", key, resolveErr),
				)
			case attempt+1 >= defaultS3MaxAttempts:
				return err
			}
			if err := retryDelay(ctx, attempt); err != nil {
				return err
			}
			continue
		}
		defer drainAndClose(resp.Body)
		if resp.StatusCode == http.StatusPreconditionFailed ||
			resp.StatusCode == http.StatusConflict ||
			resp.StatusCode == http.StatusNotFound {
			return ErrConflict
		}
		if resp.StatusCode != http.StatusNoContent &&
			(resp.StatusCode < 200 || resp.StatusCode >= 300) {
			return readS3Error(resp)
		}
		return nil
	}
	return fmt.Errorf(
		"%w: conditional delete %q exhausted retries", ErrObjectStoreUnavailable, key,
	)
}

func (s *S3Store) resolveConditionalDelete(
	ctx context.Context,
	key string,
	expectedETag string,
) error {
	meta, err := s.Head(ctx, key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if cleanETag(meta.ETag) == cleanETag(expectedETag) {
		return errConditionalWriteRetry
	}
	return ErrConflict
}
