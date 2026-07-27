package storage

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"
)

type s3ConditionalDeleteState uint8

const (
	s3ConditionalDeleteUnknown s3ConditionalDeleteState = iota
	s3ConditionalDeleteAvailable
	s3ConditionalDeleteUnavailable
)

func (s *S3Store) supportsConditionalDelete(
	ctx context.Context,
	targetKey string,
) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.conditionalDeleteMu.Lock()
	defer s.conditionalDeleteMu.Unlock()
	switch s.conditionalDeleteState {
	case s3ConditionalDeleteAvailable:
		return true, nil
	case s3ConditionalDeleteUnavailable:
		return false, nil
	}
	supported, err := s.probeConditionalDelete(ctx, targetKey)
	if err != nil {
		return false, err
	}
	if supported {
		s.conditionalDeleteState = s3ConditionalDeleteAvailable
	} else {
		s.conditionalDeleteState = s3ConditionalDeleteUnavailable
	}
	return supported, nil
}

func (s *S3Store) probeConditionalDelete(
	ctx context.Context,
	targetKey string,
) (bool, error) {
	probeID, err := newCommitID()
	if err != nil {
		return false, err
	}
	probeKey := path.Join(
		path.Dir(targetKey),
		".graphdb-conditional-delete-probe-"+probeID,
	)
	first, err := s.PutConditional(
		ctx, probeKey, []byte("first"), PutCondition{IfNoneMatch: true},
	)
	if err != nil {
		return false, fmt.Errorf("create conditional delete probe: %w", err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 10*time.Second,
		)
		defer cancel()
		_ = s.Delete(cleanupCtx, probeKey)
	}()
	second, err := s.PutConditional(
		ctx, probeKey, []byte("second"), PutCondition{IfMatch: first.ETag},
	)
	if err != nil {
		return false, fmt.Errorf("advance conditional delete probe: %w", err)
	}

	if err := s.probeDeleteWithETag(ctx, probeKey, first.ETag); err != nil {
		return false, err
	}
	current, err := s.Head(ctx, probeKey)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if cleanETag(current.ETag) != cleanETag(second.ETag) {
		return false, fmt.Errorf("conditional delete probe changed unexpectedly")
	}

	if err := s.probeDeleteWithETag(ctx, probeKey, second.ETag); err != nil {
		return false, err
	}
	_, err = s.Head(ctx, probeKey)
	if errors.Is(err, ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func (s *S3Store) probeDeleteWithETag(
	ctx context.Context,
	key string,
	etag string,
) error {
	headers := http.Header{"If-Match": {quoteETag(etag)}}
	response, err := s.doWithHeadersOnce(
		ctx, http.MethodDelete, key, nil, nil, headers,
	)
	if err != nil {
		return err
	}
	defer drainAndClose(response.Body)
	switch response.StatusCode {
	case http.StatusNoContent,
		http.StatusOK,
		http.StatusNotFound,
		http.StatusConflict,
		http.StatusPreconditionFailed:
		return nil
	default:
		return readS3Error(response)
	}
}
