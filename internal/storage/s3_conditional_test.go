package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

func TestS3ConditionalPutResolvesAppliedResponseLoss(t *testing.T) {
	for _, test := range []struct {
		name      string
		condition PutCondition
		initial   []byte
		initialET string
	}{
		{
			name:      "if_match",
			condition: PutCondition{IfMatch: "etag-old"},
			initial:   []byte("old"),
			initialET: "etag-old",
		},
		{
			name:      "if_none_match",
			condition: PutCondition{IfNoneMatch: true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var putCalls atomic.Int32
			var getCalls atomic.Int32
			stored := append([]byte(nil), test.initial...)
			etag := test.initialET
			store := newRoundTripS3Store(t, func(request *http.Request) (*http.Response, error) {
				switch request.Method {
				case http.MethodPut:
					putCalls.Add(1)
					stored = []byte("new")
					etag = "etag-new"
					return nil, errors.New("response lost after write")
				case http.MethodGet:
					getCalls.Add(1)
					return s3RoundTripResponse(request, http.StatusOK, etag, stored), nil
				default:
					t.Fatalf("unexpected request %s %s", request.Method, request.URL)
					return nil, nil
				}
			})

			meta, err := store.PutConditional(
				context.Background(), "manifests/current.parquet", []byte("new"), test.condition,
			)
			if err != nil {
				t.Fatalf("conditional put: %v", err)
			}
			if meta.ETag != "etag-new" {
				t.Fatalf("etag = %q, want etag-new", meta.ETag)
			}
			if putCalls.Load() != 1 || getCalls.Load() != 1 {
				t.Fatalf("put calls=%d get calls=%d, want 1/1", putCalls.Load(), getCalls.Load())
			}
		})
	}
}

func TestS3ConditionalPutResolvesAppliedRetryableHTTPResponse(t *testing.T) {
	var putCalls atomic.Int32
	var getCalls atomic.Int32
	store := newRoundTripS3Store(t, func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodPut:
			putCalls.Add(1)
			return s3RoundTripResponse(
				request,
				http.StatusServiceUnavailable,
				"",
				[]byte("slow down"),
			), nil
		case http.MethodGet:
			getCalls.Add(1)
			return s3RoundTripResponse(
				request,
				http.StatusOK,
				"etag-new",
				[]byte("new"),
			), nil
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})

	meta, err := store.PutConditional(
		context.Background(),
		"manifests/current.parquet",
		[]byte("new"),
		PutCondition{IfNoneMatch: true},
	)
	if err != nil {
		t.Fatalf("conditional put: %v", err)
	}
	if meta.ETag != "etag-new" {
		t.Fatalf("etag = %q, want etag-new", meta.ETag)
	}
	if putCalls.Load() != 1 || getCalls.Load() != 1 {
		t.Fatalf(
			"put calls=%d get calls=%d, want 1/1",
			putCalls.Load(),
			getCalls.Load(),
		)
	}
}

func TestS3ConditionalPutRetriesOnlyWhenOldValueRemains(t *testing.T) {
	var putCalls atomic.Int32
	var getCalls atomic.Int32
	stored := []byte("old")
	etag := "etag-old"
	store := newRoundTripS3Store(t, func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodPut:
			if putCalls.Add(1) == 1 {
				return nil, errors.New("request failed before write")
			}
			stored = []byte("new")
			etag = "etag-new"
			return s3RoundTripResponse(request, http.StatusOK, etag, nil), nil
		case http.MethodGet:
			getCalls.Add(1)
			return s3RoundTripResponse(request, http.StatusOK, etag, stored), nil
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})

	meta, err := store.PutConditional(
		context.Background(),
		"manifests/current.parquet",
		[]byte("new"),
		PutCondition{IfMatch: "etag-old"},
	)
	if err != nil {
		t.Fatalf("conditional put: %v", err)
	}
	if meta.ETag != "etag-new" {
		t.Fatalf("etag = %q, want etag-new", meta.ETag)
	}
	if putCalls.Load() != 2 || getCalls.Load() != 1 {
		t.Fatalf("put calls=%d get calls=%d, want 2/1", putCalls.Load(), getCalls.Load())
	}
}

func TestS3ConditionalPutDoesNotOverwriteChangedValueAfterResponseLoss(t *testing.T) {
	var putCalls atomic.Int32
	store := newRoundTripS3Store(t, func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodPut:
			putCalls.Add(1)
			return nil, errors.New("write outcome unknown")
		case http.MethodGet:
			return s3RoundTripResponse(
				request, http.StatusOK, "etag-other", []byte("concurrent"),
			), nil
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})

	_, err := store.PutConditional(
		context.Background(),
		"manifests/current.parquet",
		[]byte("new"),
		PutCondition{IfMatch: "etag-old"},
	)
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conditional put err = %v, want ErrConflict", err)
	}
	if putCalls.Load() != 1 {
		t.Fatalf("put calls = %d, want 1", putCalls.Load())
	}
}

func TestS3ConditionalDeleteResolvesAppliedResponseLoss(t *testing.T) {
	var deleteCalls atomic.Int32
	var headCalls atomic.Int32
	store := newRoundTripS3Store(t, func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodDelete:
			deleteCalls.Add(1)
			return nil, errors.New("response lost after delete")
		case http.MethodHead:
			headCalls.Add(1)
			return s3RoundTripResponse(request, http.StatusNotFound, "", nil), nil
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})

	if err := store.DeleteConditional(
		context.Background(),
		"coordination/mode.json",
		PutCondition{IfMatch: "etag-old"},
	); err != nil {
		t.Fatalf("conditional delete: %v", err)
	}
	if deleteCalls.Load() != 1 || headCalls.Load() != 1 {
		t.Fatalf(
			"delete calls=%d head calls=%d, want 1/1",
			deleteCalls.Load(), headCalls.Load(),
		)
	}
}

func TestS3ConditionalDeleteResolvesAppliedRetryableHTTPResponse(t *testing.T) {
	var deleteCalls atomic.Int32
	var headCalls atomic.Int32
	store := newRoundTripS3Store(t, func(request *http.Request) (*http.Response, error) {
		switch request.Method {
		case http.MethodDelete:
			deleteCalls.Add(1)
			return s3RoundTripResponse(
				request,
				http.StatusServiceUnavailable,
				"",
				[]byte("slow down"),
			), nil
		case http.MethodHead:
			headCalls.Add(1)
			return s3RoundTripResponse(
				request,
				http.StatusNotFound,
				"",
				nil,
			), nil
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL)
			return nil, nil
		}
	})

	if err := store.DeleteConditional(
		context.Background(),
		"coordination/mode.json",
		PutCondition{IfMatch: "etag-old"},
	); err != nil {
		t.Fatalf("conditional delete: %v", err)
	}
	if deleteCalls.Load() != 1 || headCalls.Load() != 1 {
		t.Fatalf(
			"delete calls=%d head calls=%d, want 1/1",
			deleteCalls.Load(),
			headCalls.Load(),
		)
	}
}

func newRoundTripS3Store(
	t *testing.T,
	roundTrip func(*http.Request) (*http.Response, error),
) *S3Store {
	t.Helper()
	store, err := NewS3StoreWithOptions(
		"http://s3.test", "bucket", "us-east-1", "access", "secret",
		S3Options{PathStyle: true},
	)
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	store.client = &http.Client{Transport: roundTripFunc(roundTrip)}
	store.conditionalDeleteState = s3ConditionalDeleteAvailable
	return store
}

func s3RoundTripResponse(
	request *http.Request,
	status int,
	etag string,
	body []byte,
) *http.Response {
	header := make(http.Header)
	if etag != "" {
		header.Set("ETag", quoteETag(etag))
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(string(body))),
		Request:    request,
	}
}
