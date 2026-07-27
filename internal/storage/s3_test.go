package storage

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestS3BatchDelete(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/bucket" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusBadRequest)
			return
		}
		if _, ok := r.URL.Query()["delete"]; !ok {
			http.Error(w, "missing delete query", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Content-MD5") == "" || r.Header.Get("Content-Type") != "application/xml" {
			http.Error(w, "missing batch delete headers", http.StatusBadRequest)
			return
		}
		var request deleteObjectsRequest
		if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(request.Objects) == 0 || len(request.Objects) > s3DeleteBatchLimit {
			http.Error(w, "invalid batch size", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte("<DeleteResult/>"))
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	keys := make([]string, s3DeleteBatchLimit+1)
	for i := range keys {
		keys[i] = fmt.Sprintf("objects/%04d", i)
	}
	if err := store.DeleteBatch(context.Background(), keys); err != nil {
		t.Fatalf("batch delete: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("batch delete calls = %d, want 2", calls.Load())
	}
}

func TestS3StoreDefaultsToVirtualHostedStyle(t *testing.T) {
	store, err := NewS3Store("https://tos.example.com/proxy", "bucket", "us-east-1", "access", "secret")
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	store.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "bucket.tos.example.com" || r.URL.Path != "/proxy/objects/current.json" {
			t.Fatalf("request URL = %s, want https://bucket.tos.example.com/proxy/objects/current.json", r.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("ok")),
			Request:    r,
		}, nil
	})
	data, err := store.Get(context.Background(), "objects/current.json")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("data = %q, want ok", string(data))
	}
}

func TestS3StorePathStyleOption(t *testing.T) {
	store, err := NewS3StoreWithOptions("https://tos.example.com/proxy", "bucket", "us-east-1", "access", "secret", S3Options{PathStyle: true})
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	store.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "tos.example.com" || r.URL.Path != "/proxy/bucket/objects/current.json" {
			t.Fatalf("request URL = %s, want https://tos.example.com/proxy/bucket/objects/current.json", r.URL.String())
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("ok")),
			Request:    r,
		}, nil
	})
	data, err := store.Get(context.Background(), "objects/current.json")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("data = %q, want ok", string(data))
	}
}

func TestS3ConditionalPutFetchesMissingETag(t *testing.T) {
	var sawConditionalPut atomic.Bool
	var sawHead atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/bucket/manifests/current.json":
			if r.Header.Get("If-None-Match") != "*" {
				t.Errorf("If-None-Match = %q, want *", r.Header.Get("If-None-Match"))
			}
			sawConditionalPut.Store(true)
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/bucket/manifests/current.json":
			sawHead.Store(true)
			w.Header().Set("ETag", `"etag-from-head"`)
		default:
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
		}
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	meta, err := store.PutConditional(context.Background(), "manifests/current.json", []byte(`{"ok":true}`), PutCondition{IfNoneMatch: true})
	if err != nil {
		t.Fatalf("put conditional: %v", err)
	}
	if !sawConditionalPut.Load() {
		t.Fatal("conditional put was not called")
	}
	if !sawHead.Load() {
		t.Fatal("conditional put fallback did not use HEAD")
	}
	if meta.ETag != "etag-from-head" {
		t.Fatalf("etag = %q, want etag-from-head", meta.ETag)
	}
}

func TestNewS3StoreSetsRequestTimeout(t *testing.T) {
	store, err := NewS3Store("http://127.0.0.1:9000", "bucket", "us-east-1", "access", "secret")
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	if store.client == nil {
		t.Fatal("client is nil")
	}
	if store.client.Timeout != defaultS3RequestTimeout {
		t.Fatalf("client timeout = %s, want %s", store.client.Timeout, defaultS3RequestTimeout)
	}
}

func TestS3StoreRetriesTransientTransportErrors(t *testing.T) {
	var serverCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalls.Add(1)
		if r.Method != http.MethodPut || r.URL.Path != "/bucket/objects/current.json" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"etag-after-retry"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	base := server.Client().Transport
	if base == nil {
		base = http.DefaultTransport
	}
	var attempts atomic.Int32
	store.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if attempts.Add(1) < defaultS3MaxAttempts {
			return nil, errors.New("connect: cannot assign requested address")
		}
		return base.RoundTrip(r)
	})
	meta, err := store.PutConditional(context.Background(), "objects/current.json", []byte("ok"), PutCondition{})
	if err != nil {
		t.Fatalf("put after retry: %v", err)
	}
	if meta.ETag != "etag-after-retry" {
		t.Fatalf("etag = %q, want etag-after-retry", meta.ETag)
	}
	if attempts.Load() != defaultS3MaxAttempts || serverCalls.Load() != 1 {
		t.Fatalf("attempts=%d server_calls=%d", attempts.Load(), serverCalls.Load())
	}
}

func TestS3StoreRetriesTransientHTTPResponses(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/bucket/indexes/page.parquet" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		if attempts.Add(1) < defaultS3MaxAttempts {
			http.Error(w, "slow down", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	data, err := store.Get(context.Background(), "indexes/page.parquet")
	if err != nil {
		t.Fatalf("get after status retry: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("data = %q, want ok", data)
	}
	if got := attempts.Load(); got != defaultS3MaxAttempts {
		t.Fatalf("attempts = %d, want %d", got, defaultS3MaxAttempts)
	}
}

func TestS3StoreRetriesClientTimeoutWhenCallerContextAlive(t *testing.T) {
	var serverCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverCalls.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/bucket/indexes/page.parquet" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	base := server.Client().Transport
	if base == nil {
		base = http.DefaultTransport
	}
	var attempts atomic.Int32
	store.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if attempts.Add(1) == 1 {
			return nil, fmt.Errorf("Get %q: %w (Client.Timeout exceeded while awaiting headers)", r.URL.String(), context.DeadlineExceeded)
		}
		return base.RoundTrip(r)
	})
	data, err := store.Get(context.Background(), "indexes/page.parquet")
	if err != nil {
		t.Fatalf("get after client timeout retry: %v", err)
	}
	if string(data) != "ok" {
		t.Fatalf("data = %q, want ok", string(data))
	}
	if attempts.Load() != 2 || serverCalls.Load() != 1 {
		t.Fatalf("attempts=%d server_calls=%d", attempts.Load(), serverCalls.Load())
	}
}

func TestS3StoreDoesNotRetryCallerContextDeadline(t *testing.T) {
	store, err := NewS3Store("http://127.0.0.1:9000", "bucket", "us-east-1", "access", "secret")
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	var attempts atomic.Int32
	store.client.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, context.DeadlineExceeded
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = store.Get(ctx, "indexes/page.parquet")
	if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("get err = %v, want caller context error", err)
	}
	if attempts.Load() > 1 {
		t.Fatalf("attempts=%d, want no retry after caller context cancellation", attempts.Load())
	}
}

func TestS3StorePreservesEndpointPathPrefix(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/proxy/bucket/objects/current.json" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"etag-from-put"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store, err := NewS3StoreWithOptions(server.URL+"/proxy", "bucket", "us-east-1", "access", "secret", S3Options{PathStyle: true})
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	store.client = server.Client()
	meta, err := store.PutConditional(context.Background(), "objects/current.json", []byte("ok"), PutCondition{})
	if err != nil {
		t.Fatalf("put conditional: %v", err)
	}
	if meta.ETag != "etag-from-put" {
		t.Fatalf("etag = %q, want etag-from-put", meta.ETag)
	}
}

func TestS3ListErrorIncludesResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/bucket" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		http.Error(w, "bucket unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	_, err := store.List(context.Background(), "objects/")
	if err == nil || !strings.Contains(err.Error(), "bucket unavailable") || !errors.Is(err, ErrObjectStoreUnavailable) {
		t.Fatalf("list err = %v, want response body", err)
	}
}

func TestS3ListFailsClosedWhenTruncatedWithoutContinuationToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/bucket" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("prefix") != "objects/" {
			http.Error(w, "unexpected prefix", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>objects/a.json</Key><Size>1</Size><ETag>"etag-a"</ETag></Contents></ListBucketResult>`))
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	_, err := store.List(context.Background(), "objects/")
	if err == nil || !strings.Contains(err.Error(), "truncated without continuation token") {
		t.Fatalf("list err = %v, want truncated token error", err)
	}
}

func TestS3ListFailsClosedOnRepeatedContinuationToken(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/bucket" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("prefix") != "objects/" {
			http.Error(w, "unexpected prefix", http.StatusBadRequest)
			return
		}
		call := calls.Add(1)
		if call == 2 && r.URL.Query().Get("continuation-token") != "same-token" {
			http.Error(w, "unexpected continuation token", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>same-token</NextContinuationToken><Contents><Key>objects/a.json</Key><Size>1</Size><ETag>"etag-a"</ETag></Contents></ListBucketResult>`))
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	_, err := store.List(context.Background(), "objects/")
	if err == nil || !strings.Contains(err.Error(), "repeated continuation token") {
		t.Fatalf("list err = %v, want repeated token error", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

func TestS3ListPageUsesStartAfterAndLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/bucket" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		query := r.URL.Query()
		if query.Get("prefix") != "objects/" || query.Get("start-after") != "objects/a" || query.Get("max-keys") != "2" {
			http.Error(w, "unexpected page query", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>objects/b</Key><Size>2</Size></Contents><Contents><Key>objects/c</Key><Size>3</Size></Contents></ListBucketResult>`))
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	items, next, err := store.ListPage(context.Background(), "objects/", "objects/a", 2)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if len(items) != 2 || items[0].Key != "objects/b" || items[1].Key != "objects/c" || next != "objects/c" {
		t.Fatalf("items=%#v next=%q", items, next)
	}
}

func TestS3ConditionalPutRequiresETagAfterFallback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/bucket/manifests/current.json":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodHead && r.URL.Path == "/bucket/manifests/current.json":
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
		}
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	_, err := store.PutConditional(context.Background(), "manifests/current.json", []byte(`{"ok":true}`), PutCondition{IfNoneMatch: true})
	if err == nil || !strings.Contains(err.Error(), "completed without returned etag") {
		t.Fatalf("put conditional err = %v, want missing etag error", err)
	}
}

func TestPutBytesWithMetaUsesS3HeadForExistingObjectMeta(t *testing.T) {
	var getCalls atomic.Int32
	var headCalls atomic.Int32
	var putCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead && r.URL.Path == "/bucket/indexes/current.parquet":
			headCalls.Add(1)
			w.Header().Set("ETag", `"etag-current"`)
		case r.Method == http.MethodGet && r.URL.Path == "/bucket/indexes/current.parquet":
			getCalls.Add(1)
			w.Header().Set("ETag", `"etag-current"`)
			_, _ = w.Write([]byte("PAR1old"))
		case r.Method == http.MethodPut && r.URL.Path == "/bucket/indexes/current.parquet":
			putCalls.Add(1)
			if r.Header.Get("If-Match") != `"etag-current"` {
				t.Errorf("If-Match = %q, want quoted current etag", r.Header.Get("If-Match"))
			}
			w.Header().Set("ETag", `"etag-new"`)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
		}
	}))
	defer server.Close()

	s3 := newTestS3Store(t, server)
	store := NewTenantStore(s3, "test")
	ctx := context.Background()
	meta, err := objectMeta(ctx, store.Objects, "indexes/current.parquet")
	if err != nil {
		t.Fatalf("object meta: %v", err)
	}
	if err := store.putBytesWithMeta(ctx, "indexes/current.parquet", []byte("PAR1new"), meta); err != nil {
		t.Fatalf("put bytes with meta: %v", err)
	}
	if headCalls.Load() != 1 || putCalls.Load() != 1 || getCalls.Load() != 0 {
		t.Fatalf("calls head=%d put=%d get=%d, want 1/1/0", headCalls.Load(), putCalls.Load(), getCalls.Load())
	}
}

func TestS3ConditionalPutConflictStatusMapsToErrConflict(t *testing.T) {
	for _, status := range []int{http.StatusPreconditionFailed, http.StatusConflict} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPut || r.URL.Path != "/bucket/manifests/current.json" {
					http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
					return
				}
				w.WriteHeader(status)
			}))
			defer server.Close()

			store := newTestS3Store(t, server)
			_, err := store.PutConditional(context.Background(), "manifests/current.json", []byte(`{"ok":true}`), PutCondition{IfNoneMatch: true})
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("put err = %v, want ErrConflict", err)
			}
		})
	}
}

func TestS3ConditionalDeleteWithIfMatch(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodDelete || r.URL.Path != "/bucket/objects/current.json" || r.Header.Get("If-Match") != `"etag-old"` {
			http.Error(w, fmt.Sprintf("unexpected %s %s if-match=%q", r.Method, r.URL.Path, r.Header.Get("If-Match")), http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	err := store.DeleteConditional(context.Background(), "objects/current.json", PutCondition{IfMatch: "etag-old"})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("conditional delete made %d HTTP calls, want 1", calls.Load())
	}
}

func TestS3ConditionalDeleteConflictStatusMapsToErrConflict(t *testing.T) {
	for _, status := range []int{http.StatusPreconditionFailed, http.StatusConflict, http.StatusNotFound} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer server.Close()
			store := newTestS3Store(t, server)
			err := store.DeleteConditional(context.Background(), "objects/current.json", PutCondition{IfMatch: "etag-old"})
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("delete err = %v, want ErrConflict", err)
			}
		})
	}
}

func TestS3ConditionalDeleteWithIfNoneMatchUsesHead(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodHead {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusBadRequest)
			return
		}
		w.Header().Set("ETag", `"existing"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	err := store.DeleteConditional(context.Background(), "objects/current.json", PutCondition{IfNoneMatch: true})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("delete err = %v, want ErrConflict", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("conditional delete made %d HTTP calls, want 1", calls.Load())
	}
}

func TestS3UnconditionalPutConflictStatusIsNotCASConflict(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/bucket/objects/current.json" {
			http.Error(w, fmt.Sprintf("unexpected %s %s", r.Method, r.URL.Path), http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	_, err := store.PutConditional(context.Background(), "objects/current.json", []byte("ok"), PutCondition{})
	if err == nil || errors.Is(err, ErrConflict) {
		t.Fatalf("put err = %v, want non-CAS S3 error", err)
	}
}

func TestS3StoreRejectsEmptyObjectKeysBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	store := newTestS3Store(t, server)
	ctx := context.Background()
	if err := store.Put(ctx, "", []byte("x")); err == nil {
		t.Fatal("put empty key succeeded")
	}
	if _, err := store.PutConditional(ctx, "", []byte("x"), PutCondition{}); err == nil {
		t.Fatal("conditional put empty key succeeded")
	}
	if _, err := store.Get(ctx, ""); err == nil {
		t.Fatal("get empty key succeeded")
	}
	if _, _, err := store.GetWithMeta(ctx, ""); err == nil {
		t.Fatal("get meta empty key succeeded")
	}
	if err := store.Delete(ctx, ""); err == nil {
		t.Fatal("delete empty key succeeded")
	}
	if err := store.DeleteConditional(ctx, "", PutCondition{IfMatch: "etag"}); err == nil {
		t.Fatal("conditional delete empty key succeeded")
	}
	if calls.Load() != 0 {
		t.Fatalf("empty key operations made %d HTTP calls", calls.Load())
	}
}

func newTestS3Store(t *testing.T, server *httptest.Server) *S3Store {
	t.Helper()
	store, err := NewS3StoreWithOptions(server.URL, "bucket", "us-east-1", "access", "secret", S3Options{PathStyle: true})
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	store.client = server.Client()
	store.conditionalDeleteState = s3ConditionalDeleteAvailable
	return store
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}
