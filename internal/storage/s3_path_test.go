package storage

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestS3StoreSignsAWSCanonicalObjectPath(t *testing.T) {
	store, err := NewS3StoreWithOptions(
		"https://s3.example.com", "bucket", "us-east-1",
		"access", "secret", S3Options{PathStyle: true},
	)
	if err != nil {
		t.Fatalf("new S3 store: %v", err)
	}
	store.client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		amzDate := request.Header.Get("X-Amz-Date")
		expected := store.authorization(
			request.Method,
			awsCanonicalPathForTest(request.URL.Path),
			request.URL.RawQuery,
			request.URL.Host,
			request.Header.Get("X-Amz-Content-Sha256"),
			amzDate,
			amzDate[:8],
		)
		if got := request.Header.Get("Authorization"); got != expected {
			t.Fatalf("authorization signed a non-AWS canonical path")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"ETag": {`"etag"`}},
			Body:       io.NopCloser(strings.NewReader("")),
			Request:    request,
		}, nil
	})

	key := "indexes/entities/by-id/sample:100.parquet"
	if err := store.Put(context.Background(), key, []byte("entity")); err != nil {
		t.Fatalf("put key containing colon: %v", err)
	}
}

func awsCanonicalPathForTest(value string) string {
	parts := strings.Split(value, "/")
	for index := range parts {
		parts[index] = awsPathEscape(parts[index])
	}
	return strings.Join(parts, "/")
}
