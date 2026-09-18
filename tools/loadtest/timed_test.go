package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestTimedWALWaitsThroughRetryAndChecksReadableVersion(t *testing.T) {
	for _, readable := range []int{7, 6} {
		t.Run(fmt.Sprint(readable), func(t *testing.T) {
			var polls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/ingest/batches":
					w.WriteHeader(http.StatusAccepted)
					fmt.Fprint(w, `{"state":"accepted","status_url":"/status"}`)
				case "/status":
					switch polls.Add(1) {
					case 1:
						fmt.Fprint(w, `{"state":"retrying","last_error":"index rebuild is running"}`)
					case 2:
						fmt.Fprint(w, `{"state":"published","result":{"version":7}}`)
					default:
						fmt.Fprint(w, `{"state":"committed","result":{"version":7}}`)
					}
				case "/v1/entities/host:seed":
					fmt.Fprintf(w, `{"version":%d}`, readable)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			metrics := newRegistry()
			version, err := newClient(server.URL, "bench", time.Second).timedIngest(ctx, metrics, storage.IngestRequest{}, true)
			if version != 7 || polls.Load() != 3 {
				t.Fatalf("returned before terminal publication: version=%d polls=%d error=%v", version, polls.Load(), err)
			}
			if (err != nil) != (readable < 7) || metrics.hasErrors() != (readable < 7) {
				t.Fatalf("readable=%d error=%v metrics=%+v", readable, err, metrics.snapshot())
			}
		})
	}
}
