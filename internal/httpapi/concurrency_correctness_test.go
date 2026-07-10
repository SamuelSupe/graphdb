package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestHTTPConcurrentCommitsAndQueriesRemainCorrect(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	handler := (&Server{Store: store, Mode: "all", Admission: NewQueryAdmission(16, 16, 0)}).Handler()
	const writers = 32
	const readers = 32
	var wg sync.WaitGroup
	errs := make(chan string, writers+readers)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr := serveJSON(handler, http.MethodPost, "/v1/commits", "tenant-a", CommitRequest{Mutations: graph.Mutations{
				UpsertEntities: []graph.Entity{{
					ID:     fmt.Sprintf("host:%03d", i),
					Kind:   "host",
					Fields: graph.Fields{"region": fmt.Sprintf("region-%d", i%4), "seq": i},
				}},
			}})
			if rr.Code != http.StatusOK {
				errs <- fmt.Sprintf("commit %d status=%d body=%s", i, rr.Code, rr.Body.String())
			}
		}()
	}
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{
				Op:    "match",
				Kind:  "host",
				Limit: 100,
			})
			if rr.Code != http.StatusOK && rr.Code != http.StatusNotFound {
				errs <- fmt.Sprintf("query status=%d body=%s", rr.Code, rr.Body.String())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	rr := serveJSON(handler, http.MethodPost, "/v1/query", "tenant-a", query.Request{Op: "match", Kind: "host", Limit: writers})
	if rr.Code != http.StatusOK {
		t.Fatalf("final query status=%d body=%s", rr.Code, rr.Body.String())
	}
	var response query.Response
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode final query: %v", err)
	}
	if len(response.Results) != writers {
		t.Fatalf("final query returned %d hosts, want %d body=%s", len(response.Results), writers, rr.Body.String())
	}
	usage := serveJSON(handler, http.MethodGet, "/v1/tenant-usage", "tenant-a", nil)
	if usage.Code != http.StatusOK || !strings.Contains(usage.Body.String(), `"name":"commits"`) {
		t.Fatalf("tenant usage status=%d body=%s", usage.Code, usage.Body.String())
	}
}
