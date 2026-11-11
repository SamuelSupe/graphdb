package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func TestHTTPCypherReadOnlyAndFreshness(t *testing.T) {
	ctx := context.Background()
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host", Fields: graph.Fields{"hostname": "app-01"}}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: store, Mode: "all"}).Handler()
	before, err := store.CurrentVersion(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`CREATE (n:host) RETURN n`, `MATCH (n) SET n.hostname='changed' RETURN n`, `MATCH (n) DETACH DELETE n`, `EXPLAIN MATCH (n) REMOVE n.hostname RETURN n`} {
		rr := serveJSON(handler, http.MethodPost, "/v1/query/cypher", "tenant-a", query.CypherRequest{Query: q})
		if rr.Code != http.StatusUnprocessableEntity {
			t.Fatalf("write rejected: %d %s", rr.Code, rr.Body.String())
		}
		var response struct {
			Detail *query.CypherError `json:"detail"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil || response.Detail == nil || response.Detail.Code != "cypher_read_only" {
			t.Fatalf("readonly error=%s err=%v", rr.Body.String(), err)
		}
	}
	after, err := store.CurrentVersion(ctx, "tenant-a")
	if err != nil || after != before {
		t.Fatalf("version before=%d after=%d err=%v", before, after, err)
	}
	rr := serveJSON(handler, http.MethodPost, "/v1/query/cypher", "tenant-a", map[string]any{"query": `MATCH (n:host) RETURN n.hostname AS hostname, $n AS number`, "parameters": map[string]any{"n": int64(9007199254740993)}})
	if rr.Code != http.StatusOK {
		t.Fatalf("query=%d %s", rr.Code, rr.Body.String())
	}
	var response struct {
		Version int64               `json:"version"`
		Rows    [][]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Version != before || len(response.Rows) != 1 || string(response.Rows[0][0]) != `"app-01"` || string(response.Rows[0][1]) != `9007199254740993` {
		t.Fatal(rr.Body.String())
	}
	if rr.Header().Get("X-GraphDB-Query-ID") == "" {
		t.Fatal("missing query ID")
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/query/cypher", "tenant-a", query.CypherRequest{Query: `RETURN 1 AS n`, MinVersion: before + 1})
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("freshness=%d %s", rr.Code, rr.Body.String())
	}
	rr = serveJSON(handler, http.MethodPost, "/v1/query/cypher", "tenant-b", query.CypherRequest{Query: `MATCH (n:host) RETURN n.hostname AS hostname`})
	if rr.Code == http.StatusOK {
		var result query.CypherResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if len(result.Rows) > 0 {
			t.Fatal("cross tenant data returned")
		}
	}
}

func TestHTTPCypherStreamCompletion(t *testing.T) {
	handler := lazyReadHandler(t)
	for _, tc := range []struct {
		query   string
		cost    int
		success bool
		rows    int
	}{{`MATCH (h:host) RETURN h.hostname AS hostname`, 0, true, 1}, {`UNWIND range(1,100) AS x RETURN x`, 5, false, 0}, {`MATCH (n:missing) RETURN n`, 0, true, 0}} {
		rr := serveJSON(handler, http.MethodPost, "/v1/query/cypher/stream", "tenant-a", query.CypherRequest{Query: tc.query, CostLimit: tc.cost})
		if rr.Code != http.StatusOK || rr.Header().Get("Content-Type") != "application/x-ndjson" {
			t.Fatalf("stream=%d %s", rr.Code, rr.Body.String())
		}
		scanner := bufio.NewScanner(strings.NewReader(rr.Body.String()))
		frames := []map[string]any{}
		for scanner.Scan() {
			var frame map[string]any
			if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
				t.Fatal(err)
			}
			frames = append(frames, frame)
		}
		if err := scanner.Err(); err != nil {
			t.Fatal(err)
		}
		if len(frames) < 2 || frames[0]["stream"] != true {
			t.Fatalf("frames=%#v", frames)
		}
		last := frames[len(frames)-1]
		if last["done"] != true || last["success"] != tc.success {
			t.Fatalf("terminal=%#v", last)
		}
		if tc.success && len(frames) != tc.rows+2 {
			t.Fatalf("frames=%d", len(frames))
		}
		if !tc.success && last["error"] == nil {
			t.Fatal("missing terminal error")
		}
	}
}
func TestHTTPCypherPersistedLazyRead(t *testing.T) {
	handler := lazyReadHandler(t)
	for _, q := range []string{`MATCH (n:host) RETURN n.hostname AS hostname ORDER BY hostname`, `MATCH (s:service)-[:runs_on]->(h:host) RETURN h.hostname AS hostname`, `MATCH (h:host)<-[:runs_on]-(s:service) RETURN h.hostname AS hostname`} {
		rr := serveJSON(handler, http.MethodPost, "/v1/query/cypher", "tenant-a", query.CypherRequest{Query: q})
		if rr.Code != http.StatusOK {
			t.Fatalf("lazy query=%d %s", rr.Code, rr.Body.String())
		}
		var result query.CypherResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(result.Rows, [][]any{{"app-01"}}) {
			t.Fatalf("lazy rows=%#v", result.Rows)
		}
	}
}

func TestHTTPCypherIndexKeepsAttachedLabelMatches(t *testing.T) {
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	other := graph.Entity{ID: "service:labeled", Kind: "service", Fields: graph.Fields{"hostname": "target"}}
	if err := graph.SetEntityLabels(&other, []string{"host"}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{UpsertCITypes: []graph.CIType{{Name: "host", Fields: map[string]graph.FieldSpec{"hostname": {Type: "string", Indexed: true}}}}, UpsertEntities: []graph.Entity{{ID: "host:1", Kind: "host", Fields: graph.Fields{"hostname": "target"}}, other}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.RebuildIndexes(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	handler := (&Server{Store: storage.NewTenantStore(&denyGraphLoadStore{base: objects}, "test"), Mode: "reader"}).Handler()
	rr := serveJSON(handler, http.MethodPost, "/v1/query/cypher", "tenant-a", query.CypherRequest{Query: `MATCH (n:host) WHERE n.hostname=$name RETURN graphdb.id(n) AS id ORDER BY id`, Parameters: map[string]any{"name": "target"}})
	if rr.Code != http.StatusOK {
		t.Fatalf("query=%d %s", rr.Code, rr.Body.String())
	}
	var result query.CypherResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.Rows, [][]any{{"host:1"}, {"service:labeled"}}) {
		t.Fatal(result.Rows)
	}
}

func TestHTTPCypherIDLookupRequiresReadablePage(t *testing.T) {
	t.Run("page", func(t *testing.T) { testCypherIDLookupRequiresReadablePage(t, false) })
	t.Run("range_page", func(t *testing.T) { testCypherIDLookupRequiresReadablePage(t, true) })
}

func testCypherIDLookupRequiresReadablePage(t *testing.T, rangePages bool) {
	t.Helper()
	ctx := context.Background()
	objects := storage.NewMemoryStore()
	writer := storage.NewTenantStore(objects, "test")
	writer.WriteIndexRangePages = rangePages
	const id = "host:app-01"
	if _, err := writer.Commit(ctx, "tenant-a", graph.Mutations{UpsertEntities: []graph.Entity{{ID: id, Kind: "host"}}}, storage.CommitOptions{}); err != nil {
		t.Fatal(err)
	}
	catalog, err := writer.RebuildIndexes(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	newReader := func() http.Handler {
		return (&Server{Store: storage.NewTenantStore(&denyGraphLoadStore{base: objects}, "test"), Mode: "reader"}).Handler()
	}
	request := func(id string) query.CypherRequest {
		return query.CypherRequest{Query: `MATCH (n) WHERE graphdb.id(n)=$id RETURN graphdb.id(n) AS id`, Parameters: map[string]any{"id": id}}
	}
	reader := newReader()
	missing := "host:missing"
	for testShardID(missing) != testShardID(id) {
		missing += "x"
	}
	for _, tc := range []struct {
		id   string
		rows [][]any
	}{{id, [][]any{{id}}}, {missing, [][]any{}}} {
		rr := serveJSON(reader, http.MethodPost, "/v1/query/cypher", "tenant-a", request(tc.id))
		var result query.CypherResponse
		if err := json.Unmarshal(rr.Body.Bytes(), &result); rr.Code != http.StatusOK || err != nil || !reflect.DeepEqual(result.Rows, tc.rows) {
			t.Fatalf("lookup %q: status=%d body=%s err=%v", tc.id, rr.Code, rr.Body, err)
		}
	}
	if err := objects.Delete(ctx, catalogEntityPageObjectKey(t, catalog, id)); err != nil {
		t.Fatal(err)
	}
	rr := serveJSON(newReader(), http.MethodPost, "/v1/query/cypher", "tenant-a", request(id))
	var failure ErrorResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &failure); rr.Code != http.StatusBadRequest || err != nil || failure.Code != ErrorCodeIndexStale {
		t.Errorf("lookup with missing index page: status=%d body=%s err=%v", rr.Code, rr.Body, err)
	}
	rr = serveJSON(newReader(), http.MethodPost, "/v1/query/cypher/stream", "tenant-a", request(id))
	var meta map[string]any
	var terminal struct {
		Done    bool
		Success bool
		Error   ErrorResponse
	}
	decoder := json.NewDecoder(rr.Body)
	if err := decoder.Decode(&meta); rr.Code != http.StatusOK || err != nil || meta["stream"] != true {
		t.Fatalf("stream metadata: status=%d meta=%v err=%v", rr.Code, meta, err)
	}
	if err := decoder.Decode(&terminal); err != nil || !terminal.Done || terminal.Success || terminal.Error.Code != ErrorCodeIndexStale {
		t.Fatalf("stream terminal: %+v err=%v", terminal, err)
	}
}
