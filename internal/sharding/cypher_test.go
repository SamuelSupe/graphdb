package sharding

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

func TestCypherPartitionedHTTPContract(t *testing.T) {
	now := time.Now().UTC()
	g := partition.Graph{Tenant: "tenant-a", State: "active", Version: 1, Generation: 1, ReadLease: &partition.Pin{Version: 1, Generation: 1, ExpiresAt: now.Add(4 * partition.MaxQueryLifetime), KeepUntil: now.Add(time.Minute)}}
	client := NewClient("")
	defer client.HTTP.CloseIdleConnections()
	for _, tc := range []struct {
		path, body string
		status     int
	}{{"/v1/query/cypher", `{"query":"RETURN $n AS n","parameters":{"n":9007199254740993}}`, 200}, {"/v1/query/cypher", `{"query":"CREATE (n) RETURN n"}`, 422}, {"/v1/query/cypher", `{"query":"UNWIND range(1,100) AS n RETURN n","cost_limit":5}`, 429}, {"/v1/query/cypher/stream", `{"query":"RETURN 1 AS n"}`, 200}} {
		request := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
		response := httptest.NewRecorder()
		if !ServeGraphData(response, request, g, partition.Group{}, client) {
			t.Fatal("route not handled")
		}
		if response.Code != tc.status {
			t.Fatalf("response=%d %s", response.Code, response.Body.String())
		}
		if tc.path == "/v1/query/cypher" && tc.status == 200 {
			var body query.CypherResponse
			decoder := json.NewDecoder(response.Body)
			decoder.UseNumber()
			if err := decoder.Decode(&body); err != nil {
				t.Fatal(err)
			}
			if len(body.Rows) != 1 || body.Rows[0][0].(json.Number).String() != "9007199254740993" {
				t.Fatal(body.Rows)
			}
		}
		if tc.path == "/v1/query/cypher/stream" {
			decoder := json.NewDecoder(response.Body)
			var frame map[string]any
			for i := 0; i < 3; i++ {
				if err := decoder.Decode(&frame); err != nil {
					t.Fatal(err)
				}
			}
			if frame["done"] != true || frame["success"] != true {
				t.Fatal(frame)
			}
		}
	}
}
