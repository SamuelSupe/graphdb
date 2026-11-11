package query

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func TestCypherTCKReadOnly(t *testing.T) {
	data, err := os.Open("testdata/cypher-tck.json")
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	var suite struct {
		Cases []struct {
			Source, Name, Query, Excluded string
			Columns                       []string
			Rows                          [][]any
			Parameters                    map[string]any
			Ordered                       bool
		}
	}
	d := json.NewDecoder(data)
	d.UseNumber()
	if err := d.Decode(&suite); err != nil {
		t.Fatal(err)
	}
	passed, unsupported, excluded, resourceLimited := 0, 0, 0, 0
	for _, tc := range suite.Cases {
		if tc.Excluded != "" {
			excluded++
			continue
		}
		t.Run(tc.Source+"/"+tc.Name, func(t *testing.T) {
			p, err := ParseCypher(CypherRequest{Query: tc.Query, Parameters: tc.Parameters, CostLimit: maxQueryCostLimit})
			var ce *CypherError
			if errors.As(err, &ce) && ce.Code == "cypher_unsupported" {
				unsupported++
				t.Skip(ce.Message)
			}
			if err != nil {
				t.Fatal(err)
			}
			response, err := p.Execute(context.Background(), graph.New(), ExecuteOptions{})
			if errors.Is(err, ErrLimitExceeded) {
				resourceLimited++
				t.Skip("bounded execution profile: ", err)
			}
			if err != nil {
				t.Fatal(err)
			}
			actualColumns := append([]string(nil), response.Columns...)
			expectedColumns := append([]string(nil), tc.Columns...)
			sort.Strings(actualColumns)
			sort.Strings(expectedColumns)
			if !reflect.DeepEqual(actualColumns, expectedColumns) {
				t.Fatalf("columns=%q expected=%q", response.Columns, tc.Columns)
			}
			want := make([]string, len(tc.Rows))
			actual := make([]string, len(response.Rows))
			for i, row := range tc.Rows {
				v, err := cyNormalize(row, 0)
				if err != nil {
					t.Fatal(err)
				}
				want[i] = cyKey(v)
			}
			for i, row := range response.Rows {
				reordered := make([]any, len(tc.Columns))
				for column, name := range tc.Columns {
					for index, actualName := range response.Columns {
						if actualName == name {
							reordered[column] = row[index]
							break
						}
					}
				}
				actual[i] = cyKey(reordered)
			}
			if !tc.Ordered {
				sort.Strings(want)
				sort.Strings(actual)
			}
			if strings.Join(actual, "\n") != strings.Join(want, "\n") {
				t.Fatalf("rows=%#v expected=%#v", response.Rows, tc.Rows)
			}
			passed++
		})
	}
	t.Logf("TCK M23 read-only scenarios: total=%d passed=%d unsupported=%d adapter_excluded=%d resource_limited=%d", len(suite.Cases), passed, unsupported, excluded, resourceLimited)
}
