package query

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

func cypherGraph(t testing.TB) *graph.Graph {
	t.Helper()
	g := graph.New()
	entities := []graph.Entity{
		{ID: "person:a", Kind: "person", Fields: graph.Fields{"name": "Alice", "team": "x", "score": int64(3), "id": "business-a"}},
		{ID: "person:b", Kind: "person", Fields: graph.Fields{"name": "Bob", "team": "x", "score": int64(2)}},
		{ID: "person:c", Kind: "person", Fields: graph.Fields{"name": "Carol", "team": "y", "score": int64(1)}},
	}
	if err := graph.SetEntityLabels(&entities[1], []string{"production", "person"}); err != nil {
		t.Fatal(err)
	}
	if err := g.ApplyCommit(graph.Commit{ID: "seed", Version: 1, Mutations: graph.Mutations{UpsertEntities: entities, UpsertRelationTypes: []graph.RelationType{{Name: "links", FromKind: "person", ToKind: "person", Directed: true}}, UpsertEdges: []graph.Edge{{Type: "links", From: "person:a", To: "person:b"}, {Type: "links", From: "person:b", To: "person:a"}, {Type: "links", From: "person:a", To: "person:a"}}}}); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestCypherPlanConcurrentExecution(t *testing.T) {
	g := cypherGraph(t)
	request := CypherRequest{Query: `MATCH (n:person) WHERE graphdb.id(n)=$id RETURN n.name AS name`, Parameters: map[string]any{"id": "person:a"}}
	plan, err := ParseCypher(request)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for i := 0; i < 8; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := 0; i < 10; i++ {
				p, err := ParseCypher(request)
				if err != nil {
					t.Error(err)
					return
				}
				for _, p := range []*CypherPlan{p, plan} {
					response, err := p.Execute(context.Background(), g, ExecuteOptions{})
					if err != nil || !reflect.DeepEqual(response.Rows, [][]any{{"Alice"}}) {
						t.Errorf("concurrent rows=%#v err=%v", response.Rows, err)
						return
					}
				}
			}
		}()
	}
	workers.Wait()
}

func TestCypherPipelineLimitAndSinkFailureStopReads(t *testing.T) {
	g := graph.New()
	entities := make([]graph.Entity, 2000)
	for i := range entities {
		entities[i] = graph.Entity{ID: fmt.Sprintf("node:%04d", i), Kind: "node", Fields: graph.Fields{"score": i}}
	}
	if err := g.ApplyCommit(graph.Commit{ID: "seed", Version: 1, Mutations: graph.Mutations{UpsertEntities: entities}}); err != nil {
		t.Fatal(err)
	}
	p, err := ParseCypher(CypherRequest{Query: `MATCH (n:node) RETURN n.score AS score LIMIT 1`, CostLimit: 20})
	if err != nil {
		t.Fatal(err)
	}
	response, err := p.Execute(context.Background(), g, ExecuteOptions{})
	if err != nil || len(response.Rows) != 1 || response.Stats.Scanned != 1 {
		t.Fatalf("early stop=%#v err=%v", response, err)
	}
	p, err = ParseCypher(CypherRequest{Query: `MATCH (n:node) RETURN n.score AS score`})
	if err != nil {
		t.Fatal(err)
	}
	sinkError := errors.New("consumer disconnected")
	calls := 0
	_, err = p.ExecuteStream(context.Background(), g, ExecuteOptions{}, func(row []any) error { calls++; return sinkError })
	if !errors.Is(err, sinkError) || calls != 1 {
		t.Fatalf("sink calls=%d err=%v", calls, err)
	}
}

func FuzzParseCypher(f *testing.F) {
	for _, s := range []string{`RETURN 1`, `MATCH (n) RETURN n`, `CREATE (n)`, `RETURN [x IN [1,null] | x]`, `RETURN '\\'`, `EXPLAIN MATCH p=(n)-[*0..3]->(m) RETURN p`} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 65536 {
			t.Skip()
		}
		_, _ = ParseCypher(CypherRequest{Query: s})
	})
}
func runCypher(t testing.TB, g *graph.Graph, q string, parameters map[string]any) CypherResponse {
	t.Helper()
	p, err := ParseCypher(CypherRequest{Query: q, Parameters: parameters})
	if err != nil {
		t.Fatal(err)
	}
	res, err := p.Execute(context.Background(), g, ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return res
}
func TestCypherQuerySemantics(t *testing.T) {
	g := cypherGraph(t)
	cases := []struct{ name, query, rows string }{
		{"scan", `MATCH (n:person) RETURN n.name AS name ORDER BY name`, `[["Alice"],["Bob"],["Carol"]]`},
		{"labels", `MATCH (n:production:person) RETURN n.name AS name`, `[["Bob"]]`},
		{"business identity", `MATCH (n:person) WHERE graphdb.id(n) = 'person:a' RETURN n.id AS id`, `[["business-a"]]`},
		{"three valued logic", `RETURN NOT null AS a, null OR true AS b, null AND false AS c, null = null AS d, 1 IN [2,null] AS e`, `[[null,true,false,null,null]]`},
		{"case sensitive strings", `RETURN 'Alice' CONTAINS 'alice' AS a, 'Alice' STARTS WITH 'Al' AS b`, `[[false,true]]`},
		{"aggregation", `MATCH (n:person) RETURN n.team AS team, count(*) AS n, sum(n.score) AS total ORDER BY team`, `[["x",2,5],["y",1,1]]`},
		{"empty aggregate", `MATCH (n:missing) RETURN count(*) AS n, collect(n) AS values, sum(n.score) AS total, avg(n.score) AS mean`, `[[0,[],0,null]]`},
		{"count and collect", `UNWIND [1,1,null,2] AS x RETURN count(*) AS n, count(x) AS nonnull, count(DISTINCT x) AS distinct_count, collect(DISTINCT x) AS values`, `[[4,3,2,[1,2]]]`},
		{"optional", `MATCH (n:person) OPTIONAL MATCH (n)-[:missing]->(m) RETURN n.name AS name, m.name AS other ORDER BY name`, `[["Alice",null],["Bob",null],["Carol",null]]`},
		{"with", `MATCH (n:person) WITH n.team AS team, count(*) AS total WHERE total > 1 RETURN team, total`, `[["x",2]]`},
		{"zero limit", `MATCH (n) RETURN n LIMIT 0`, `[]`},
		{"skip limit", `MATCH (n) RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 1`, `[["Bob"]]`},
		{"union", `RETURN 1 AS x UNION RETURN 1 AS x UNION RETURN 2 AS x`, `[[1],[2]]`},
		{"union all", `RETURN 1 AS x UNION ALL RETURN 1 AS x`, `[[1],[1]]`},
		{"case and lists", `RETURN CASE WHEN 2 > 1 THEN 'yes' ELSE 'no' END AS x, [x IN [1,2,3] WHERE x > 1 | x*2] AS values`, `[["yes",[4,6]]]`},
		{"quantifier", `RETURN all(x IN [1,2] WHERE x > 0) AS a, single(x IN [1,2] WHERE x=1) AS b, any(x IN [null] WHERE x=1) AS c`, `[[true,true,null]]`},
		{"dynamic property and slice", `RETURN {x: 3}['x'] AS x, [1,2,3][-2..] AS xs`, `[[3,[2,3]]]`},
		{"cycle and self loop", `MATCH p = (n:person)-[:links*0..2]->(m) WHERE graphdb.id(n) = 'person:a' RETURN length(p) AS hops, m.name AS name ORDER BY hops,name`, `[[0,"Alice"],[1,"Alice"],[1,"Bob"],[2,"Alice"],[2,"Bob"]]`},
		{"same match relationship uniqueness", `MATCH (a)-[r:links]->(b), (b)-[s:links]->(a) WHERE graphdb.id(a)='person:a' RETURN count(*) AS n`, `[[1]]`},
		{"separate match relationship scope", `MATCH (a)-[r:links]->(b) WHERE graphdb.id(a)='person:a' MATCH (b)-[s:links]->(a) RETURN count(*) AS n`, `[[2]]`},
		{"undirected self loop once", `MATCH (a)-[r:links]-(a) RETURN count(*) AS n`, `[[1]]`},
		{"escaped identifiers", "MATCH (`a b`:person) RETURN `a b`.name AS `person name` ORDER BY `person name`", `[["Alice"],["Bob"],["Carol"]]`},
		{"label predicate", `MATCH (n:person) WHERE n:production:person RETURN n.name AS name`, `[["Bob"]]`},
		{"where sees pre-WITH input", `UNWIND [1,2] AS n WITH n+1 AS x WHERE n=1 RETURN x`, `[[2]]`},
		{"distinct where binds quantifier variable", `MATCH (n:person) WITH DISTINCT n.name AS name WHERE any(x IN [name] WHERE x='Alice') RETURN name`, `[["Alice"]]`},
		{"distinct where binds pattern variable", `MATCH (n:person) WITH DISTINCT n AS person WHERE EXISTS { (person)-[:links]->(m) WHERE m.name='Bob' } RETURN person.name AS name`, `[["Alice"]]`},
		{"distinct pattern can use a projected node", `MATCH (n:person) WITH DISTINCT n AS person WHERE EXISTS { (n)-[:links]->(m) WHERE m.name='Bob' } RETURN person.name AS name`, `[["Alice"]]`},
		{"distinct sort by projected arithmetic", `UNWIND [2,1,2] AS n RETURN DISTINCT n+1 AS x ORDER BY n+1`, `[[2],[3]]`},
		{"sort distinguishes integer and floating aggregate operands", `UNWIND [2,3] AS n RETURN n AS original, sum(n/2) AS whole, sum(n/2.0) AS half ORDER BY sum(n/2.0) DESC, original`, `[[3,1,1.5],[2,1,1]]`},
		{"implicit grouping", `UNWIND [1,1,2] AS n RETURN n+count(*) AS result ORDER BY result`, `[[3],[3]]`},
		{"group by correlated pattern", `MATCH (n:person) RETURN EXISTS { (n)-[:links]->() } AS linked, count(*) AS total ORDER BY linked`, `[[false,1],[true,2]]`},
		{"sort by pattern aggregate alias", `MATCH (n:person) RETURN n.name AS name, count(CASE WHEN EXISTS { (n)-[:links]->() } THEN 1 END) AS links ORDER BY links, name`, `[["Carol",0],["Alice",1],["Bob",1]]`},
		{"constant comprehension on empty input", `UNWIND [] AS n RETURN [x IN [1,2] | x] AS xs, count(*) AS total`, `[[[1,2],0]]`},
		{"aggregate with constant on empty input", `MATCH (n:missing) RETURN 1 AS k,count(*) AS n`, `[[1,0]]`},
		{"pattern predicate", `MATCH (n:person) WHERE (n)-[:links]->() RETURN n.name AS name ORDER BY name`, `[["Alice"],["Bob"]]`},
		{"pattern exists", `MATCH (n:person) RETURN n.name AS name, EXISTS { (n)-[:links]->(m) WHERE m.name='Bob' } AS linked ORDER BY name`, `[["Alice",true],["Bob",false],["Carol",false]]`},
		{"pattern comprehension", `MATCH (n:person) WHERE graphdb.id(n)='person:b' RETURN [(n)-[:links]->(m) | m.name] AS names`, `[[["Alice"]]]`},
		{"bounded shortest", `MATCH (a:person),(b:person) WHERE graphdb.id(a)='person:a' AND graphdb.id(b)='person:b' MATCH p=shortestPath((a)-[:links*0..3]->(b)) RETURN length(p) AS hops`, `[[1]]`},
		{"optional shortest no path", `MATCH (a:person),(b:person) WHERE graphdb.id(a)='person:a' AND graphdb.id(b)='person:c' OPTIONAL MATCH p=shortestPath((a)-[:links*0..3]->(b)) RETURN p`, `[[null]]`},
		{"reversed relationship list order", `MATCH (a)-[rs:links*2]->(b) WHERE graphdb.id(a)='person:a' AND graphdb.id(b)='person:a' RETURN graphdb.id(startNode(rs[0])) AS first, graphdb.id(endNode(rs[0])) AS second`, `[["person:a","person:b"]]`},
		{"pattern reversal respects correlated property", `MATCH (a:person)-[:links]->(b:person {score: CASE WHEN EXISTS { (a)-[:links]->(a) } THEN 3 ELSE 2 END}) WHERE graphdb.id(b)='person:a' RETURN a.name AS name ORDER BY name`, `[["Alice"]]`},
		{"reuse relationship list across MATCH", `MATCH (a)-[rs:links*1]->(b) WHERE graphdb.id(a)='person:a' MATCH (a)-[rs:links*1]->(b) RETURN count(*) AS total`, `[[2]]`},
		{"scalar UNWIND through aggregate", `UNWIND 7 AS n RETURN count(n) AS total`, `[[1]]`},
		{"scalar UNWIND through sort", `UNWIND 7 AS n RETURN n ORDER BY n`, `[[7]]`},
		{"repeated labels with indexed equality", `MATCH (n:person:person) WHERE n.score = 3 RETURN n.name AS name`, `[["Alice"]]`},
		{"integer conversion at signed bounds", `RETURN toInteger('-9223372036854775809') AS below, toInteger('-9223372036854775808') AS min, toInteger('9223372036854775808') AS above, toInteger('18446744073709551616e-18') AS exponent`, `[[null,-9223372036854775808,null,18]]`},
		{"ordinary maps retain user keys", `RETURN keys({__graphdb_labels: 'user data'}) AS names, properties({__graphdb_labels: 'user data'}) AS values`, `[[["__graphdb_labels"],{"__graphdb_labels":"user data"}]]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response := runCypher(t, g, tc.query, nil)
			b, err := json.Marshal(response.Rows)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != tc.rows {
				t.Fatalf("rows=%s want=%s", b, tc.rows)
			}
		})
	}
}

func TestCypherProjectionPreservesColumnNames(t *testing.T) {
	response := runCypher(t, graph.New(), "WITH '观测' AS `a``b` RETURN `a``b`, `a``b` + '云'", nil)
	if want := []string{"a`b", "`a``b` + '云'"}; !reflect.DeepEqual(response.Columns, want) {
		t.Fatalf("columns=%q want=%q", response.Columns, want)
	}
	if want := [][]any{{"观测", "观测云"}}; !reflect.DeepEqual(response.Rows, want) {
		t.Fatalf("rows=%q want=%q", response.Rows, want)
	}
}

func TestCypherProjectionRejectsUnprojectedPatternBindings(t *testing.T) {
	for _, q := range []string{
		`MATCH (n:person) WITH DISTINCT 1 AS k WHERE (n)-[:links]->() RETURN k`,
		`MATCH (n:person) WITH DISTINCT 1 AS k WHERE EXISTS { (:person {name:n.name}) } RETURN k`,
	} {
		_, err := ParseCypher(CypherRequest{Query: q})
		var detail *CypherError
		if !errors.As(err, &detail) || detail.Code != "cypher_semantic_error" {
			t.Errorf("accepted an unprojected binding in %q: %v", q, err)
		}
	}
}

func TestCypherListLimitsApplyToExpressionResults(t *testing.T) {
	for _, tc := range []struct {
		name, expr string
		parameters map[string]any
	}{
		{"split", `split($text, '')`, map[string]any{"text": strings.Repeat("观", cypherMaxRows+1)}},
		{"concat", `range(1,50000) + range(50001,100001)`, nil},
		{"typed parameter", `$values`, map[string]any{"values": make([]string, cypherMaxRows+1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParseCypher(CypherRequest{Query: "RETURN size(" + tc.expr + ") AS total", Parameters: tc.parameters, CostLimit: maxQueryCostLimit})
			if err == nil {
				_, err = p.Execute(context.Background(), graph.New(), ExecuteOptions{})
			}
			if !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("oversized list: %v", err)
			}
		})
	}
	r := runCypher(t, graph.New(), `RETURN size(split($text, '')) AS total`, map[string]any{"text": strings.Repeat("观", cypherMaxRows)})
	if !reflect.DeepEqual(r.Rows, [][]any{{int64(cypherMaxRows)}}) {
		t.Fatalf("at list size limit: %v", r.Rows)
	}
}

func BenchmarkReadOnlyCypher(b *testing.B) {
	g := graph.New()
	entities := make([]graph.Entity, 20000)
	for i := range entities {
		entities[i] = graph.Entity{ID: fmt.Sprintf("node:%06d", i), Kind: "node", Fields: graph.Fields{"score": i}}
	}
	if err := g.ApplyCommit(graph.Commit{ID: "seed", Version: 1, Mutations: graph.Mutations{UpsertEntities: entities}}); err != nil {
		b.Fatal(err)
	}
	request := CypherRequest{Query: `MATCH (n:node) WHERE n.score=$score RETURN n.score AS score LIMIT 1`, Parameters: map[string]any{"score": 19999}}
	p, err := ParseCypher(request)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("json_dsl", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := Execute(g, Request{Op: "match", Kind: "node", Where: []Filter{{Field: "score", Op: "eq", Value: 19999}}, Project: []string{"score"}, Limit: 1})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cypher_execute", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := p.Execute(context.Background(), g, ExecuteOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("cypher_parse_execute", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			p, err := ParseCypher(request)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := p.Execute(context.Background(), g, ExecuteOptions{}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
func TestCypherCompileAndResourceBoundaries(t *testing.T) {
	for _, q := range []string{`CREATE (n) RETURN n`, `MATCH (n) SET n.x=1 RETURN n`, `MATCH (n) DELETE n`, `CALL db.labels()`, `RETURN unknown`, `RETURN unsupported(1)`, `RETURN $missing`, `MATCH ()-[*]->() RETURN 1`, `RETURN 1; RETURN 2`, `RETURN count(count(1))`, `RETURN count()`, `MATCH (n) WHERE (n)-->(m) RETURN n`} {
		t.Run(q, func(t *testing.T) {
			if _, err := ParseCypher(CypherRequest{Query: q}); err == nil {
				t.Fatal("accepted invalid or unsupported query")
			}
		})
	}
	for _, q := range []string{
		`RETURN 1 AS x UNION RETURN 2 AS x UNION ALL RETURN 3 AS x`,
		`RETURN 1 AS x UNION ALL RETURN 2 AS x UNION RETURN 3 AS x`,
		"RETURN 1 AS `a\x00b`, 2 AS c UNION RETURN 3 AS a, 4 AS `b\x00c`",
	} {
		_, err := ParseCypher(CypherRequest{Query: q})
		var detail *CypherError
		if !errors.As(err, &detail) || detail.Code != "cypher_semantic_error" {
			t.Errorf("invalid UNION %q accepted or misclassified: %v", q, err)
		}
	}
	_, err := ParseCypher(CypherRequest{Query: `MATCH (n) RETURN count(EXISTS { (n)-->() }) AS total ORDER BY count(EXISTS { (n)<--() })`})
	var invalidOrder *CypherError
	if !errors.As(err, &invalidOrder) || invalidOrder.Code != "cypher_unsupported" {
		t.Errorf("ORDER BY reused a different pattern aggregate: %v", err)
	}
	open, close := strings.Repeat("(", 33), strings.Repeat(")", 33)
	_, err = ParseCypher(CypherRequest{Query: "RETURN " + open + "1" + close})
	var nesting *CypherError
	if !errors.As(err, &nesting) || nesting.Code != "cypher_unsupported" || nesting.Line != 1 || nesting.Column != 40 {
		t.Fatalf("nesting error=%v", err)
	}
	if _, err := ParseCypher(CypherRequest{Query: "RETURN '" + open + "'"}); err != nil {
		t.Fatalf("quoted parentheses affect nesting: %v", err)
	}
	g := cypherGraph(t)
	p, err := ParseCypher(CypherRequest{Query: `UNWIND range(1,1000) AS x RETURN sum(x) AS total`, CostLimit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Execute(context.Background(), g, ExecuteOptions{}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("cost error=%v", err)
	}
	p, err = ParseCypher(CypherRequest{Query: `RETURN 1 AS x`})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Execute(context.Background(), g, ExecuteOptions{PlannerStats: PlannerStats{Version: g.Version + 1}}); !errors.Is(err, ErrIndexUnavailable) {
		t.Fatalf("mixed version options accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = p.Execute(ctx, g, ExecuteOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
}

func TestCypherAllShortestPathsKeepsEqualLengthAlternatives(t *testing.T) {
	g := cypherGraph(t)
	if err := g.ApplyCommit(graph.Commit{ID: "diamond", Version: 2, Mutations: graph.Mutations{UpsertEntities: []graph.Entity{{ID: "person:d", Kind: "person", Fields: graph.Fields{"name": "Dana"}}}, UpsertEdges: []graph.Edge{{Type: "links", From: "person:a", To: "person:c"}, {Type: "links", From: "person:c", To: "person:d"}, {Type: "links", From: "person:b", To: "person:d"}}}}); err != nil {
		t.Fatal(err)
	}
	r := runCypher(t, g, `MATCH (a:person),(d:person) WHERE graphdb.id(a)='person:a' AND graphdb.id(d)='person:d' MATCH p=allShortestPaths((a)-[:links*1..4]->(d)) RETURN [n IN nodes(p) | n.name] AS names ORDER BY names`, nil)
	if !reflect.DeepEqual(r.Rows, [][]any{{[]any{"Alice", "Bob", "Dana"}}, {[]any{"Alice", "Carol", "Dana"}}}) {
		t.Fatal(r.Rows)
	}
}
func TestCypherParameterPrecisionAndGraphValues(t *testing.T) {
	var request CypherRequest
	if err := json.Unmarshal([]byte(`{"query":"RETURN $n AS n, $n = 9007199254740993 AS equal","parameters":{"n":9007199254740993}}`), &request); err != nil {
		t.Fatal(err)
	}
	p, err := ParseCypher(request)
	if err != nil {
		t.Fatal(err)
	}
	response, err := p.Execute(context.Background(), graph.New(), ExecuteOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(response.Rows, [][]any{{int64(9007199254740993), true}}) {
		t.Fatalf("precision=%#v", response.Rows)
	}
	response = runCypher(t, cypherGraph(t), `MATCH p=(n)-[r:links]->(m) WHERE graphdb.id(n)='person:a' AND graphdb.id(m)='person:b' RETURN n,r,p`, nil)
	if len(response.Rows) != 1 {
		t.Fatal(response.Rows)
	}
	for i, kind := range []string{"node", "relationship", "path"} {
		value, ok := response.Rows[0][i].(map[string]any)
		if !ok || value["$type"] != kind {
			t.Fatalf("graph value=%#v", response.Rows[0][i])
		}
	}
}
