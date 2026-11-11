package query

import (
	"slices"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
	"github.com/SamuelSupe/graphdb/v2/internal/query/cypherparser"
	"github.com/antlr4-go/antlr/v4"
)

// cySyntax removes whitespace from the generated parse tree while retaining
// rule boundaries and source positions. Language syntax stays in Cypher.g4.
type cySyntax struct {
	rule     string
	source   antlr.ParseTree
	pos      cyPos
	children []cySyntax
}

// GetText recursively joins the source tokens, so only read it for rules
// whose text the compiler needs, such as an unaliased result column.
func (n *cySyntax) text() string { return n.source.GetText() }

func (n *cySyntax) one(rule string) *cySyntax {
	if n != nil {
		for i := range n.children {
			if n.children[i].rule == rule {
				return &n.children[i]
			}
		}
	}
	return nil
}
func (n *cySyntax) many(rule string) []*cySyntax {
	var out []*cySyntax
	if n != nil {
		for i := range n.children {
			if n.children[i].rule == rule {
				out = append(out, &n.children[i])
			}
		}
	}
	return out
}
func (n *cySyntax) token(t string) bool {
	if n != nil {
		for i := range n.children {
			c := &n.children[i]
			if c.rule == "" && strings.EqualFold(c.text(), t) {
				return true
			}
		}
	}
	return false
}
func (n *cySyntax) name() string {
	if n == nil {
		return ""
	}
	s := n.text()
	if strings.HasPrefix(s, "`") {
		return strings.ReplaceAll(s[1:len(s)-1], "``", "`")
	}
	return s
}

type cyErrorListener struct {
	*antlr.DefaultErrorListener
	err error
}

func (l *cyErrorListener) SyntaxError(_ antlr.Recognizer, _ interface{}, line, column int, msg string, _ antlr.RecognitionException) {
	if l.err == nil {
		l.err = cyPos{line, column + 1}.fail("cypher_syntax_error", msg)
	}
}
func cyTree(t antlr.Tree, ruleNames []string, out *cySyntax) bool {
	switch n := t.(type) {
	case antlr.TerminalNode:
		tok := n.GetSymbol()
		if tok.GetTokenType() == cypherparser.CypherLexerSP || tok.GetTokenType() == antlr.TokenEOF {
			return false
		}
		*out = cySyntax{source: n, pos: cyPos{tok.GetLine(), tok.GetColumn() + 1}}
		return true
	case antlr.ParserRuleContext:
		start := n.GetStart()
		children := n.GetChildren()
		*out = cySyntax{
			rule:     strings.TrimPrefix(ruleNames[n.GetRuleIndex()], "oC_"),
			source:   n,
			pos:      cyPos{start.GetLine(), start.GetColumn() + 1},
			children: make([]cySyntax, len(children)),
		}
		count := 0
		for _, child := range children {
			if cyTree(child, ruleNames, &out.children[count]) {
				count++
			}
		}
		out.children = out.children[:count]
		return true
	}
	return false
}

type cySymbol struct {
	slot int
	kind string
}
type cyCompiler struct {
	plan  *CypherPlan
	scope map[string]cySymbol
	exprs int
}

// ParseCypher validates one read-only statement, binds parameters and resolves
// variable scopes before any graph reads. Unsupported syntax is rejected.
func ParseCypher(request CypherRequest) (*CypherPlan, error) {
	if len(request.Query) == 0 || len(request.Query) > maxTextQueryBytes {
		return nil, cyPos{1, 1}.fail("cypher_syntax_error", "query must contain 1 to 65536 bytes")
	}
	if err := ValidateRequest(Request{Op: "match", TimeoutMS: request.TimeoutMS, CostLimit: request.CostLimit, MinVersion: request.MinVersion}); err != nil {
		return nil, err
	}
	parameters, err := cyNormalize(request.Parameters, 0)
	if err != nil {
		return nil, err
	}
	if parameters != nil {
		request.Parameters = parameters.(map[string]any)
	}
	l := &cyErrorListener{DefaultErrorListener: antlr.NewDefaultErrorListener()}
	lexer := cypherparser.NewCypherLexer(antlr.NewInputStream(request.Query))
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(l)
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	tokens.Fill()
	if l.err != nil {
		return nil, l.err
	}
	all := tokens.GetAllTokens()
	if len(all) > 8192 {
		return nil, cyPos{1, 1}.fail("cypher_unsupported", "query exceeds 8192 tokens")
	}
	depth := 0
	for _, t := range all {
		if t.GetStart() != t.GetStop() {
			continue
		}
		switch t.GetText() {
		case "(", "[", "{":
			depth++
		case ")", "]", "}":
			depth--
		}
		if depth > 32 {
			return nil, cyPos{t.GetLine(), t.GetColumn() + 1}.fail("cypher_unsupported", "nesting exceeds 32")
		}
	}
	p := &CypherPlan{request: request, profile: request.Profile, definitions: map[int]*cyExpr{}}
	for i, t := range all {
		if t.GetTokenType() == cypherparser.CypherLexerSP {
			continue
		}
		text := t.GetText()
		if strings.EqualFold(text, "EXPLAIN") || strings.EqualFold(text, "PROFILE") {
			p.explain = strings.EqualFold(text, "EXPLAIN")
			p.profile = p.profile || !p.explain
			tokens.Seek(i + 1)
		}
		break
	}
	parser := cypherparser.NewCypherParser(tokens)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(l)
	root := parser.OC_Cypher()
	if l.err != nil {
		return nil, l.err
	}
	var tree cySyntax
	cyTree(root, parser.RuleNames, &tree)
	// Check every subtree, including existential expressions, before any reads.
	var readonly func(*cySyntax) error
	readonly = func(n *cySyntax) error {
		switch n.rule {
		case "UpdatingClause":
			return n.pos.fail("cypher_read_only", "updating clauses are not allowed")
		case "StandaloneCall", "InQueryCall":
			return n.pos.fail("cypher_unsupported", "procedure calls are not supported")
		}
		for i := range n.children {
			if err := readonly(&n.children[i]); err != nil {
				return err
			}
		}
		return nil
	}
	if err := readonly(&tree); err != nil {
		return nil, err
	}
	regular := tree.one("Statement").one("Query").one("RegularQuery")
	if regular == nil {
		return nil, cyPos{1, 1}.fail("cypher_unsupported", "a regular read query is required")
	}
	c := &cyCompiler{plan: p}
	single := regular.one("SingleQuery")
	b, err := c.branch(single)
	if err != nil {
		return nil, err
	}
	p.branches = append(p.branches, b)
	for _, u := range regular.many("Union") {
		b, err = c.branch(u.one("SingleQuery"))
		if err != nil {
			return nil, err
		}
		b.unionAll = u.token("ALL")
		if len(p.branches) > 1 && b.unionAll != p.branches[1].unionAll {
			return nil, u.pos.fail("cypher_semantic_error", "UNION and UNION ALL cannot be mixed")
		}
		if !slices.Equal(b.columns, p.branches[0].columns) {
			return nil, u.pos.fail("cypher_semantic_error", "UNION branches must return the same column names")
		}
		p.branches = append(p.branches, b)
	}
	if len(p.branches) > 32 {
		return nil, cyPos{1, 1}.fail("cypher_unsupported", "too many UNION branches")
	}
	fields := map[string]bool{graph.ReservedLabelsField: true}
	visitedDefinitions := map[int]bool{}
	if p.slots > 1024 {
		return nil, cyPos{1, 1}.fail("cypher_unsupported", "query exceeds 1024 variable slots")
	}
	var visit func(*cyExpr)
	visit = func(e *cyExpr) {
		if e == nil {
			return
		}
		if e.op == "property" && e.args[0].op == "var" {
			fields[e.name] = true
			return
		}
		if e.op == "var" {
			if definition := p.definitions[e.slot]; definition != nil {
				if visitedDefinitions[e.slot] {
					return
				}
				visitedDefinitions[e.slot] = true
				visit(definition)
				return
			}
			p.allFields = true
		}
		if len(e.patterns) > 0 {
			p.allFields = true
		}
		if e.op == "call" && (e.name == "labels" || e.name == "type" || e.name == "graphdb.id") && len(e.args) == 1 && e.args[0].op == "var" {
			return
		}
		for _, a := range e.args {
			visit(a)
		}
	}
	for _, b := range p.branches {
		for _, cl := range b.clauses {
			visit(cl.where)
			visit(cl.expr)
			for _, it := range cl.items {
				visit(it.expr)
			}
			for _, s := range cl.sort {
				visit(s.expr)
			}
			for _, pat := range cl.patterns {
				for _, n := range pat.nodes {
					visit(n.props)
					if n.props != nil {
						for _, key := range n.props.keys {
							fields[key] = true
						}
					}
				}
				for _, r := range pat.rels {
					visit(r.props)
				}
			}
		}
	}
	for f := range fields {
		p.fields = append(p.fields, f)
	}
	sort.Strings(p.fields)
	return p, nil
}
