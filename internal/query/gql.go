package query

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseGQL compiles the legacy FIND/MATCH text syntax into a JSON query request.
func ParseGQL(input string) (Request, error) {
	if len(input) > maxTextQueryBytes {
		return Request{}, fmt.Errorf("%w: legacy text query exceeds %d bytes", ErrInvalid, maxTextQueryBytes)
	}
	tokens, err := tokenizeGQL(input)
	if err != nil {
		return Request{}, err
	}
	parser := gqlParser{tokens: tokens}
	request, err := parser.parse()
	if err != nil {
		return Request{}, err
	}
	if err := validateGQLParsedRequest(request); err != nil {
		return Request{}, err
	}
	return request, nil
}

type gqlParser struct {
	tokens      []gqlToken
	pos         int
	filterDepth int
}

func (p *gqlParser) parse() (Request, error) {
	if p.matchKeyword("EXPLAIN") {
		return p.parseWrapper("explain")
	}
	if p.matchKeyword("PROFILE") {
		return p.parseWrapper("profile")
	}
	return p.parsePrimary()
}

func (p *gqlParser) parseWrapper(op string) (Request, error) {
	target, err := p.parsePrimary()
	if err != nil {
		return Request{}, err
	}
	target.TargetOp = target.Op
	target.Op = op
	return target, nil
}

func (p *gqlParser) parsePrimary() (Request, error) {
	switch {
	case p.matchKeyword("MATCH"):
		kind, err := p.expectValueToken("start entity kind")
		if err != nil {
			return Request{}, err
		}
		request := Request{Op: "pattern", Kind: kind, Direction: "out"}
		return request, p.parseClauses(&request)
	case p.matchKeyword("FIND"):
		kind, err := p.expectValueToken("entity kind")
		if err != nil {
			return Request{}, err
		}
		request := Request{Op: "match", Kind: kind}
		return request, p.parseClauses(&request)
	case p.matchKeyword("NEIGHBORS"):
		id, err := p.expectValueToken("entity id")
		if err != nil {
			return Request{}, err
		}
		request := Request{Op: "neighbors", ID: id}
		p.parseOptionalDirection(&request)
		return request, p.parseClauses(&request)
	case p.matchKeyword("TRAVERSE"):
		id, err := p.expectValueToken("entity id")
		if err != nil {
			return Request{}, err
		}
		request := Request{Op: "traverse", ID: id}
		p.parseOptionalDirection(&request)
		return request, p.parseClauses(&request)
	case p.matchKeyword("IMPACT"):
		id, err := p.expectValueToken("entity id")
		if err != nil {
			return Request{}, err
		}
		request := Request{Op: "impact", ID: id}
		p.parseOptionalDirection(&request)
		return request, p.parseClauses(&request)
	case p.matchKeyword("SHORTEST"):
		id, err := p.expectValueToken("entity id")
		if err != nil {
			return Request{}, err
		}
		if !p.matchKeyword("TO") {
			return Request{}, fmt.Errorf("%w: SHORTEST requires TO <target_id>", ErrInvalid)
		}
		targetID, err := p.expectValueToken("target entity id")
		if err != nil {
			return Request{}, err
		}
		request := Request{Op: "shortest_path", ID: id, TargetID: targetID}
		p.parseOptionalDirection(&request)
		return request, p.parseClauses(&request)
	default:
		return Request{}, fmt.Errorf("%w: expected MATCH, FIND, NEIGHBORS, TRAVERSE, IMPACT, SHORTEST, EXPLAIN, or PROFILE", ErrInvalid)
	}
}

func validateGQLParsedRequest(request Request) error {
	if request.Op == "explain" || request.Op == "profile" {
		target, err := targetRequest(request)
		if err != nil {
			return err
		}
		return validateRequest(target)
	}
	return validateRequest(request)
}

func (p *gqlParser) parseClauses(request *Request) error {
	for !p.atEnd() {
		switch {
		case p.matchKeyword("WHERE"):
			expr, err := p.parseFilterExpr()
			if err != nil {
				return err
			}
			applyFilterExpr(&request.Where, &request.WhereExpr, expr)
		case p.matchKeyword("EDGE"):
			if !p.matchKeyword("WHERE") {
				return fmt.Errorf("%w: EDGE requires WHERE", ErrInvalid)
			}
			expr, err := p.parseFilterExpr()
			if err != nil {
				return err
			}
			applyFilterExpr(&request.EdgeWhere, &request.EdgeWhereExpr, expr)
		case p.matchKeyword("PROJECT"):
			fields, err := p.parseNameList()
			if err != nil {
				return err
			}
			request.Project = fields
		case p.matchKeyword("ORDER"):
			if !p.matchKeyword("BY") {
				return fmt.Errorf("%w: ORDER requires BY", ErrInvalid)
			}
			sortSpecs, err := p.parseSortList()
			if err != nil {
				return err
			}
			request.Sort = sortSpecs
		case p.matchKeyword("SORT"):
			_ = p.matchKeyword("BY")
			sortSpecs, err := p.parseSortList()
			if err != nil {
				return err
			}
			request.Sort = sortSpecs
		case p.matchKeyword("AGG"), p.matchKeyword("AGGREGATE"):
			aggregates, err := p.parseAggregates()
			if err != nil {
				return err
			}
			request.Aggregate = aggregates
		case p.matchKeyword("GROUP"):
			if !p.matchKeyword("BY") {
				return fmt.Errorf("%w: GROUP requires BY", ErrInvalid)
			}
			fields, err := p.parseNameList()
			if err != nil {
				return err
			}
			request.GroupBy = fields
		case p.matchKeyword("HAVING"):
			expr, err := p.parseFilterExpr()
			if err != nil {
				return err
			}
			applyFilterExpr(&request.Having, &request.HavingExpr, expr)
		case p.matchKeyword("LIMIT"):
			limit, err := p.expectInt("limit")
			if err != nil {
				return err
			}
			request.Limit = limit
		case p.matchKeyword("REL"), p.matchKeyword("RELATION"), p.matchKeyword("RELATIONS"), p.matchKeyword("RELATION_TYPES"):
			values, err := p.parseNameList()
			if err != nil {
				return err
			}
			request.RelationTypes = values
		case p.matchKeyword("DEPTH"):
			depth, err := p.expectInt("depth")
			if err != nil {
				return err
			}
			request.Depth = depth
		case p.matchKeyword("PATH"):
			if err := p.parsePathClause(request); err != nil {
				return err
			}
		case p.matchKeyword("END"):
			if err := p.parseEndClause(request); err != nil {
				return err
			}
		case p.parseOptionalDirection(request):
		default:
			return fmt.Errorf("%w: unexpected token %q", ErrInvalid, p.peek().value)
		}
	}
	return nil
}

func (p *gqlParser) parseOptionalDirection(request *Request) bool {
	switch {
	case p.matchKeyword("OUT"):
		request.Direction = "out"
		return true
	case p.matchKeyword("IN"):
		request.Direction = "in"
		return true
	case p.matchKeyword("BOTH"):
		request.Direction = "both"
		return true
	default:
		return false
	}
}

func (p *gqlParser) parsePathClause(request *Request) error {
	for !p.atEnd() {
		switch {
		case p.matchKeyword("STEP"):
			step, err := p.parsePathStep()
			if err != nil {
				return err
			}
			request.Path.Steps = append(request.Path.Steps, step)
		case p.matchKeyword("NODES"):
			values, err := p.parseNameList()
			if err != nil {
				return err
			}
			request.Path.NodeKinds = values
		case p.matchKeyword("REL"), p.matchKeyword("RELATIONS"), p.matchKeyword("RELATION_TYPES"):
			values, err := p.parseNameList()
			if err != nil {
				return err
			}
			request.Path.RelationTypes = values
		default:
			return nil
		}
	}
	return nil
}

func (p *gqlParser) parsePathStep() (PathStep, error) {
	step := PathStep{}
	switch {
	case p.matchKeyword("OUT"):
		step.Direction = "out"
	case p.matchKeyword("IN"):
		step.Direction = "in"
	case p.matchKeyword("BOTH"):
		step.Direction = "both"
	}
	for !p.atEnd() && !p.peekKeyword("STEP") && !p.peekPathStepBoundary() {
		switch {
		case p.matchKeyword("REL"), p.matchKeyword("RELATIONS"), p.matchKeyword("RELATION_TYPES"):
			values, err := p.parseNameList()
			if err != nil {
				return PathStep{}, err
			}
			step.RelationTypes = values
		case p.matchKeyword("NODE"), p.matchKeyword("NODES"):
			values, err := p.parseNameList()
			if err != nil {
				return PathStep{}, err
			}
			step.NodeKinds = values
		case p.matchKeyword("WHERE"):
			expr, err := p.parseFilterExpr()
			if err != nil {
				return PathStep{}, err
			}
			applyFilterExpr(&step.Where, &step.WhereExpr, expr)
		case p.matchKeyword("EDGE"):
			if !p.matchKeyword("WHERE") {
				return PathStep{}, fmt.Errorf("%w: path step EDGE requires WHERE", ErrInvalid)
			}
			expr, err := p.parseFilterExpr()
			if err != nil {
				return PathStep{}, err
			}
			applyFilterExpr(&step.EdgeWhere, &step.EdgeWhereExpr, expr)
		default:
			return PathStep{}, fmt.Errorf("%w: unexpected path step token %q", ErrInvalid, p.peek().value)
		}
	}
	return step, nil
}

func (p *gqlParser) parseEndClause(request *Request) error {
	switch {
	case p.matchKeyword("KIND"):
		kind, err := p.expectValueToken("end kind")
		if err != nil {
			return err
		}
		request.Path.EndKind = kind
		return nil
	case p.matchKeyword("WHERE"):
		expr, err := p.parseFilterExpr()
		if err != nil {
			return err
		}
		applyFilterExpr(&request.Path.EndWhere, &request.Path.EndWhereExpr, expr)
		return nil
	default:
		return fmt.Errorf("%w: END requires KIND or WHERE", ErrInvalid)
	}
}

func (p *gqlParser) parseFilterExpr() (*FilterExpr, error) {
	return p.parseOrExpr()
}

func (p *gqlParser) parseOrExpr() (*FilterExpr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("OR") {
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = combineFilterExpr("or", left, right)
	}
	return left, nil
}

func (p *gqlParser) parseAndExpr() (*FilterExpr, error) {
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}
	for p.matchKeyword("AND") {
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = combineFilterExpr("and", left, right)
	}
	return left, nil
}

func (p *gqlParser) parseNotExpr() (*FilterExpr, error) {
	if p.matchKeyword("NOT") {
		if err := p.enterFilterNesting(); err != nil {
			return nil, err
		}
		defer p.leaveFilterNesting()
		child, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		return &FilterExpr{Op: "not", Children: []FilterExpr{*child}}, nil
	}
	return p.parsePrimaryExpr()
}

func (p *gqlParser) parsePrimaryExpr() (*FilterExpr, error) {
	if p.matchSymbol("(") {
		if err := p.enterFilterNesting(); err != nil {
			return nil, err
		}
		defer p.leaveFilterNesting()
		expr, err := p.parseFilterExpr()
		if err != nil {
			return nil, err
		}
		if !p.matchSymbol(")") {
			return nil, fmt.Errorf("%w: missing closing parenthesis", ErrInvalid)
		}
		return expr, nil
	}
	return p.parseConditionExpr()
}

func (p *gqlParser) enterFilterNesting() error {
	if p.filterDepth >= maxFilterExpressionDepth {
		return fmt.Errorf("%w: filter expression depth exceeds %d", ErrInvalid, maxFilterExpressionDepth)
	}
	p.filterDepth++
	return nil
}

func (p *gqlParser) leaveFilterNesting() {
	p.filterDepth--
}

func (p *gqlParser) parseConditionExpr() (*FilterExpr, error) {
	if p.matchKeyword("EXISTS") {
		field, err := p.expectValueToken("field")
		return &FilterExpr{Field: field, Op: "exists", Value: true}, err
	}
	field, err := p.expectValueToken("field")
	if err != nil {
		return nil, err
	}
	if p.matchKeyword("EXISTS") {
		return &FilterExpr{Field: field, Op: "exists", Value: true}, nil
	}
	op, err := p.parseFilterOp()
	if err != nil {
		return nil, err
	}
	value, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	return &FilterExpr{Field: field, Op: op, Value: value}, nil
}

func combineFilterExpr(op string, left *FilterExpr, right *FilterExpr) *FilterExpr {
	children := []FilterExpr{}
	if left != nil && left.Op == op && left.Field == "" {
		children = append(children, left.Children...)
	} else if left != nil {
		children = append(children, *left)
	}
	if right != nil && right.Op == op && right.Field == "" {
		children = append(children, right.Children...)
	} else if right != nil {
		children = append(children, *right)
	}
	return &FilterExpr{Op: op, Children: children}
}

func applyFilterExpr(filters *[]Filter, expr **FilterExpr, next *FilterExpr) {
	if next == nil {
		return
	}
	if flat, ok := flattenAndFilters(next); ok {
		*filters = append(*filters, flat...)
		return
	}
	if *expr == nil {
		*expr = next
		return
	}
	*expr = combineFilterExpr("and", *expr, next)
}

func flattenAndFilters(expr *FilterExpr) ([]Filter, bool) {
	if expr == nil {
		return nil, true
	}
	op := expr.Op
	if op == "" && expr.Field != "" {
		op = "eq"
	}
	if op == "and" {
		out := []Filter{}
		for i := range expr.Children {
			child, ok := flattenAndFilters(&expr.Children[i])
			if !ok {
				return nil, false
			}
			out = append(out, child...)
		}
		return out, true
	}
	if expr.Field == "" || op == "or" || op == "not" {
		return nil, false
	}
	return []Filter{{Field: expr.Field, Op: op, Value: expr.Value}}, true
}

func (p *gqlParser) parseFilterOp() (string, error) {
	token := p.next()
	if token.kind == gqlSymbol {
		switch token.value {
		case "=":
			return "eq", nil
		case "!=":
			return "neq", nil
		case ">":
			return "gt", nil
		case ">=":
			return "gte", nil
		case "<":
			return "lt", nil
		case "<=":
			return "lte", nil
		}
	}
	switch strings.ToUpper(token.value) {
	case "EQ":
		return "eq", nil
	case "NEQ":
		return "neq", nil
	case "GT":
		return "gt", nil
	case "GTE":
		return "gte", nil
	case "LT":
		return "lt", nil
	case "LTE":
		return "lte", nil
	case "IN":
		return "in", nil
	case "PREFIX":
		return "prefix", nil
	case "CONTAINS":
		return "contains", nil
	case "FUZZY":
		return "fuzzy", nil
	default:
		return "", fmt.Errorf("%w: unsupported legacy text filter operator %q", ErrInvalid, token.value)
	}
}

func (p *gqlParser) parseNameList() ([]string, error) {
	values := []string{}
	for {
		value, err := p.expectValueToken("name")
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		if !p.matchSymbol(",") {
			return values, nil
		}
	}
}

func (p *gqlParser) parseSortList() ([]SortSpec, error) {
	specs := []SortSpec{}
	for {
		field, err := p.expectValueToken("sort field")
		if err != nil {
			return nil, err
		}
		spec := SortSpec{Field: field}
		if p.matchKeyword("DESC") {
			spec.Desc = true
		} else {
			_ = p.matchKeyword("ASC")
		}
		specs = append(specs, spec)
		if !p.matchSymbol(",") {
			return specs, nil
		}
	}
}

func (p *gqlParser) parseAggregates() ([]Aggregation, error) {
	aggregates := []Aggregation{}
	for {
		aggregation, err := p.parseAggregate()
		if err != nil {
			return nil, err
		}
		aggregates = append(aggregates, aggregation)
		if !p.matchSymbol(",") {
			return aggregates, nil
		}
	}
}

func (p *gqlParser) parseAggregate() (Aggregation, error) {
	op, err := p.expectValueToken("aggregate op")
	if err != nil {
		return Aggregation{}, err
	}
	op = strings.ToLower(op)
	if !p.matchSymbol("(") {
		return Aggregation{}, fmt.Errorf("%w: aggregate %q requires parentheses", ErrInvalid, op)
	}
	field := ""
	if !p.matchSymbol(")") {
		field, err = p.expectValueToken("aggregate field")
		if err != nil {
			return Aggregation{}, err
		}
		if !p.matchSymbol(")") {
			return Aggregation{}, fmt.Errorf("%w: aggregate %q missing closing parenthesis", ErrInvalid, op)
		}
	}
	aggregation := Aggregation{Op: op, Field: field}
	if p.matchKeyword("AS") {
		aggregation.Name, err = p.expectValueToken("aggregate alias")
	}
	return aggregation, err
}

func (p *gqlParser) parseValue() (any, error) {
	token := p.next()
	switch token.kind {
	case gqlString:
		return token.value, nil
	case gqlNumber:
		if strings.ContainsAny(token.value, ".eE") {
			return strconv.ParseFloat(token.value, 64)
		}
		value, err := strconv.ParseInt(token.value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid number %q", ErrInvalid, token.value)
		}
		return value, nil
	case gqlIdent:
		switch strings.ToLower(token.value) {
		case "true":
			return true, nil
		case "false":
			return false, nil
		case "null":
			return nil, nil
		default:
			return token.value, nil
		}
	case gqlSymbol:
		if token.value == "[" {
			return p.parseArray()
		}
	}
	return nil, fmt.Errorf("%w: expected value, got %q", ErrInvalid, token.value)
}

func (p *gqlParser) parseArray() ([]any, error) {
	values := []any{}
	if p.matchSymbol("]") {
		return values, nil
	}
	for {
		value, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
		if p.matchSymbol("]") {
			return values, nil
		}
		if !p.matchSymbol(",") {
			return nil, fmt.Errorf("%w: array values must be comma separated", ErrInvalid)
		}
	}
}

func (p *gqlParser) expectInt(name string) (int, error) {
	token := p.next()
	value, err := strconv.Atoi(token.value)
	if err != nil {
		return 0, fmt.Errorf("%w: invalid %s", ErrInvalid, name)
	}
	return value, nil
}

func (p *gqlParser) expectValueToken(name string) (string, error) {
	token := p.next()
	switch token.kind {
	case gqlIdent, gqlString, gqlNumber:
		return token.value, nil
	default:
		return "", fmt.Errorf("%w: expected %s", ErrInvalid, name)
	}
}

func (p *gqlParser) matchKeyword(value string) bool {
	if !strings.EqualFold(p.peek().value, value) {
		return false
	}
	p.pos++
	return true
}

func (p *gqlParser) peekKeyword(value string) bool {
	return strings.EqualFold(p.peek().value, value)
}

func (p *gqlParser) peekTopLevelClause() bool {
	switch strings.ToUpper(p.peek().value) {
	case "WHERE", "EDGE", "PROJECT", "ORDER", "SORT", "AGG", "AGGREGATE", "GROUP", "HAVING", "LIMIT", "DEPTH", "PATH", "END", "OUT", "IN", "BOTH":
		return true
	default:
		return false
	}
}

func (p *gqlParser) peekPathStepBoundary() bool {
	switch strings.ToUpper(p.peek().value) {
	case "PROJECT", "ORDER", "SORT", "AGG", "AGGREGATE", "GROUP", "HAVING", "LIMIT", "DEPTH", "PATH", "END", "OUT", "IN", "BOTH":
		return true
	default:
		return false
	}
}

func (p *gqlParser) matchSymbol(value string) bool {
	if p.peek().kind != gqlSymbol || p.peek().value != value {
		return false
	}
	p.pos++
	return true
}

func (p *gqlParser) peek() gqlToken {
	if p.pos >= len(p.tokens) {
		return gqlToken{kind: gqlEOF}
	}
	return p.tokens[p.pos]
}

func (p *gqlParser) next() gqlToken {
	token := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return token
}

func (p *gqlParser) atEnd() bool {
	return p.peek().kind == gqlEOF
}
