package query

// ValidatedQuery is the normalized, validated intermediate representation
// shared across JSON Query, GQL, and GraphQL frontends before planning and
// physical execution.
type ValidatedQuery struct {
	Original    Request
	Normalized  Request
	WrapperOp   string // "", "explain", or "profile"
	EffectiveOp string
}

// CompileRequest validates and normalizes a query Request from any frontend
// (JSON, GQL, or GraphQL) into a canonical ValidatedQuery IR.
func CompileRequest(request Request) (ValidatedQuery, error) {
	wrapper := ""
	target := request
	if request.Op == "explain" || request.Op == "profile" {
		wrapper = request.Op
		unwrapped, err := targetRequest(request)
		if err != nil {
			return ValidatedQuery{}, err
		}
		if wrapper == "profile" {
			unwrapped.Profile = true
		}
		target = unwrapped
	}
	if err := validateRequest(target); err != nil {
		return ValidatedQuery{}, err
	}
	normalized := normalizeCompiledRequest(target)
	return ValidatedQuery{
		Original:    request,
		Normalized:  normalized,
		WrapperOp:   wrapper,
		EffectiveOp: normalized.Op,
	}, nil
}

func normalizeCompiledRequest(request Request) Request {
	switch request.Op {
	case "impact":
		request.DirectionStrategy = "impact"
		if request.Depth <= 0 {
			request.Depth = 4
		}
	case "pattern":
		request.Depth = len(request.Path.Steps)
		if request.Direction == "" {
			request.Direction = "out"
		}
	case "traverse", "shortest_path":
		if len(request.Path.Steps) > 0 {
			depth := normalizedDepth(request.Depth)
			if depth > len(request.Path.Steps) {
				request.Depth = len(request.Path.Steps)
			}
		}
	}
	return request
}
