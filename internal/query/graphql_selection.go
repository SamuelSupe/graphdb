package query

import (
	"fmt"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const maxGraphQLSelections = 256

type graphQLSelectionWalk struct {
	visited int
}

func collectGraphQLRoots(
	document *ast.QueryDocument,
	selections ast.SelectionSet,
	variables map[string]any,
) ([]*ast.Field, []string, error) {
	var roots []*ast.Field
	var typenames []string
	err := walkGraphQLSelections(document, selections, variables, func(field *ast.Field) error {
		switch field.Name {
		case "graph", "evidenceSearch":
			roots = append(roots, field)
		case "__typename":
			typenames = append(typenames, responseName(field))
		default:
			return gqlerror.ErrorPosf(field.Position, "unsupported GraphQL root field %q", field.Name)
		}
		return nil
	})
	return roots, typenames, err
}

func collectGraphQLResultFields(
	document *ast.QueryDocument,
	selections ast.SelectionSet,
	variables map[string]any,
) ([]graphQLResultField, error) {
	fields := make([]graphQLResultField, 0, len(selections))
	seen := map[string]struct{}{}
	err := walkGraphQLSelections(document, selections, variables, func(field *ast.Field) error {
		name := responseName(field)
		if _, ok := seen[name]; ok {
			return nil
		}
		seen[name] = struct{}{}
		fields = append(fields, graphQLResultField{Name: field.Name, ResponseName: name})
		return nil
	})
	return fields, err
}

func walkGraphQLSelections(
	document *ast.QueryDocument,
	selections ast.SelectionSet,
	variables map[string]any,
	visit func(*ast.Field) error,
) error {
	walk := graphQLSelectionWalk{}
	return walk.walk(document, selections, variables, 1, visit)
}

func (w *graphQLSelectionWalk) walk(
	document *ast.QueryDocument,
	selections ast.SelectionSet,
	variables map[string]any,
	depth int,
	visit func(*ast.Field) error,
) error {
	if depth > maxFilterExpressionDepth {
		return fmt.Errorf("GraphQL selection depth exceeds %d", maxFilterExpressionDepth)
	}
	for _, selection := range selections {
		w.visited++
		if w.visited > maxGraphQLSelections {
			return fmt.Errorf("GraphQL document expands to more than %d selections", maxGraphQLSelections)
		}
		switch item := selection.(type) {
		case *ast.Field:
			include, err := includeGraphQLSelection(item.Directives, variables)
			if err != nil {
				return err
			}
			if include {
				if err := visit(item); err != nil {
					return err
				}
			}
		case *ast.FragmentSpread:
			include, err := includeGraphQLSelection(item.Directives, variables)
			if err != nil {
				return err
			}
			if !include {
				continue
			}
			fragment := document.Fragments.ForName(item.Name)
			if fragment == nil {
				return fmt.Errorf("unknown GraphQL fragment %q", item.Name)
			}
			if err := w.walk(document, fragment.SelectionSet, variables, depth+1, visit); err != nil {
				return err
			}
		case *ast.InlineFragment:
			include, err := includeGraphQLSelection(item.Directives, variables)
			if err != nil {
				return err
			}
			if include {
				if err := w.walk(document, item.SelectionSet, variables, depth+1, visit); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unsupported GraphQL selection %T", selection)
		}
	}
	return nil
}

func includeGraphQLSelection(directives ast.DirectiveList, variables map[string]any) (bool, error) {
	if skip := directives.ForName("skip"); skip != nil {
		value, ok := skip.ArgumentMap(variables)["if"].(bool)
		if !ok {
			return false, gqlerror.ErrorPosf(skip.Position, "@skip(if:) must be a Boolean")
		}
		if value {
			return false, nil
		}
	}
	if include := directives.ForName("include"); include != nil {
		value, ok := include.ArgumentMap(variables)["if"].(bool)
		if !ok {
			return false, gqlerror.ErrorPosf(include.Position, "@include(if:) must be a Boolean")
		}
		if !value {
			return false, nil
		}
	}
	return true, nil
}

func responseName(field *ast.Field) string {
	if field.Alias != "" {
		return field.Alias
	}
	return field.Name
}

func graphQLError(position *ast.Position, format string, args ...any) gqlerror.List {
	return gqlerror.List{gqlerror.ErrorPosf(position, format, args...)}
}

func graphQLErrorFrom(err error) gqlerror.List {
	if err == nil {
		return nil
	}
	if list, ok := err.(gqlerror.List); ok {
		return list
	}
	if item, ok := err.(*gqlerror.Error); ok {
		return gqlerror.List{item}
	}
	return gqlerror.List{gqlerror.Wrap(err)}
}
