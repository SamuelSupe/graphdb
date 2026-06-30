package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type gqlTokenKind int

const (
	gqlEOF gqlTokenKind = iota
	gqlIdent
	gqlString
	gqlNumber
	gqlSymbol
)

type gqlToken struct {
	kind  gqlTokenKind
	value string
}

func tokenizeGQL(input string) ([]gqlToken, error) {
	tokens := []gqlToken{}
	for i := 0; i < len(input); {
		r := rune(input[i])
		if unicode.IsSpace(r) {
			i++
			continue
		}
		if input[i] == '"' || input[i] == '\'' {
			value, next, err := readGQLString(input, i)
			if err != nil {
				return nil, err
			}
			tokens = append(tokens, gqlToken{kind: gqlString, value: value})
			i = next
			continue
		}
		if i+1 < len(input) && isGQLOperator(input[i:i+2]) {
			tokens = append(tokens, gqlToken{kind: gqlSymbol, value: input[i : i+2]})
			i += 2
			continue
		}
		if strings.ContainsRune("=><(),[]", r) {
			tokens = append(tokens, gqlToken{kind: gqlSymbol, value: input[i : i+1]})
			i++
			continue
		}
		start := i
		for i < len(input) && isGQLBareChar(rune(input[i])) {
			i++
		}
		if start == i {
			return nil, fmt.Errorf("%w: unexpected character %q", ErrInvalid, input[i])
		}
		value := input[start:i]
		kind := gqlIdent
		if isGQLNumber(value) {
			kind = gqlNumber
		}
		tokens = append(tokens, gqlToken{kind: kind, value: value})
	}
	return append(tokens, gqlToken{kind: gqlEOF}), nil
}

func readGQLString(input string, start int) (string, int, error) {
	quote := input[start]
	i := start + 1
	escaped := false
	for i < len(input) {
		ch := input[i]
		if escaped {
			escaped = false
			i++
			continue
		}
		if ch == '\\' {
			escaped = true
			i++
			continue
		}
		if ch == quote {
			raw := input[start : i+1]
			if quote == '\'' {
				raw = `"` + strings.ReplaceAll(strings.Trim(raw, "'"), `"`, `\"`) + `"`
			}
			value, err := strconv.Unquote(raw)
			if err != nil {
				return "", 0, fmt.Errorf("%w: invalid string literal", ErrInvalid)
			}
			return value, i + 1, nil
		}
		i++
	}
	return "", 0, fmt.Errorf("%w: unterminated string literal", ErrInvalid)
}

func isGQLOperator(value string) bool {
	switch value {
	case "!=", ">=", "<=":
		return true
	default:
		return false
	}
}

func isGQLBareChar(r rune) bool {
	if unicode.IsSpace(r) || strings.ContainsRune("=><!(),[]\"'", r) {
		return false
	}
	return true
}

func isGQLNumber(value string) bool {
	if value == "" {
		return false
	}
	_, err := strconv.ParseFloat(value, 64)
	return err == nil && (value[0] == '-' || value[0] == '+' || value[0] >= '0' && value[0] <= '9')
}
