package query

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
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
		r, size := utf8.DecodeRuneInString(input[i:])
		if unicode.IsSpace(r) {
			i += size
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
			tokens = append(tokens, gqlToken{kind: gqlSymbol, value: input[i : i+size]})
			i += size
			continue
		}
		start := i
		for i < len(input) {
			cr, csize := utf8.DecodeRuneInString(input[i:])
			if !isGQLBareChar(cr) {
				break
			}
			i += csize
		}
		if start == i {
			return nil, fmt.Errorf("%w: unexpected character %q", ErrInvalid, r)
		}
		value := input[start:i]
		kind := gqlIdent
		if isNum, rangeErr := classifyGQLNumber(value); rangeErr != nil {
			return nil, rangeErr
		} else if isNum {
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
				raw = singleQuotedToDoubleQuoted(raw[1 : len(raw)-1])
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

func singleQuotedToDoubleQuoted(inner string) string {
	var b strings.Builder
	b.Grow(len(inner) + 2)
	b.WriteByte('"')
	for i := 0; i < len(inner); i++ {
		ch := inner[i]
		if ch == '\\' && i+1 < len(inner) {
			next := inner[i+1]
			if next == '\'' {
				b.WriteByte('\'')
				i++
				continue
			}
			b.WriteByte('\\')
			b.WriteByte(next)
			i++
			continue
		}
		if ch == '"' {
			b.WriteString(`\"`)
			continue
		}
		b.WriteByte(ch)
	}
	b.WriteByte('"')
	return b.String()
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

func classifyGQLNumber(value string) (bool, error) {
	if value == "" {
		return false, nil
	}
	if !(value[0] == '-' || value[0] == '+' || (value[0] >= '0' && value[0] <= '9')) {
		return false, nil
	}
	_, err := strconv.ParseFloat(value, 64)
	if err == nil {
		return true, nil
	}
	if numErr, ok := err.(*strconv.NumError); ok && numErr.Err == strconv.ErrRange {
		return false, fmt.Errorf("%w: numeric literal %q out of range", ErrInvalid, value)
	}
	return false, nil
}
