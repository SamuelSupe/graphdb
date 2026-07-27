package query

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type cursorState struct {
	Version int64  `json:"version"`
	After   string `json:"after,omitempty"`
	Offset  int    `json:"offset,omitempty"`
	Query   string `json:"query,omitempty"`
	Order   string `json:"order,omitempty"`
	Legacy  bool   `json:"-"`
}

func parseCursor(raw string) (cursorState, error) {
	if raw == "" {
		return cursorState{}, nil
	}
	if offset, err := strconv.Atoi(raw); err == nil && offset >= 0 {
		return cursorState{Offset: offset, Legacy: true}, nil
	}
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return cursorState{}, fmt.Errorf("%w: invalid cursor", ErrInvalid)
	}
	var cursor cursorState
	if err := json.Unmarshal(data, &cursor); err != nil {
		return cursorState{}, fmt.Errorf("%w: invalid cursor", ErrInvalid)
	}
	if cursor.Offset < 0 {
		return cursorState{}, fmt.Errorf("%w: invalid cursor offset", ErrInvalid)
	}
	if cursor.After != "" && cursor.Version <= 0 {
		return cursorState{}, fmt.Errorf("%w: cursor version is required", ErrInvalid)
	}
	if cursor.Order != "" {
		if cursor.After == "" || !validCursorOrder(cursor.Order) {
			return cursorState{}, fmt.Errorf("%w: invalid cursor order", ErrInvalid)
		}
	}
	return cursor, nil
}

func validateCursor(cursor cursorState, version int64, request Request) error {
	if cursor.Version != 0 && cursor.Version != version {
		return fmt.Errorf("%w: cursor version %d does not match graph version %d", ErrInvalid, cursor.Version, version)
	}
	if cursor.Query != "" && cursor.Query != cursorQueryHash(request) {
		return fmt.Errorf("%w: cursor query does not match request", ErrInvalid)
	}
	if cursor.Order != "" && request.Op != "match" {
		return fmt.Errorf("%w: cursor order does not match request", ErrInvalid)
	}
	return nil
}

func validCursorOrder(order string) bool {
	return order == EntityPageOrderIdentity || order == EntityPageOrderShard
}

func paginate(version int64, results []Result, request Request, cursor cursorState) ([]Result, string, error) {
	limit := normalizedLimit(request.Limit)
	start := cursor.Offset
	if !cursor.Legacy && cursor.After != "" {
		start = len(results)
		found := false
		if cursor.Offset > 0 && cursor.Offset <= len(results) &&
			resultMatchesCursor(results[cursor.Offset-1], cursor.After) {
			start = cursor.Offset
			found = true
		}
		if !found {
			for i, result := range results {
				if resultMatchesCursor(result, cursor.After) {
					start = i + 1
					found = true
					break
				}
			}
		}
		if !found {
			return nil, "", invalidCursorAfter(cursor)
		}
	}
	if start > len(results) {
		start = len(results)
	}
	end := start + limit
	if end > len(results) {
		end = len(results)
	}
	page := append([]Result(nil), results[start:end]...)
	if end >= len(results) || len(page) == 0 {
		return page, "", nil
	}
	return page, encodeCursor(cursorState{
		Version: version,
		After:   resultIdentity(page[len(page)-1]),
		Offset:  end,
		Query:   cursorQueryHash(request),
		Order:   cursor.Order,
	}), nil
}

func encodeCursor(cursor cursorState) string {
	data, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(data)
}

func cursorQueryHash(request Request) string {
	canonical := request
	canonical.Cursor = ""
	canonical.Limit = 0
	canonical.TimeoutMS = 0
	canonical.CostLimit = 0
	canonical.Profile = false
	data, _ := json.Marshal(canonical)
	sum := sha256.Sum256(data)
	return fmt.Sprintf("%x", sum[:])
}

func invalidCursorAfter(cursor cursorState) error {
	return fmt.Errorf("%w: cursor after %q was not found in result set", ErrInvalid, cursor.After)
}

func cursorEntityID(cursor cursorState) (string, bool) {
	if cursor.Legacy || cursor.After == "" {
		return "", false
	}
	id, ok := strings.CutPrefix(cursor.After, "entity:")
	return id, ok && id != ""
}
