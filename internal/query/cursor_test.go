package query

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
)

func TestEncodedCursorRejectsNegativeOffset(t *testing.T) {
	g := seedGraph(t)
	_, err := Execute(g, Request{
		Op:     "match",
		Kind:   "person",
		Limit:  1,
		Cursor: rawTestCursor(t, map[string]any{"offset": -1}),
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func TestEncodedCursorRejectsMissingVersionForAfter(t *testing.T) {
	g := seedGraph(t)
	_, err := Execute(g, Request{
		Op:     "match",
		Kind:   "person",
		Limit:  1,
		Cursor: rawTestCursor(t, map[string]any{"after": "person:alice"}),
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
}

func rawTestCursor(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal cursor: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(data)
}
