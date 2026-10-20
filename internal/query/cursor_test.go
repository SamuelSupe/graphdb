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

func TestQueryRejectsUnversionedPagination(t *testing.T) {
	for _, cursor := range []string{"1", rawTestCursor(t, map[string]any{"offset": 1})} {
		_, err := Execute(seedGraph(t), Request{Op: "match", Kind: "person", Limit: 1, Cursor: cursor})
		if !errors.Is(err, ErrInvalid) {
			t.Fatalf("unversioned cursor %q accepted: %v", cursor, err)
		}
	}
	g := seedGraph(t)
	if _, err := Execute(g, Request{Op: "match", Kind: "person", Limit: 1, Cursor: "1", MinVersion: g.Version}); err != nil {
		t.Fatalf("explicitly versioned legacy cursor rejected: %v", err)
	}
	if _, err := Execute(g, Request{Op: "match", Kind: "person", Limit: 1, Cursor: "1", MinVersion: g.Version + 1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("legacy cursor version mismatch accepted: %v", err)
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
