package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/query"
)

// CypherStreamWriter shares framing, write deadlines and completion semantics
// between the local and partitioned query transports.
type CypherStreamWriter struct {
	ctx        context.Context
	w          http.ResponseWriter
	encoder    *json.Encoder
	controller *http.ResponseController
	started    bool
	writeErr   error
	rows       int
}

func NewCypherStreamWriter(ctx context.Context, w http.ResponseWriter) *CypherStreamWriter {
	return &CypherStreamWriter{ctx: ctx, w: w, encoder: json.NewEncoder(w), controller: http.NewResponseController(w)}
}
func (s *CypherStreamWriter) Started() bool { return s.started }
func (s *CypherStreamWriter) write(frame any, flush bool) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(10 * time.Minute)
	if d, ok := s.ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := s.controller.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		s.writeErr = err
		return err
	}
	s.writeErr = s.encoder.Encode(frame)
	if s.writeErr != nil {
		return s.writeErr
	}
	if flush {
		if err := s.controller.Flush(); err != nil && !errors.Is(err, http.ErrNotSupported) {
			s.writeErr = err
			return err
		}
	}
	return s.ctx.Err()
}
func (s *CypherStreamWriter) Begin(version int64, columns []string) error {
	s.w.Header().Set("Content-Type", "application/x-ndjson")
	s.started = true
	return s.write(map[string]any{"stream": true, "version": version, "columns": columns}, true)
}
func (s *CypherStreamWriter) Row(row []any) error {
	s.rows++
	return s.write(map[string]any{"row": row}, s.rows%streamFlushEvery == 0)
}
func (s *CypherStreamWriter) Finish(response query.CypherResponse, err error) error {
	if err != nil {
		_, failure := queryErrorResponse(err)
		var ce *query.CypherError
		if errors.As(err, &ce) {
			failure.Detail = ce
		}
		return s.write(map[string]any{"done": true, "success": false, "error": failure}, true)
	}
	return s.write(map[string]any{"done": true, "success": true, "version": response.Version, "stats": response.Stats, "plan": response.Plan, "profile": response.Profile}, true)
}
