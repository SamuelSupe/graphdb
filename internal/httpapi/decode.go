package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
)

const (
	maxWriteRequestBytes  = 32 << 20
	maxQueryRequestBytes  = 4 << 20
	maxConfigRequestBytes = 1 << 20
)

func decodeJSONBody(w http.ResponseWriter, r *http.Request, value any, maxBytes int64) (ok bool) {
	_, span := startAPIPhase(r.Context(), "decode_request", traceRequestAttributes(r, maxBytes)...)
	var spanErr error
	defer func() {
		if span != nil {
			span.SetAttributes(attribute.Bool("graphdb.request.decoded", ok))
		}
		endHTTPSpan(span, spanErr)
	}()
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(value); err != nil {
		spanErr = writeDecodeError(w, r, err)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			spanErr = traceError("request body contains multiple JSON documents")
			writeError(w, http.StatusBadRequest, "request body must contain a single JSON document")
			return false
		}
		spanErr = writeDecodeError(w, r, err)
		return false
	}
	return true
}

func traceRequestAttributes(r *http.Request, maxBytes int64) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int64("graphdb.request.max_bytes", maxBytes),
		attribute.String("graphdb.request.content_type", r.Header.Get("Content-Type")),
	}
	if r.ContentLength >= 0 {
		attrs = append(attrs, attribute.Int64("graphdb.request.content_length", r.ContentLength))
	}
	return attrs
}

func writeDecodeError(w http.ResponseWriter, r *http.Request, err error) error {
	if requestErr := r.Context().Err(); requestErr != nil {
		writeRequestError(w, requestErr)
		return requestErr
	}
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeErrorDetail(w, http.StatusRequestEntityTooLarge, ErrorCodeRequestTooLarge, fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit), false, map[string]any{"limit": maxErr.Limit})
		return err
	}
	writeErrorDetail(w, http.StatusBadRequest, ErrorCodeInvalidJSON, err.Error(), false, nil)
	return err
}
