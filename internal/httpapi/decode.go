package httpapi

import (
	"errors"
	"fmt"
	"io"
	"net/http"

	json "github.com/goccy/go-json"
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
	reader := &decodeErrorReader{Reader: r.Body}
	decoder := json.NewDecoder(reader)
	if err := decoder.Decode(value); err != nil {
		err = reader.preferReadError(err)
		spanErr = err
		writeDecodeError(w, err)
		return false
	}
	var extra any
	err := reader.preferReadError(decoder.Decode(&extra))
	if err != io.EOF {
		if err == nil {
			spanErr = traceError("request body contains multiple JSON documents")
			writeError(w, http.StatusBadRequest, "request body must contain a single JSON document")
			return false
		}
		spanErr = err
		writeDecodeError(w, err)
		return false
	}
	return true
}

type decodeErrorReader struct {
	io.Reader
	err error
}

func (r *decodeErrorReader) Read(buffer []byte) (int, error) {
	n, err := r.Reader.Read(buffer)
	// The accelerated decoder turns source read errors into EOF. Retain the
	// MaxBytesReader error so oversized requests keep their HTTP 413 contract.
	if err != nil && err != io.EOF {
		r.err = err
	}
	return n, err
}

func (r *decodeErrorReader) preferReadError(err error) error {
	if r.err != nil {
		return r.err
	}
	return err
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

func writeDecodeError(w http.ResponseWriter, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeErrorDetail(w, http.StatusRequestEntityTooLarge, ErrorCodeRequestTooLarge, fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit), false, map[string]any{"limit": maxErr.Limit})
		return
	}
	writeErrorDetail(w, http.StatusBadRequest, ErrorCodeInvalidJSON, err.Error(), false, nil)
}
