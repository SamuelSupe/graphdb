package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const (
	maxWriteRequestBytes  = 32 << 20
	maxQueryRequestBytes  = 4 << 20
	maxConfigRequestBytes = 1 << 20
)

func decodeJSONBody(w http.ResponseWriter, r *http.Request, value any, maxBytes int64) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(value); err != nil {
		writeDecodeError(w, err)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			writeError(w, http.StatusBadRequest, "request body must contain a single JSON document")
			return false
		}
		writeDecodeError(w, err)
		return false
	}
	return true
}

func writeDecodeError(w http.ResponseWriter, err error) {
	var maxErr *http.MaxBytesError
	if errors.As(err, &maxErr) {
		writeErrorDetail(w, http.StatusRequestEntityTooLarge, ErrorCodeRequestTooLarge, fmt.Sprintf("request body exceeds %d bytes", maxErr.Limit), false, map[string]any{"limit": maxErr.Limit})
		return
	}
	writeErrorDetail(w, http.StatusBadRequest, ErrorCodeInvalidJSON, err.Error(), false, nil)
}
