package graphdb

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Retryable  bool
	RetryAfter time.Duration
	Detail     map[string]any
	Reasons    []map[string]any
	Body       []byte
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if e.Code != "" && e.Message != "" {
		return fmt.Sprintf("graphdb: status=%d code=%s message=%s", e.StatusCode, e.Code, e.Message)
	}
	if e.Message != "" {
		return fmt.Sprintf("graphdb: status=%d message=%s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("graphdb: status=%d", e.StatusCode)
}

func parseAPIError(resp *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	envelope := decodeErrorEnvelope(data)
	apiErr := &APIError{
		StatusCode: resp.StatusCode,
		Code:       envelope.Code,
		Message:    envelope.Message,
		Retryable:  envelope.Retryable,
		Detail:     envelope.Detail,
		Reasons:    envelope.Reasons,
		Body:       data,
	}
	if envelope.RetryAfterMS > 0 {
		apiErr.RetryAfter = time.Duration(envelope.RetryAfterMS) * time.Millisecond
	}
	if retryAfter := parseRetryAfter(resp.Header.Get("Retry-After")); retryAfter > 0 {
		apiErr.RetryAfter = retryAfter
	}
	return apiErr
}

type errorEnvelope struct {
	Error        string           `json:"error"`
	Code         string           `json:"code"`
	Message      string           `json:"message"`
	Retryable    bool             `json:"retryable"`
	RetryAfterMS int64            `json:"retry_after_ms"`
	Detail       map[string]any   `json:"detail"`
	Reasons      []map[string]any `json:"reasons"`
}

func decodeErrorEnvelope(data []byte) errorEnvelope {
	var envelope errorEnvelope
	_ = json.Unmarshal(data, &envelope)
	if envelope.Message == "" {
		envelope.Message = envelope.Error
	}
	if envelope.Message == "" {
		envelope.Message = strings.TrimSpace(string(data))
	}
	return envelope
}

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(raw); err == nil {
		delay := time.Until(when)
		if delay > 0 {
			return delay
		}
	}
	return 0
}
