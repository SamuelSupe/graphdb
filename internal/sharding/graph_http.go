package sharding

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/httpapi"
	"github.com/SamuelSupe/graphdb/v2/internal/partition"
	"github.com/SamuelSupe/graphdb/v2/internal/query"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

func graphJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func graphError(w http.ResponseWriter, err error) {
	var unknown *partition.OutcomeUnknownError
	if errors.As(err, &unknown) {
		graphJSON(w, http.StatusServiceUnavailable, map[string]any{"code": "write_outcome_unknown", "error": unknown.Error(), "message": unknown.Error(), "retryable": false})
		return
	}
	status, code := http.StatusServiceUnavailable, "shard_unavailable"
	switch {
	case errors.Is(err, partition.ErrQuotaExceeded):
		status, code = http.StatusTooManyRequests, "quota_exceeded"
	case errors.Is(err, partition.ErrVersionNotReady):
		status, code = http.StatusServiceUnavailable, "reader_not_fresh"
	case errors.Is(err, storage.ErrInvalidArgument):
		status, code = http.StatusBadRequest, "bad_request"
	case errors.Is(err, query.ErrInvalid):
		status, code = http.StatusBadRequest, "invalid_query"
	case errors.Is(err, query.ErrLimitExceeded), errors.Is(err, partition.ErrReadBudget):
		status, code = http.StatusUnprocessableEntity, "query_limit_exceeded"
	case errors.Is(err, storage.ErrConflict):
		status, code = http.StatusConflict, "write_conflict"
	case errors.Is(err, storage.ErrNotFound):
		status, code = http.StatusNotFound, "not_found"
	}
	graphJSON(w, status, map[string]any{"code": code, "error": err.Error(), "message": err.Error(), "retryable": status >= 500})
}
func graphDecode(w http.ResponseWriter, r *http.Request, value any) bool {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, partition.MaxChangeBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		graphJSON(w, http.StatusBadRequest, map[string]any{"code": "invalid_json", "error": err.Error(), "message": err.Error(), "retryable": false})
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		graphJSON(w, http.StatusBadRequest, map[string]any{"code": "invalid_json", "error": "a single JSON value is required", "message": "a single JSON value is required", "retryable": false})
		return false
	}
	return true
}

// ServeGraphData executes supported graph routes at any stateless router. It
// returns false for management routes, which remain on the graph home group.
func ServeGraphData(w http.ResponseWriter, r *http.Request, g partition.Graph, home partition.Group, client *Client) bool {
	path := r.URL.Path
	supported := r.Method == http.MethodPost && (path == "/v1/commits" || path == "/v1/query" || path == "/v1/query/stream") || r.Method == http.MethodGet && (path == "/v1/entities" || strings.HasPrefix(path, "/v1/entities/") && path != "/v1/entities/stream" || path == "/v1/ci-types" || path == "/v1/entity-types" || path == "/v1/relation-types" || path == "/v1/source-policy")
	if !supported {
		return false
	}
	client.mu.Lock()
	if client.graphSlots == nil {
		client.graphSlots = make(chan struct{}, 8)
	}
	slots := client.graphSlots
	client.mu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), partition.MaxQueryLifetime)
	defer cancel()
	r = r.WithContext(ctx)
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
	default:
		graphJSON(w, http.StatusTooManyRequests, map[string]any{"code": "graph_busy", "error": "graph request concurrency budget is exhausted", "retryable": true})
		return true
	}
	if g.State != "active" {
		graphJSON(w, http.StatusServiceUnavailable, map[string]any{"code": "shard_unavailable", "error": "partitioned graph is not active", "message": "partitioned graph is not active", "retryable": true})
		return true
	}
	if raw := r.Header.Get("X-GraphDB-Read-Generation"); raw != "" && raw != strconv.FormatInt(g.Generation, 10) {
		graphJSON(w, http.StatusConflict, map[string]any{"code": "tenant_generation_changed", "error": "tenant generation changed", "message": "tenant generation changed", "retryable": false})
		return true
	}
	w.Header().Set("X-GraphDB-Tenant-Generation", strconv.FormatInt(g.Generation, 10))
	remote := GraphRemote{Client: client}
	view := partition.View{Graph: g, Remote: remote}
	if path == "/v1/commits" {
		allowed := false
		httpapi.ProtectBrowserWrite(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { allowed = true })).ServeHTTP(w, r)
		if !allowed {
			return true
		}
		var request partition.CommitRequest
		if !graphDecode(w, r, &request) {
			return true
		}
		if header := r.Header.Get("Idempotency-Key"); header != "" {
			if request.IdempotencyKey != "" && request.IdempotencyKey != header {
				graphError(w, fmt.Errorf("%w: idempotency header and body differ", storage.ErrInvalidArgument))
				return true
			}
			request.IdempotencyKey = header
		}
		result, err := (partition.Engine{Remote: remote, Home: home, Tenant: g.Tenant, Generation: g.Generation}).Submit(r.Context(), request)
		if err != nil {
			graphError(w, err)
		} else {
			graphJSON(w, http.StatusOK, result)
		}
		return true
	}
	if r.Method == http.MethodGet {
		freshness, err := httpapi.ParseReadFreshness(r, httpapi.ReadFreshness{})
		if err != nil {
			graphError(w, fmt.Errorf("%w: %v", storage.ErrInvalidArgument, err))
			return true
		}
		if freshness.MinVersion > g.Version {
			graphError(w, partition.ErrVersionNotReady)
			return true
		}
	}
	if path == "/v1/query" || path == "/v1/query/stream" {
		var request query.Request
		if !graphDecode(w, r, &request) {
			return true
		}
		freshness, parseErr := httpapi.ParseReadFreshness(r, httpapi.ReadFreshness{MinVersion: request.MinVersion, AllowStale: request.AllowStale})
		if parseErr != nil {
			graphError(w, fmt.Errorf("%w: %v", storage.ErrInvalidArgument, parseErr))
			return true
		}
		request.MinVersion = freshness.MinVersion
		request.AllowStale = freshness.AllowStale
		response, err := partition.ExecutePinnedQuery(r.Context(), remote, home, g, request)
		if err != nil {
			graphError(w, err)
			return true
		}
		if path == "/v1/query/stream" {
			w.Header().Set("Content-Type", "application/x-ndjson")
			encoder := json.NewEncoder(w)
			if encoder.Encode(query.StreamMeta{Version: response.Version, NextCursor: response.NextCursor, Stats: &response.Stats, Aggregates: response.Aggregates, Groups: response.Groups, Plan: response.Plan, Profile: response.Profile}) != nil {
				return true
			}
			for _, result := range response.Results {
				if encoder.Encode(result) != nil {
					return true
				}
			}
			_ = encoder.Encode(query.StreamMeta{Done: true, Version: response.Version, Stats: &response.Stats})
			return true
		}
		graphJSON(w, http.StatusOK, response)
		return true
	}
	if path == "/v1/entities" {
		limit := 100
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 1000 {
				graphError(w, fmt.Errorf("%w: invalid scan limit", storage.ErrInvalidArgument))
				return true
			}
			limit = parsed
		}
		request := query.Request{Op: "match", Kind: r.URL.Query().Get("kind"), Limit: limit, Cursor: r.URL.Query().Get("cursor")}
		freshness, _ := httpapi.ParseReadFreshness(r, httpapi.ReadFreshness{})
		request.MinVersion = freshness.MinVersion
		request.AllowStale = freshness.AllowStale
		if source := r.URL.Query().Get("source"); source != "" {
			request.Where = []query.Filter{{Field: "source", Value: source}}
		}
		response, err := partition.ExecutePinnedQuery(r.Context(), remote, home, g, request)
		if err != nil {
			graphError(w, err)
			return true
		}
		entities := make([]any, 0, len(response.Results))
		for _, result := range response.Results {
			entities = append(entities, result.Entity)
		}
		graphJSON(w, http.StatusOK, map[string]any{"tenant_id": g.Tenant, "version": response.Version, "entities": entities, "next_cursor": response.NextCursor, "indexed_read": true})
		return true
	}
	if strings.HasPrefix(path, "/v1/entities/") && path != "/v1/entities/stream" {
		id, err := url.PathUnescape(strings.TrimPrefix(r.URL.EscapedPath(), "/v1/entities/"))
		if err != nil || id == "" {
			graphError(w, fmt.Errorf("%w: invalid entity ID", storage.ErrInvalidArgument))
			return true
		}
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte("entity:"+id)))
		pinned, err := remote.Pin(r.Context(), home, g.Tenant, g.Generation, g.Version, digest, "")
		if err != nil {
			graphError(w, err)
			return true
		}
		defer func() {
			_ = remote.Action(r.Context(), home, partition.Action{Operation: "release_pin", Tenant: g.Tenant, Generation: g.Generation, Txn: pinned.Pin.ID, Digest: digest}, nil)
		}()
		view.Graph = pinned.Graph
		entity, found, err := (&partition.Lookup{View: view}).GetEntity(r.Context(), id, nil)
		if err != nil {
			graphError(w, err)
		} else if !found {
			graphError(w, storage.ErrNotFound)
		} else {
			graphJSON(w, http.StatusOK, map[string]any{"version": g.Version, "entity": entity.JSONValue()})
		}
		return true
	}
	field := "ci_types"
	var value any = g.CITypes
	switch path {
	case "/v1/entity-types":
		field = "entity_types"
	case "/v1/relation-types":
		field = "relation_types"
		value = g.RelationTypes
	case "/v1/source-policy":
		graphJSON(w, http.StatusOK, httpapi.SourcePolicyResponse{Configured: g.SourcePolicyConfigured, Policy: g.SourcePolicy})
		return true
	}
	graphJSON(w, http.StatusOK, map[string]any{"version": g.Version, field: value})
	return true
}
