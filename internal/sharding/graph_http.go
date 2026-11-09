package sharding

import (
	"context"
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
	writeGraphError(w, err, "graph_conflict")
}

func writeGraphError(w http.ResponseWriter, err error, conflictCode string) {
	var unknown *partition.OutcomeUnknownError
	if errors.As(err, &unknown) {
		graphJSON(w, http.StatusServiceUnavailable, map[string]any{"code": "write_outcome_unknown", "error": unknown.Error(), "message": unknown.Error(), "retryable": false})
		return
	}
	status, code := http.StatusInternalServerError, "internal_error"
	switch {
	case errors.Is(err, partition.ErrPartitionCorrupt):
		status, code = http.StatusInternalServerError, "partition_corrupt"
	case errors.Is(err, partition.ErrShardUnavailable):
		status, code = http.StatusServiceUnavailable, "shard_unavailable"
	case errors.Is(err, partition.ErrPlacementChanged):
		status, code = http.StatusConflict, "graph_layout_changed"
	case errors.Is(err, partition.ErrGraphBusy):
		status, code = http.StatusTooManyRequests, "graph_busy"
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
		status, code = http.StatusConflict, conflictCode
	case errors.Is(err, storage.ErrNotFound):
		status, code = http.StatusNotFound, "not_found"
	}
	graphJSON(w, status, map[string]any{"code": code, "error": err.Error(), "message": err.Error(), "retryable": status == 503 || code == "graph_busy" || code == "graph_layout_changed"})
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

func (c *Client) admitGraph(key string, write bool) func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	slots, total, tenantLimit, limit := &c.graphReadSlots, &c.graphReads, 8, 32
	if write {
		slots, total, tenantLimit, limit = &c.graphWriteSlots, &c.graphWrites, 1, 8
	}
	if *total >= limit || (*slots)[key] >= tenantLimit {
		return nil
	}
	if *slots == nil {
		*slots = make(map[string]int)
	}
	(*slots)[key]++
	*total++
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		*total--
		(*slots)[key]--
		if (*slots)[key] == 0 {
			delete(*slots, key)
		}
	}
}

func graphDataSupported(r *http.Request) bool {
	path := r.URL.Path
	return r.Method == http.MethodPost && (path == "/v1/commits" || path == "/v1/query" || path == "/v1/query/stream") ||
		r.Method == http.MethodGet && (path == "/v1/entities" || strings.HasPrefix(path, "/v1/entities/") && path != "/v1/entities/stream" ||
			path == "/v1/ci-types" || path == "/v1/entity-types" || path == "/v1/relation-types" || path == "/v1/source-policy")
}

func (c *Client) admitGraphRequest(w http.ResponseWriter, r *http.Request, home partition.Group, tenant string) func() {
	release := c.admitGraph(home.ClusterID+"/"+tenant, r.URL.Path == "/v1/commits")
	if release == nil {
		w.Header().Set("Retry-After", "1")
		graphJSON(w, http.StatusTooManyRequests, map[string]any{"code": "graph_busy", "error": "graph request concurrency budget is exhausted", "retryable": true})
	}
	return release
}

// ServeGraphData returns false for management routes, which remain on the
// graph home group.
func ServeGraphData(w http.ResponseWriter, r *http.Request, g partition.Graph, home partition.Group, client *Client) bool {
	if !graphDataSupported(r) {
		return false
	}
	release := client.admitGraphRequest(w, r, home, g.Tenant)
	if release == nil {
		return true
	}
	defer release()
	return serveGraphData(w, r, g, home, client)
}

func serveGraphData(w http.ResponseWriter, r *http.Request, g partition.Graph, home partition.Group, client *Client) bool {
	path := r.URL.Path
	ctx, cancel := context.WithTimeout(r.Context(), partition.MaxQueryLifetime)
	defer cancel()
	r = r.WithContext(ctx)
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
			writeGraphError(w, err, "write_conflict")
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
		pinned, err := partition.AcquireReadView(r.Context(), remote, home, g)
		if err != nil {
			graphError(w, err)
			return true
		}
		view.Graph = pinned.Graph
		entity, found, err := (&partition.Lookup{View: view}).GetEntity(r.Context(), id, nil)
		if err != nil {
			graphError(w, err)
		} else if !found {
			graphError(w, storage.ErrNotFound)
		} else {
			graphJSON(w, http.StatusOK, map[string]any{"version": pinned.Graph.Version, "entity": entity.JSONValue()})
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
