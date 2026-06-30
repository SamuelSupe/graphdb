package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"graphdb/internal/query"
)

type RunningQueryRegistry struct {
	mu      sync.RWMutex
	nextID  uint64
	queries map[string]*runningQuery
}

type runningQuery struct {
	info   RunningQueryInfo
	cancel context.CancelFunc
}

type RunningQueryInfo struct {
	ID         string        `json:"id"`
	TenantID   string        `json:"tenant_id"`
	Op         string        `json:"op"`
	TargetOp   string        `json:"target_op,omitempty"`
	Route      string        `json:"route,omitempty"`
	RemoteAddr string        `json:"remote_addr,omitempty"`
	StartedAt  time.Time     `json:"started_at"`
	DurationMS int64         `json:"duration_ms"`
	Canceled   bool          `json:"canceled,omitempty"`
	Request    query.Request `json:"request"`
}

func NewRunningQueryRegistry() *RunningQueryRegistry {
	return &RunningQueryRegistry{queries: map[string]*runningQuery{}}
}

func (r *RunningQueryRegistry) Start(parent context.Context, tenantID string, request query.Request, route string, remoteAddr string) (context.Context, string, func()) {
	if r == nil {
		return parent, "", func() {}
	}
	id := fmt.Sprintf("query-%d-%d", time.Now().UnixNano(), atomic.AddUint64(&r.nextID, 1))
	ctx, cancel := context.WithCancel(parent)
	now := time.Now().UTC()
	item := &runningQuery{
		info: RunningQueryInfo{
			ID:         id,
			TenantID:   tenantID,
			Op:         request.Op,
			TargetOp:   request.TargetOp,
			Route:      route,
			RemoteAddr: remoteAddr,
			StartedAt:  now,
			Request:    request,
		},
		cancel: cancel,
	}
	r.mu.Lock()
	r.queries[id] = item
	r.mu.Unlock()
	return ctx, id, func() {
		r.mu.Lock()
		delete(r.queries, id)
		r.mu.Unlock()
		cancel()
	}
}

func (r *RunningQueryRegistry) List(tenantID string) []RunningQueryInfo {
	if r == nil {
		return nil
	}
	now := time.Now().UTC()
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]RunningQueryInfo, 0, len(r.queries))
	for _, item := range r.queries {
		if tenantID != "" && item.info.TenantID != tenantID {
			continue
		}
		info := item.info
		info.DurationMS = now.Sub(info.StartedAt).Milliseconds()
		if info.DurationMS < 0 {
			info.DurationMS = 0
		}
		result = append(result, info)
	}
	return result
}

func (r *RunningQueryRegistry) Kill(tenantID string, queryID string) (RunningQueryInfo, bool) {
	if r == nil {
		return RunningQueryInfo{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	item := r.queries[queryID]
	if item == nil || item.info.TenantID != tenantID {
		return RunningQueryInfo{}, false
	}
	item.info.Canceled = true
	item.info.DurationMS = time.Since(item.info.StartedAt).Milliseconds()
	item.cancel()
	return item.info, true
}

func (s *Server) listRunningQueries(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"queries": s.QueryRegistry.List(tenantID)})
}

func (s *Server) killRunningQuery(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	queryID := strings.TrimPrefix(r.URL.Path, "/v1/queries/running/")
	queryID = strings.TrimSpace(queryID)
	if queryID == "" || strings.Contains(queryID, "/") {
		writeError(w, http.StatusBadRequest, "query path must be /v1/queries/running/{id}")
		return
	}
	info, killed := s.QueryRegistry.Kill(tenantID, queryID)
	if !killed {
		writeError(w, http.StatusNotFound, "running query not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"killed": true, "query": info})
}
