package httpapi

import (
	"net/http"
	"strconv"

	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (s *Server) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	tenantID, source, ok := deadLetterPath(w, r, false)
	if !ok {
		return
	}
	items, err := s.Store.ListDeadLetters(r.Context(), tenantID, source)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deadletters": items})
}

func (s *Server) replayDeadLetters(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "deadletter replay is disabled in reader mode")
		return
	}
	tenantID, source, ok := deadLetterPath(w, r, true)
	if !ok {
		return
	}
	limit, ok := replayLimitFromRequest(w, r)
	if !ok {
		return
	}
	report, err := s.Store.ReplayDeadLetters(r.Context(), tenantID, source, limit)
	if err != nil {
		if replayReportChangedData(report) {
			s.invalidate(tenantID)
		}
		s.auditError("deadletter_replay_failed", tenantID, err, map[string]any{"source": source, "limit": limit})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if replayReportChangedData(report) {
		s.invalidate(tenantID)
	}
	s.auditInfo("deadletter_replay_completed", tenantID, map[string]any{
		"source": source, "limit": limit, "replayed": report.Replayed, "resolved": report.Resolved, "failed": report.Failed,
	})
	writeJSON(w, http.StatusOK, report)
}

func replayReportChangedData(report storage.ReplayReport) bool {
	for _, result := range report.Results {
		if result.Version > 0 && result.Applied > 0 && !result.Skipped {
			return true
		}
	}
	return false
}

func replayLimitFromRequest(w http.ResponseWriter, r *http.Request) (int, bool) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, true
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 {
		writeError(w, http.StatusBadRequest, "limit must be a non-negative integer")
		return 0, false
	}
	return limit, true
}

func deadLetterPath(w http.ResponseWriter, r *http.Request, replay bool) (string, string, bool) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return "", "", false
	}
	if replay {
		parts, err := escapedPathParts(r, "/v1/ingest/deadletters/", 2)
		if err != nil || len(parts) != 2 || parts[0] == "" || parts[1] != "replay" {
			writeError(w, http.StatusBadRequest, "deadletter replay path must be /v1/ingest/deadletters/{source}/replay")
			return "", "", false
		}
		return tenantID, parts[0], true
	}
	parts, err := escapedPathParts(r, "/v1/ingest/deadletters/", 1)
	if err != nil || len(parts) != 1 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, "deadletter path must be /v1/ingest/deadletters/{source}")
		return "", "", false
	}
	return tenantID, parts[0], true
}
