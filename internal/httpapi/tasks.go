package httpapi

import (
	"net/http"
	"strings"

	"graphdb/internal/storage"
)

type TaskStartRequest struct {
	Type   string         `json:"type"`
	Params map[string]any `json:"params,omitempty"`
}

func (s *Server) startTask(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "tasks are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	var request TaskStartRequest
	if !decodeJSONBody(w, r, &request, maxConfigRequestBytes) {
		return
	}
	task, err := s.Store.StartTask(r.Context(), tenantID, request.Type, request.Params)
	if err != nil {
		s.auditError("task_start_failed", tenantID, err, map[string]any{"type": request.Type})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("task_started", tenantID, map[string]any{"task_id": task.ID, "type": task.Type})
	writeJSON(w, http.StatusAccepted, task)
}

func (s *Server) listTasks(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	tasks, err := s.Store.ListTasks(r.Context(), tenantID, storage.TaskListOptions{
		Type:   strings.TrimSpace(r.URL.Query().Get("type")),
		Status: strings.TrimSpace(r.URL.Query().Get("status")),
		Limit:  parsePositiveInt(r.URL.Query().Get("limit")),
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tasks": tasks})
}

func (s *Server) task(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	taskID := strings.TrimPrefix(r.URL.Path, "/v1/tasks/")
	if taskID == "" || strings.Contains(taskID, "/") {
		writeError(w, http.StatusBadRequest, "task path must be /v1/tasks/{id}")
		return
	}
	task, err := s.Store.GetTask(r.Context(), tenantID, taskID)
	if err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, task)
}

func (s *Server) taskAction(w http.ResponseWriter, r *http.Request) {
	if !s.writeAllowed() {
		writeError(w, http.StatusMethodNotAllowed, "task updates are disabled in reader mode")
		return
	}
	tenantID, ok := tenantFromRequest(w, r)
	if !ok {
		return
	}
	parts, err := escapedPathParts(r, "/v1/tasks/", 2)
	if err != nil || len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusBadRequest, "task action path must be /v1/tasks/{id}/cancel or /v1/tasks/{id}/retry")
		return
	}
	var task storage.Task
	switch parts[1] {
	case "cancel":
		task, err = s.Store.CancelTask(r.Context(), tenantID, parts[0])
	case "retry":
		task, err = s.Store.RetryTask(r.Context(), tenantID, parts[0])
	default:
		writeError(w, http.StatusBadRequest, "unsupported task action")
		return
	}
	if err != nil {
		s.auditError("task_action_failed", tenantID, err, map[string]any{"task_id": parts[0], "action": parts[1]})
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.auditInfo("task_action_applied", tenantID, map[string]any{"task_id": task.ID, "action": parts[1], "status": task.Status})
	if parts[1] == "retry" {
		writeJSON(w, http.StatusAccepted, task)
		return
	}
	writeJSON(w, http.StatusOK, task)
}
