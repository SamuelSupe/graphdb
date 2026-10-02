package httpapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
	"github.com/SamuelSupe/graphdb/v2/internal/observability"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Diagnostics are observations from this process, including when quorum is
// unavailable. They do not promise a linearizable cluster-wide snapshot.
func (s *Server) diagnostics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	deployment := "standalone"
	report := map[string]any{"status": "ok", "mode": s.Mode, "checked_at": time.Now().UTC(), "build": buildinfo.Current(), "coordination": s.Store.CachedCoordinatorStatus()}
	report["admission"] = s.admissionObservations()
	report["queries_running"] = s.QueryRegistry.Count()
	problems := []string{}
	disks := s.resourceDisks(ctx)
	report["filesystems"] = disks
	for role, observation := range disks {
		prefix := role + "_"
		if role == "data" {
			prefix = ""
		}
		if observation.Error != "" {
			problems = append(problems, prefix+"disk_inspection_failed")
		} else if !observation.Space.WriteReady {
			problems = append(problems, prefix+"disk_space_low")
		}
	}
	if data := disks["data"]; data.Error != "" {
		report["disk_error"] = data.Error
	} else {
		report["disk"] = data.Space
	}
	if s.Cluster != nil {
		deployment = "raft"
		status := s.Cluster.Status()
		report["raft"] = status
		if status["leader_id"] == uint64(0) {
			problems = append(problems, "raft_no_leader")
		}
		if status["error"] != nil {
			problems = append(problems, "raft_failed")
		}
		if status["snapshot_error"] != nil {
			problems = append(problems, "raft_snapshot_failed")
		}
	}
	if s.IngestService != nil {
		wal := s.IngestService.Readiness()
		report["ingest_wal"] = wal
		if !wal.Ready {
			problems = append(problems, "ingest_wal_not_ready")
		}
	}
	if tenant := r.Header.Get("X-Tenant-ID"); tenant != "" {
		if err := storage.ValidateTenantID(tenant); err != nil {
			writeStorageError(w, err)
			return
		}
		tasks, err := s.Store.ListTasks(ctx, tenant, storage.TaskListOptions{Limit: 100})
		if err != nil {
			report["tasks_error"] = err.Error()
			problems = append(problems, "tasks_unavailable")
		} else {
			report["tasks"] = tasks
			report["tasks_limit"] = 100
		}
	}
	report["deployment"] = deployment
	report["problems"] = problems
	if len(problems) > 0 {
		report["status"] = "degraded"
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) writeResourceMetrics(w http.ResponseWriter, ctx context.Context) {
	disks := s.resourceDisks(ctx)
	disk := disks["data"].Space
	known := 0
	if disks["data"].Error == "" {
		known = 1
	}
	ready := 0
	if known == 1 && disk.WriteReady {
		ready = 1
	}
	observability.WriteScalar(w, "graphdb_disk_inspection_success", "Data filesystem inspection succeeded.", "gauge", float64(known))
	observability.WriteScalar(w, "graphdb_disk_available_bytes", "Data filesystem bytes available to this process.", "gauge", float64(disk.AvailableBytes))
	observability.WriteScalar(w, "graphdb_disk_minimum_free_bytes", "Data filesystem configured free space admission floor.", "gauge", float64(disk.MinimumFreeBytes))
	observability.WriteScalar(w, "graphdb_disk_write_ready", "Data filesystem inspection succeeded and admission floor is satisfied.", "gauge", float64(ready))
	for _, metric := range []struct {
		name, help string
		value      func(diskObservation) float64
	}{
		{"inspection_success", "Filesystem inspection succeeded for this storage role.", func(d diskObservation) float64 {
			if d.Error == "" {
				return 1
			}
			return 0
		}},
		{"total_bytes", "Total filesystem capacity; roles sharing a filesystem must not be summed.", func(d diskObservation) float64 { return float64(d.Space.TotalBytes) }},
		{"available_bytes", "Filesystem bytes available to this process.", func(d diskObservation) float64 { return float64(d.Space.AvailableBytes) }},
		{"minimum_free_bytes", "Configured free space admission floor for this storage role.", func(d diskObservation) float64 { return float64(d.Space.MinimumFreeBytes) }},
		{"write_ready", "Inspection succeeded and the filesystem is above its configured admission floor.", func(d diskObservation) float64 {
			if d.Error == "" && d.Space.WriteReady {
				return 1
			}
			return 0
		}},
	} {
		name := "graphdb_filesystem_" + metric.name
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", name, metric.help, name)
		for _, role := range []string{"data", "wal", "raft"} {
			if d, ok := disks[role]; ok {
				fmt.Fprintf(w, "%s{role=%q} %g\n", name, role, metric.value(d))
			}
		}
	}
	observability.WriteRuntimeMetrics(w)
	s.writeAdmissionMetrics(w)
	if s.Cluster != nil {
		status := s.Cluster.Status()
		leader := 0
		if id, ok := status["leader_id"].(uint64); ok && id != 0 {
			leader = 1
		}
		snapshotFailure := 0
		if status["snapshot_error"] != nil {
			snapshotFailure = 1
		}
		lag, _ := status["application_lag"].(uint64)
		failed := 0
		if status["error"] != nil {
			failed = 1
		}
		observability.WriteScalar(w, "graphdb_raft_failed", "Local Raft node stopped after an internal failure.", "gauge", float64(failed))
		observability.WriteScalar(w, "graphdb_raft_leader_known", "Local replica knows a leader ID; not a quorum guarantee.", "gauge", float64(leader))
		observability.WriteScalar(w, "graphdb_raft_application_lag", "Committed log entries not yet durably applied locally.", "gauge", float64(lag))
		observability.WriteScalar(w, "graphdb_raft_snapshot_failed", "Local snapshot failure is retained.", "gauge", float64(snapshotFailure))
		if metrics, ok := s.Cluster.(interface{ WriteMetrics(io.Writer) }); ok {
			metrics.WriteMetrics(w)
		}
	}
}

type diskObserver interface {
	DiskSpace(context.Context) (storage.DiskSpaceStatus, error)
}

type diskObservation struct {
	Space storage.DiskSpaceStatus `json:"space"`
	Error string                  `json:"error,omitempty"`
}

func (s *Server) resourceDisks(ctx context.Context) map[string]diskObservation {
	providers := map[string]diskObserver{"data": s.Store}
	if wal, ok := s.IngestService.(diskObserver); ok {
		providers["wal"] = wal
	}
	if raft, ok := s.Cluster.(diskObserver); ok {
		providers["raft"] = raft
	}
	result := make(map[string]diskObservation, len(providers))
	for role, provider := range providers {
		space, err := provider.DiskSpace(ctx)
		observation := diskObservation{Space: space}
		if err != nil {
			observation.Error = err.Error()
		}
		result[role] = observation
	}
	return result
}
