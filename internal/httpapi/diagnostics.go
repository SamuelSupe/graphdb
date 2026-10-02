package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/buildinfo"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

// Diagnostics are observations from this process, including when quorum is
// unavailable. They do not promise a linearizable cluster-wide snapshot.
func (s *Server) diagnostics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	deployment := "standalone"
	report := map[string]any{"status": "ok", "mode": s.Mode, "checked_at": time.Now().UTC(), "build": buildinfo.Current(), "coordination": s.Store.CachedCoordinatorStatus()}
	problems := []string{}
	disk, err := s.Store.DiskSpace(ctx)
	if err != nil {
		problems = append(problems, "disk_inspection_failed")
		report["disk_error"] = err.Error()
	} else {
		report["disk"] = disk
		if !disk.WriteReady {
			problems = append(problems, "disk_space_low")
		}
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
	disk, err := s.Store.DiskSpace(ctx)
	known := 0
	if err == nil {
		known = 1
	}
	ready := 0
	if err == nil && disk.WriteReady {
		ready = 1
	}
	fmt.Fprintf(w, "# TYPE graphdb_disk_inspection_success gauge\ngraphdb_disk_inspection_success %d\n# TYPE graphdb_disk_available_bytes gauge\ngraphdb_disk_available_bytes %d\n# TYPE graphdb_disk_minimum_free_bytes gauge\ngraphdb_disk_minimum_free_bytes %d\n# TYPE graphdb_disk_write_ready gauge\ngraphdb_disk_write_ready %d\n", known, disk.AvailableBytes, disk.MinimumFreeBytes, ready)
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
		fmt.Fprintf(w, "# TYPE graphdb_raft_failed gauge\ngraphdb_raft_failed %d\n", failed)
		fmt.Fprintf(w, "# TYPE graphdb_raft_leader_known gauge\ngraphdb_raft_leader_known %d\n# TYPE graphdb_raft_application_lag gauge\ngraphdb_raft_application_lag %d\n# TYPE graphdb_raft_snapshot_failed gauge\ngraphdb_raft_snapshot_failed %d\n", leader, lag, snapshotFailure)
	}
}
