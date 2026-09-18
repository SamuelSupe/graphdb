package main

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func (r *runner) checkBackupRestore(ctx context.Context) error {
	before, err := r.writer.do(ctx, http.MethodGet, "/v1/export/snapshot", nil, http.StatusOK)
	if err != nil {
		return err
	}
	backup, err := r.writer.do(ctx, http.MethodPost, "/v1/tenants/"+r.cfg.tenant+"/backup", nil, http.StatusAccepted)
	if err != nil {
		return err
	}
	completed, err := r.waitTask(ctx, stringValue(backup.json["id"]))
	if err != nil {
		return err
	}
	result, _ := completed.json["result"].(map[string]any)
	key := stringValue(result["backup_key"])
	if key == "" {
		return fmt.Errorf("backup task has no backup_key: %s", completed.body)
	}
	if _, err = r.commitResponse(ctx, "after-backup", graph.Mutations{UpsertEntities: []graph.Entity{{ID: "backup-transient", Kind: "temporary"}}}); err != nil {
		return err
	}
	// Prime the reader before restoring an earlier version in the same tenant.
	if _, err = r.reader.do(ctx, http.MethodGet, entityPath("backup-transient"), nil, http.StatusOK); err != nil {
		return err
	}
	restore, err := r.writer.do(ctx, http.MethodPost, "/v1/tenants/"+r.cfg.tenant+"/restore", map[string]any{"backup_key": key, "overwrite": true}, http.StatusAccepted)
	if err != nil {
		return err
	}
	if _, err = r.waitTask(ctx, stringValue(restore.json["id"])); err != nil {
		return err
	}
	after, err := r.writer.do(ctx, http.MethodGet, "/v1/export/snapshot", nil, http.StatusOK)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(before.json, after.json) {
		return fmt.Errorf("restored snapshot differs from backup")
	}
	_, err = r.reader.do(ctx, http.MethodGet, entityPath("backup-transient"), nil, http.StatusNotFound)
	return err
}

func (r *runner) waitTask(ctx context.Context, id string) (apiResponse, error) {
	if id == "" {
		return apiResponse{}, fmt.Errorf("missing task id")
	}
	for {
		response, err := r.writer.do(ctx, http.MethodGet, "/v1/tasks/"+id, nil, http.StatusOK)
		if err != nil {
			return response, err
		}
		switch stringValue(response.json["status"]) {
		case storage.TaskStatusSucceeded:
			return response, nil
		case storage.TaskStatusFailed, storage.TaskStatusCanceled:
			return response, fmt.Errorf("task failed: %s", response.body)
		}
		select {
		case <-ctx.Done():
			return apiResponse{}, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}
