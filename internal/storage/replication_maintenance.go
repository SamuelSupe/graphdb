package storage

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type ReplicatedMaintenanceSource struct {
	root      string
	store     *TenantStore
	task      Task
	token     string
	release   func()
	originals map[string]os.FileInfo
	once      sync.Once
}

type preparedMaintenanceHeader struct {
	Format    int      `json:"format"`
	BaseToken string   `json:"base_token"`
	Task      Task     `json:"task"`
	Deleted   []string `json:"deleted,omitempty"`
}

func PreparedMaintenanceTask(task Task) bool {
	return task.Type == TaskTypeCompact || task.Type == TaskTypeIndexRebuild || task.Type == TaskTypeGC
}

func (s *TenantStore) maintenanceToken(ctx context.Context, tenant string) (string, error) {
	manifest, err := s.CurrentManifest(ctx, tenant)
	if err != nil {
		return "", err
	}
	metadata, err := s.tenantBackupMetadata(ctx, tenant)
	if err != nil {
		return "", err
	}
	config, _, err := s.GetTenantConfig(ctx, tenant)
	if err != nil {
		return "", err
	}
	policy, _, err := s.GetSourcePolicy(ctx, tenant)
	if err != nil {
		return "", err
	}
	schemas, err := s.GetRelationSchemas(ctx, tenant)
	if err != nil {
		return "", err
	}
	definitions, err := s.getIndexDefinitions(ctx, tenant)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal([]any{manifest, metadata, config, policy, schemas, definitions})
	return sha256Hex(data), err
}

// CaptureReplicatedMaintenance pins a tenant's current files. Application must
// be excluded until it returns; Build does not access mutable live state.
func (s *TenantStore) CaptureReplicatedMaintenance(ctx context.Context, task Task) (*ReplicatedMaintenanceSource, error) {
	if !PreparedMaintenanceTask(task) || task.Status != TaskStatusQueued {
		return nil, fmt.Errorf("unsupported prepared maintenance task")
	}
	files := s.localFileStore()
	if files == nil {
		return nil, fmt.Errorf("prepared maintenance requires local disk")
	}
	token, err := s.maintenanceToken(ctx, task.TenantID)
	if err != nil {
		return nil, err
	}
	release, err := files.beginLifecycleOperation(ctx)
	if err != nil {
		return nil, err
	}
	root, err := os.MkdirTemp(files.root, ".maintenance-view-")
	if err != nil {
		release()
		return nil, err
	}
	source := &ReplicatedMaintenanceSource{root: root, store: s, task: task, token: token, release: release, originals: map[string]os.FileInfo{}}
	objects, err := files.List(ctx, s.tenantObjectPrefix(task.TenantID))
	if err == nil {
		for _, object := range objects {
			if err = ctx.Err(); err != nil {
				break
			}
			filename, pathErr := files.path(object.Key)
			if pathErr != nil {
				err = pathErr
				break
			}
			if err = files.verifySafeParent(filename); err != nil {
				break
			}
			destination := filepath.Join(root, "build", filepath.FromSlash(object.Key))
			if err = os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
				break
			}
			if err = os.Link(filename, destination); err != nil {
				break
			}
			source.originals[object.Key], err = os.Stat(destination)
			if err != nil {
				break
			}
		}
	}
	if err != nil {
		source.Close()
		return nil, err
	}
	return source, nil
}

func (s *ReplicatedMaintenanceSource) Close() error {
	var err error
	s.once.Do(func() { err = os.RemoveAll(s.root); s.release() })
	return err
}

func (s *ReplicatedMaintenanceSource) Build(ctx context.Context, maxBytes int64) (*os.File, error) {
	buildRoot := filepath.Join(s.root, "build")
	files, err := OpenFileStore(buildRoot)
	if err != nil {
		return nil, err
	}
	defer files.Close()
	stage := NewTenantStoreWithOptions(files, s.store.Prefix, TenantStoreOptions{InstanceID: s.store.InstanceID, MaxWriteCacheBytes: s.store.MaxWriteCacheBytes})
	stage.WriteEntityRecords = s.store.WriteEntityRecords
	stage.UseEntityRecordsForRead = s.store.UseEntityRecordsForRead
	stage.EntityPagePackMaxBytes = s.store.EntityPagePackMaxBytes
	stage.MaxMaintenanceBytes = s.store.MaxMaintenanceBytes
	defer stage.ShutdownTasks(context.Background())
	fixed := ReplicatedContext(ctx, s.task.ID, s.task.StartedAt)
	task, err := stage.RunReplicatedTask(fixed, s.task.TenantID, s.task.ID, nil)
	if err != nil {
		return nil, err
	}
	objects, err := files.List(ctx, stage.tenantObjectPrefix(task.TenantID))
	if err != nil {
		return nil, err
	}
	header := preparedMaintenanceHeader{Format: 1, BaseToken: s.token, Task: task}
	if task.Type == TaskTypeGC {
		header.Format = 2
		remaining := make(map[string]bool, len(objects))
		for _, object := range objects {
			remaining[object.Key] = true
		}
		for key := range s.originals {
			if !remaining[key] {
				header.Deleted = append(header.Deleted, key)
			}
		}
		sort.Strings(header.Deleted)
	}
	output, err := os.CreateTemp(s.root, "prepared-")
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			output.Close()
			os.Remove(output.Name())
		}
	}()
	archive := tar.NewWriter(&snapshotBudgetWriter{writer: output, remaining: maxBytes})
	metadata, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	headerLimit := int64(1 << 20)
	if task.Type == TaskTypeGC {
		headerLimit = 16 << 20
	}
	if int64(len(metadata)) > headerLimit {
		return nil, fmt.Errorf("prepared maintenance metadata exceeds its budget")
	}
	if err := archive.WriteHeader(&tar.Header{Name: "maintenance.json", Size: int64(len(metadata)), Mode: 0600}); err != nil {
		return nil, err
	}
	if _, err := archive.Write(metadata); err != nil {
		return nil, err
	}
	for _, object := range objects {
		// Unchanged files still share the captured inode. Export only objects
		// rewritten by the staged task, keeping publication and transfer bounded.
		current := filepath.Join(buildRoot, filepath.FromSlash(object.Key))
		before := s.originals[object.Key]
		after, err := os.Stat(current)
		if err != nil {
			return nil, err
		}
		if before != nil && os.SameFile(before, after) {
			continue
		}
		file, err := os.Open(current)
		if err != nil {
			return nil, err
		}
		err = archive.WriteHeader(&tar.Header{Name: "objects/" + object.Key, Size: after.Size(), Mode: 0600})
		if err == nil {
			_, err = io.Copy(archive, file)
		}
		err = errors.Join(err, file.Close())
		if err != nil {
			return nil, err
		}
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	if err := output.Sync(); err != nil {
		return nil, err
	}
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	success = true
	return output, nil
}

func (s *TenantStore) PublishReplicatedMaintenance(ctx context.Context, tenant, id string, input io.Reader, maxBytes int64) (Task, error) {
	task, err := s.getTaskObject(ctx, tenant, id)
	if err != nil {
		return task, err
	}
	if task.Status != TaskStatusQueued {
		return task, nil
	}
	if !PreparedMaintenanceTask(task) {
		return task, fmt.Errorf("unsupported prepared maintenance publication")
	}
	limited := &io.LimitedReader{R: input, N: maxBytes + 1}
	archive := tar.NewReader(limited)
	header, err := archive.Next()
	headerLimit := int64(1 << 20)
	if task.Type == TaskTypeGC {
		headerLimit = 16 << 20
	}
	if err != nil || header.Name != "maintenance.json" || header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > headerLimit {
		return task, fmt.Errorf("invalid prepared maintenance header")
	}
	data, err := io.ReadAll(archive)
	if err != nil {
		return task, err
	}
	var prepared preparedMaintenanceHeader
	if json.Unmarshal(data, &prepared) != nil || (prepared.Format != 1 && prepared.Format != 2) || (prepared.Format == 2) != (task.Type == TaskTypeGC) || (prepared.Format == 1 && len(prepared.Deleted) != 0) || prepared.Task.ID != id || prepared.Task.TenantID != tenant || prepared.Task.Type != task.Type || !taskTerminal(prepared.Task.Status) {
		return task, fmt.Errorf("invalid prepared maintenance identity")
	}
	token, err := s.maintenanceToken(ctx, tenant)
	if err != nil {
		return task, err
	}
	if token != prepared.BaseToken {
		return task, fmt.Errorf("%w: tenant changed during maintenance preparation", ErrConflict)
	}
	staging, err := os.MkdirTemp(s.localFileStore().root, ".maintenance-publish-")
	if err != nil {
		return task, err
	}
	defer os.RemoveAll(staging)
	keys := []string{}
	seen := map[string]bool{}
	for _, key := range prepared.Deleted {
		if !strings.HasPrefix(key, s.tenantObjectPrefix(tenant)) || validateObjectKey(key) != nil || seen[key] || key == s.taskKey(tenant, id) {
			return task, fmt.Errorf("invalid prepared maintenance deletion")
		}
		seen[key] = true
	}
	for {
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return task, err
		}
		key := strings.TrimPrefix(header.Name, "objects/")
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > maxBytes || key == header.Name || !strings.HasPrefix(key, s.tenantObjectPrefix(tenant)) || validateObjectKey(key) != nil || seen[key] {
			return task, fmt.Errorf("invalid prepared maintenance object")
		}
		seen[key] = true
		keys = append(keys, key)
		file, err := os.Create(filepath.Join(staging, fmt.Sprint(len(keys)-1)))
		if err != nil {
			return task, err
		}
		written, copyErr := io.Copy(file, archive)
		err = errors.Join(copyErr, file.Close())
		if err != nil {
			return task, err
		}
		if written != header.Size {
			return task, fmt.Errorf("truncated prepared maintenance object")
		}
	}
	if _, err := io.Copy(io.Discard, limited); err != nil {
		return task, err
	}
	if limited.N == 0 {
		return task, ErrReplicationSnapshotTooLarge
	}
	if !seen[s.taskKey(tenant, id)] {
		return task, fmt.Errorf("prepared maintenance is missing its terminal task")
	}
	mutationKeys := append(append([]string(nil), keys...), prepared.Deleted...)
	if err := s.localFileStore().journalObjects(ctx, mutationKeys); err != nil {
		return task, err
	}
	for i, key := range keys {
		file, err := os.Open(filepath.Join(staging, fmt.Sprint(i)))
		if err != nil {
			return task, err
		}
		err = errors.Join(s.localFileStore().putReader(ctx, key, file), file.Close())
		if err != nil {
			return task, err
		}
	}
	for _, key := range prepared.Deleted {
		if err := s.Objects.Delete(ctx, key); err != nil {
			return task, err
		}
	}
	s.invalidateTenantState(tenant)
	if prepared.Task.Status == TaskStatusFailed {
		if observer, ok := s.backpressureObserver.(interface{ RecordTaskFailure(string, string) }); ok {
			observer.RecordTaskFailure(tenant, task.Type)
		}
	}
	return prepared.Task, nil
}
