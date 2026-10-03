package ha

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/url"
	"path"

	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

const restoreChunkBytes = 1 << 20

type restoreManifest struct {
	Maintenance bool   `json:"maintenance,omitempty"`
	Bytes       int64  `json:"bytes"`
	SHA256      string `json:"sha256"`
	Generation  int64  `json:"generation,omitempty"`
}

type restorePart struct {
	restoreManifest
	Part int64 `json:"part"`
}

func restoreTask(task storage.Task) bool {
	return task.Type == storage.TaskTypeTenantRestore || task.Type == storage.TaskTypeTenantRestoreDrill
}

func transferTask(task storage.Task) bool {
	return restoreTask(task) || storage.PreparedMaintenanceTask(task)
}

func (a *Application) restorePrefix(tenant, id string) string {
	return path.Join(a.Store.Prefix, "control", "restore-staging", url.PathEscape(tenant), url.PathEscape(id)) + "/"
}

func restorePartKey(prefix string, part int64) string {
	return fmt.Sprintf("%s%08d", prefix, part)
}

func (a *Application) clearRestore(ctx context.Context, tenant, id string) error {
	objects, err := a.Files.List(ctx, a.restorePrefix(tenant, id))
	if err != nil {
		return err
	}
	for _, object := range objects {
		if err := a.Files.Delete(ctx, object.Key); err != nil {
			return err
		}
	}
	return nil
}

func (a *Application) stageRestore(ctx context.Context, cmd command) ([]byte, error) {
	if len(cmd.IDs) != 1 {
		return nil, fmt.Errorf("invalid restore task identity")
	}
	task, err := a.Store.GetTask(ctx, cmd.Tenant, cmd.IDs[0])
	if err != nil {
		return nil, err
	}
	if task.Status != storage.TaskStatusQueued {
		return nil, a.clearRestore(ctx, cmd.Tenant, task.ID)
	}
	var part restorePart
	if err := json.Unmarshal(cmd.Body, &part); err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(part.SHA256)
	if !transferTask(task) || err != nil || len(digest) != sha256.Size || part.Bytes <= 0 ||
		part.Bytes > a.MaxSnapshotBytes || part.Part < 0 || part.Part >= (part.Bytes+restoreChunkBytes-1)/restoreChunkBytes ||
		int64(len(cmd.Restore)) != min(restoreChunkBytes, part.Bytes-part.Part*restoreChunkBytes) {
		return nil, fmt.Errorf("invalid restore part")
	}
	prefix := a.restorePrefix(cmd.Tenant, task.ID)
	manifest, err := json.Marshal(part.restoreManifest)
	if err != nil {
		return nil, err
	}
	previous, err := a.Files.Get(ctx, prefix+"manifest.json")
	if errors.Is(err, storage.ErrNotFound) && part.Part == 0 {
		if err := a.Files.Put(ctx, prefix+"manifest.json", manifest); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else if !bytes.Equal(previous, manifest) {
		return nil, fmt.Errorf("restore input changed during transfer")
	}
	key := restorePartKey(prefix, part.Part)
	previous, err = a.Files.Get(ctx, key)
	if err == nil {
		if !bytes.Equal(previous, cmd.Restore) {
			return nil, fmt.Errorf("restore part conflicts with persisted input")
		}
		return nil, nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return nil, err
	}
	return nil, a.Files.Put(ctx, key, cmd.Restore)
}

func (a *Application) runStagedRestore(ctx context.Context, cmd command) (storage.Task, error) {
	task, err := a.Store.GetTask(ctx, cmd.Tenant, cmd.IDs[0])
	if err != nil {
		return task, err
	}
	if task.Status != storage.TaskStatusQueued {
		return task, a.clearRestore(ctx, cmd.Tenant, task.ID)
	}
	var manifest restoreManifest
	if err := json.Unmarshal(cmd.Body, &manifest); err != nil {
		return task, err
	}
	prefix := a.restorePrefix(cmd.Tenant, task.ID)
	persisted, err := a.Files.Get(ctx, prefix+"manifest.json")
	if err != nil {
		return task, err
	}
	var expected restoreManifest
	if json.Unmarshal(persisted, &expected) != nil || expected != manifest || !transferTask(task) || manifest.Maintenance != storage.PreparedMaintenanceTask(task) {
		return task, fmt.Errorf("restore manifest mismatch")
	}
	source := &restoreReader{ctx: ctx, app: a, prefix: prefix, manifest: manifest}
	digest := sha256.New()
	size, err := io.Copy(digest, source)
	err = errors.Join(err, source.Close())
	if err != nil {
		return task, err
	}
	if size != manifest.Bytes || hex.EncodeToString(digest.Sum(nil)) != manifest.SHA256 {
		return task, fmt.Errorf("restore transfer integrity failed")
	}
	source = &restoreReader{ctx: ctx, app: a, prefix: prefix, manifest: manifest}
	if manifest.Maintenance {
		task, err = a.Store.PublishReplicatedMaintenance(ctx, cmd.Tenant, task.ID, source, a.MaxSnapshotBytes)
		if errors.Is(err, storage.ErrConflict) {
			err = nil
		}
	} else {
		task, err = a.Store.RunReplicatedTaskFromReader(ctx, cmd.Tenant, task.ID, source)
	}
	err = errors.Join(err, source.Close())
	if err != nil {
		return task, err
	}
	return task, a.clearRestore(ctx, cmd.Tenant, task.ID)
}

type restoreReader struct {
	ctx            context.Context
	app            *Application
	prefix         string
	manifest       restoreManifest
	part           int64
	current        io.ReadCloser
	partKey        func(int64) string
	partDigests    []string
	digest         hash.Hash
	expectedDigest string
	readError      error
}

func (r *restoreReader) Read(buffer []byte) (n int, err error) {
	defer func() {
		if err != nil && !errors.Is(err, io.EOF) {
			r.readError = err
		}
	}()
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		if r.current == nil {
			remaining := r.manifest.Bytes - r.part*restoreChunkBytes
			if remaining <= 0 {
				return 0, io.EOF
			}
			key := restorePartKey(r.prefix, r.part)
			if r.partKey != nil {
				key = r.partKey(r.part)
			}
			file, err := r.app.Files.OpenReader(r.ctx, key)
			if err != nil {
				return 0, err
			}
			size, err := file.Seek(0, io.SeekEnd)
			if err == nil && size != min(restoreChunkBytes, remaining) {
				err = fmt.Errorf("restore part size mismatch")
			}
			if err == nil {
				_, err = file.Seek(0, io.SeekStart)
			}
			if err != nil {
				return 0, errors.Join(err, file.Close())
			}
			var source io.Reader = io.NewSectionReader(file, 0, size)
			if r.part < int64(len(r.partDigests)) && r.partDigests[r.part] != "" {
				r.digest = sha256.New()
				r.expectedDigest = r.partDigests[r.part]
				source = io.TeeReader(source, r.digest)
			}
			r.current = &sectionReadCloser{Reader: source, Closer: file}
			r.part++
		}
		n, err = r.current.Read(buffer)
		if err == io.EOF {
			var integrityErr error
			if r.digest != nil && hex.EncodeToString(r.digest.Sum(nil)) != r.expectedDigest {
				integrityErr = fmt.Errorf("persisted migration chunk %d integrity failed", r.part-1)
			}
			err = errors.Join(integrityErr, r.Close())
			if n == 0 && err == nil {
				continue
			}
		}
		return n, err
	}
}

type sectionReadCloser struct {
	io.Reader
	io.Closer
}

func (r *restoreReader) Close() error {
	if r.current == nil {
		return nil
	}
	err := r.current.Close()
	r.current = nil
	r.digest, r.expectedDigest = nil, ""
	return err
}

func (a *Application) stagedRestoreReady(ctx context.Context, task storage.Task) (restoreManifest, bool, error) {
	prefix := a.restorePrefix(task.TenantID, task.ID)
	data, err := a.Files.Get(ctx, prefix+"manifest.json")
	if errors.Is(err, storage.ErrNotFound) {
		return restoreManifest{}, false, nil
	}
	if err != nil {
		return restoreManifest{}, false, err
	}
	var manifest restoreManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return manifest, false, err
	}
	if manifest.Bytes <= 0 || manifest.Bytes > a.MaxSnapshotBytes {
		return manifest, false, fmt.Errorf("invalid staged restore size")
	}
	for part := int64(0); part*restoreChunkBytes < manifest.Bytes; part++ {
		file, err := a.Files.OpenReader(ctx, restorePartKey(prefix, part))
		if errors.Is(err, storage.ErrNotFound) {
			return manifest, false, nil
		}
		if err != nil {
			return manifest, false, err
		}
		size, err := file.Seek(0, io.SeekEnd)
		err = errors.Join(err, file.Close())
		if err != nil {
			return manifest, false, err
		}
		if size != min(restoreChunkBytes, manifest.Bytes-part*restoreChunkBytes) {
			return manifest, false, fmt.Errorf("staged restore part size mismatch")
		}
	}
	return manifest, true, nil
}

func (c *Cluster) publishRestore(ctx context.Context, task storage.Task, manifest restoreManifest) error {
	cmd, err := newCommand("task")
	if err != nil {
		return err
	}
	cmd.Tenant, cmd.IDs, cmd.ExpectedGeneration = task.TenantID, []string{task.ID}, manifest.Generation
	cmd.Body, err = json.Marshal(manifest)
	if err != nil {
		return err
	}
	data, err := c.propose(ctx, cmd)
	if err == nil && manifest.Maintenance {
		var result storage.Task
		if err = json.Unmarshal(data, &result); err == nil && result.ID == task.ID && result.Status == storage.TaskStatusQueued {
			return storage.ErrConflict
		}
	}
	return err
}

func (c *Cluster) replicateRestore(ctx context.Context, task storage.Task, input []byte, generation int64) error {
	digest := sha256.Sum256(input)
	manifest := restoreManifest{Bytes: int64(len(input)), SHA256: hex.EncodeToString(digest[:]), Generation: generation}
	c.App.mu.RLock()
	prefix := c.App.restorePrefix(task.TenantID, task.ID)
	previous, err := c.App.Files.Get(ctx, prefix+"manifest.json")
	var persisted restoreManifest
	if err == nil && (json.Unmarshal(previous, &persisted) != nil || persisted != manifest) {
		err = fmt.Errorf("restore input changed during transfer")
	} else if errors.Is(err, storage.ErrNotFound) {
		err = nil
		objects, listErr := c.App.Files.List(ctx, "")
		if listErr != nil {
			err = listErr
		} else {
			// Leave room for tar headers, padding and the staged part manifest.
			total := manifest.Bytes + ((manifest.Bytes+restoreChunkBytes-1)/restoreChunkBytes)*1024 + 4096
			for _, object := range objects {
				total += object.Size + 1024
			}
			if total > c.App.MaxSnapshotBytes {
				err = fmt.Errorf("restore staging exceeds snapshot byte budget; increase GRAPHDB_RAFT_MAX_SNAPSHOT_BYTES on every replica")
			}
		}
	}
	c.App.mu.RUnlock()
	if err != nil {
		message := err.Error()
		cmd, err := newCommand("task")
		if err != nil {
			return err
		}
		cmd.Tenant, cmd.IDs, cmd.Error = task.TenantID, []string{task.ID}, message
		cmd.ExpectedGeneration = generation
		_, err = c.propose(ctx, cmd)
		return err
	}
	for offset := int64(0); offset < manifest.Bytes; offset += restoreChunkBytes {
		part := offset / restoreChunkBytes
		data := input[offset:min(offset+restoreChunkBytes, manifest.Bytes)]
		c.App.mu.RLock()
		previous, err := c.App.Files.Get(ctx, restorePartKey(prefix, part))
		c.App.mu.RUnlock()
		if err == nil && bytes.Equal(previous, data) {
			continue
		}
		if err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
		cmd, err := newCommand("restore_part")
		if err != nil {
			return err
		}
		cmd.Tenant, cmd.IDs, cmd.Restore = task.TenantID, []string{task.ID}, data
		cmd.ExpectedGeneration = generation
		cmd.Body, err = json.Marshal(restorePart{restoreManifest: manifest, Part: part})
		if err != nil {
			return err
		}
		if _, err := c.propose(ctx, cmd); err != nil {
			return err
		}
	}
	return c.publishRestore(ctx, task, manifest)
}
