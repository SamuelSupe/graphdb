package ha

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"sort"
	"time"

	"github.com/SamuelSupe/graphdb/v2/internal/sharding"
	"github.com/SamuelSupe/graphdb/v2/internal/storage"
)

type tenantExport struct {
	action sharding.Action
	info   sharding.TransferInfo
	view   *storage.ObjectView
	file   *os.File
	used   time.Time
}

func (e *tenantExport) close() { e.file.Close(); e.view.Close() }

func (c *Cluster) runExportCleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.exportMu.Lock()
			c.App.mu.RLock()
			for key, entry := range c.exports {
				owner, err := c.App.ownership(ctx, entry.action.Tenant)
				if err == nil && (owner.State != "frozen" || owner.MoveID != entry.action.MoveID || owner.Epoch != entry.action.Epoch || time.Since(entry.used) > 5*time.Minute) {
					entry.close()
					delete(c.exports, key)
				}
			}
			c.App.mu.RUnlock()
			c.exportMu.Unlock()
		}
	}
}

func (c *Cluster) tenantExport(ctx context.Context, action sharding.Action) (*tenantExport, error) {
	c.App.mu.RLock()
	owner, err := c.App.ownership(ctx, action.Tenant)
	c.App.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if owner.State != "frozen" || owner.MoveID != action.MoveID || owner.Epoch != action.Epoch {
		return nil, fmt.Errorf("source is not frozen for this migration")
	}
	key := action.Tenant + "/" + action.MoveID
	if cached := c.exports[key]; cached != nil {
		cached.used = time.Now()
		return cached, nil
	}
	if len(c.exports) >= 4 {
		oldest := ""
		for id, entry := range c.exports {
			if oldest == "" || entry.used.Before(c.exports[oldest].used) {
				oldest = id
			}
		}
		c.exports[oldest].close()
		delete(c.exports, oldest)
	}
	finish := c.metrics.Start("migration_export_build")
	entry, err := c.buildTenantExport(ctx, action)
	finish(err)
	if err != nil {
		return nil, err
	}
	if c.exports == nil {
		c.exports = make(map[string]*tenantExport)
	}
	c.exports[key] = entry
	return entry, nil
}

func (c *Cluster) buildTenantExport(ctx context.Context, action sharding.Action) (_ *tenantExport, err error) {
	// Ownership is frozen. Pin only this tenant and its terminal acceptance
	// records while excluding application, then encode outside the barrier.
	c.App.mu.RLock()
	owner, err := c.App.ownership(ctx, action.Tenant)
	if err == nil && (owner.State != "frozen" || owner.MoveID != action.MoveID || owner.Epoch != action.Epoch) {
		err = fmt.Errorf("source ownership changed before export capture")
	}
	keys := []string{}
	var objects []storage.ObjectInfo
	if err == nil {
		objects, err = c.App.Files.List(ctx, path.Join(c.App.Store.Prefix, "tenants", action.Tenant)+"/")
	}
	var estimate int64
	for _, object := range objects {
		keys = append(keys, object.Key)
		estimate += object.Size*4/3 + 1024
	}
	var accepted []storage.ObjectInfo
	if err == nil {
		accepted, err = c.App.Files.List(ctx, c.App.ingestPrefix())
	}
	for _, object := range accepted {
		var record acceptedRequest
		if err == nil {
			record, err = c.App.accepted(ctx, object.Key)
		}
		if err != nil {
			break
		}
		if record.Tenant == action.Tenant {
			keys = append(keys, object.Key)
			estimate += object.Size*4/3 + 1024
		}
	}
	var generation int64
	if err == nil {
		generation, err = c.App.Store.ReplicationTenantGeneration(ctx, action.Tenant)
	}
	var tombstone []byte
	if err == nil {
		tombstone, err = c.App.Files.Get(ctx, c.App.purgeKey(action.Tenant))
		if errors.Is(err, storage.ErrNotFound) {
			err = nil
		}
	}
	var view *storage.ObjectView
	if err == nil {
		view, err = c.App.Files.CaptureObjectView(ctx, keys, c.App.MaxSnapshotBytes)
	}
	c.App.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			view.Close()
		}
	}()
	data, _ := json.Marshal(generation)
	virtual := map[string][]byte{c.App.generationKey(action.Tenant): data, c.App.purgeKey(action.Tenant): tombstone}
	for key := range virtual {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if err := c.App.Store.CheckWriteDiskSpace(ctx, max(8<<20, min(estimate+4096, c.App.MaxSnapshotBytes))); err != nil {
		return nil, err
	}
	file, err := view.CreateTemp()
	if err != nil {
		return nil, err
	}
	defer func() {
		if !success {
			file.Close()
		}
	}()
	digest := sha256.New()
	output := &transferWriter{Writer: io.MultiWriter(file, digest), remaining: c.App.MaxSnapshotBytes}
	tenant, _ := json.Marshal(action.Tenant)
	move, _ := json.Marshal(action.MoveID)
	if _, err := fmt.Fprintf(output, `{"tenant_id":%s,"move_id":%s,"objects":[`, tenant, move); err != nil {
		return nil, err
	}
	for i, key := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i > 0 {
			if _, err := io.WriteString(output, ","); err != nil {
				return nil, err
			}
		}
		encoded, _ := json.Marshal(key)
		if _, err := fmt.Fprintf(output, `{"key":%s,"data":`, encoded); err != nil {
			return nil, err
		}
		if value, ok := virtual[key]; ok {
			encoded, _ := json.Marshal(value)
			if _, err := output.Write(encoded); err != nil {
				return nil, err
			}
		} else {
			reader, err := view.Store.OpenReader(ctx, key)
			if err != nil {
				return nil, err
			}
			size, err := reader.Seek(0, io.SeekEnd)
			if err == nil {
				_, err = io.WriteString(output, `"`)
			}
			encoder := base64.NewEncoder(base64.StdEncoding, output)
			if err == nil {
				_, err = io.Copy(encoder, io.NewSectionReader(reader, 0, size))
			}
			err = errors.Join(err, encoder.Close(), reader.Close())
			if err != nil {
				return nil, err
			}
			if _, err := io.WriteString(output, `"`); err != nil {
				return nil, err
			}
		}
		if _, err := io.WriteString(output, "}"); err != nil {
			return nil, err
		}
	}
	if _, err := io.WriteString(output, "]}"); err != nil {
		return nil, err
	}
	if err := file.Sync(); err != nil {
		return nil, err
	}
	bytes := c.App.MaxSnapshotBytes - output.remaining
	entry := &tenantExport{action: action, view: view, file: file, used: time.Now(), info: sharding.TransferInfo{Bytes: bytes, Digest: fmt.Sprintf("%x", digest.Sum(nil)), Parts: int((bytes + sharding.ChunkBytes - 1) / sharding.ChunkBytes)}}
	success = true
	return entry, nil
}

type transferWriter struct {
	io.Writer
	remaining int64
}

func (w *transferWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, fmt.Errorf("tenant transfer exceeds the configured snapshot budget")
	}
	n, err := w.Writer.Write(data)
	w.remaining -= int64(n)
	return n, err
}

func (c *Cluster) exportChunk(w http.ResponseWriter, request *http.Request) {
	var action sharding.Action
	if c.App.ShardID == "" {
		http.NotFound(w, request)
		return
	}
	if json.NewDecoder(http.MaxBytesReader(w, request.Body, 4096)).Decode(&action) != nil || storage.ValidateTenantID(action.Tenant) != nil || sharding.ValidateIdentifier(action.MoveID) != nil || action.Epoch == 0 {
		http.Error(w, "invalid migration export", http.StatusBadRequest)
		return
	}
	if err := c.Node.ReadBarrier(request.Context()); err != nil {
		c.writeError(w, err)
		return
	}
	c.exportMu.Lock()
	defer c.exportMu.Unlock()
	c.App.mu.RLock()
	owner, err := c.App.ownership(request.Context(), action.Tenant)
	c.App.mu.RUnlock()
	if err != nil {
		c.writeError(w, err)
		return
	}
	if owner.State != "frozen" || owner.MoveID != action.MoveID || owner.Epoch != action.Epoch {
		http.Error(w, "source is not frozen for this migration", http.StatusConflict)
		return
	}
	entry, err := c.tenantExport(request.Context(), action)
	if err != nil {
		c.writeError(w, err)
		return
	}
	if request.URL.Path == "/cluster/export/manifest" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(entry.info)
		return
	}
	if action.Part < 0 || action.Part >= entry.info.Parts || action.Digest != entry.info.Digest {
		http.Error(w, "migration range or digest changed", http.StatusConflict)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	size := min(int64(sharding.ChunkBytes), entry.info.Bytes-int64(action.Part)*sharding.ChunkBytes)
	_, err = io.CopyN(w, io.NewSectionReader(entry.file, int64(action.Part)*sharding.ChunkBytes, size), size)
	if err == nil {
		c.metrics.Event("migration_export_chunk")
	}
}
