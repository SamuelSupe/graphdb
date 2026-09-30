package storage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var ErrReplicationSnapshotTooLarge = errors.New("replication snapshot exceeds uncompressed byte budget")

// Snapshots include control records, queued work and idempotency history, in
// addition to the graph. They are independent of S3 tenant backup records.
func (s *FileStore) ReplicationSnapshot(ctx context.Context, budgets ...int64) ([]byte, error) {
	checkpoint, err := s.ReplicationCheckpoint()
	if err != nil {
		return nil, err
	}
	budget := int64(512 << 20)
	if len(budgets) > 0 {
		budget = budgets[0]
	}
	var total int64
	var raw bytes.Buffer
	compressed := gzip.NewWriter(&raw)
	archive := tar.NewWriter(&snapshotBudgetWriter{writer: compressed, remaining: budget})
	metadata, err := json.Marshal(checkpoint)
	if err != nil {
		return nil, err
	}
	if err := archive.WriteHeader(&tar.Header{Name: "checkpoint.json", Mode: 0600, Size: int64(len(metadata))}); err != nil {
		return nil, err
	}
	if _, err := archive.Write(metadata); err != nil {
		return nil, err
	}
	err = filepath.WalkDir(s.root, func(filename string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if filename == s.root {
			return nil
		}
		relative, err := filepath.Rel(s.root, filename)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(relative)
		if strings.HasPrefix(entry.Name(), ".") {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("replication snapshot contains non-regular file %s", key)
		}
		data, err := s.Get(ctx, key)
		if err != nil {
			return err
		}
		total += int64(len(data))
		if total > budget {
			return ErrReplicationSnapshotTooLarge
		}
		if err := archive.WriteHeader(&tar.Header{Name: "objects/" + key, Mode: 0600, Size: int64(len(data))}); err != nil {
			return err
		}
		_, err = archive.Write(data)
		return err
	})
	if err != nil {
		return nil, err
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	if err := compressed.Close(); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw.Bytes())
	return append(sum[:], raw.Bytes()...), nil
}

type snapshotBudgetWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *snapshotBudgetWriter) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		return 0, ErrReplicationSnapshotTooLarge
	}
	n, err := w.writer.Write(data)
	w.remaining -= int64(n)
	return n, err
}

func (s *FileStore) InstallReplicationSnapshot(ctx context.Context, index uint64, data []byte, maxBytes int64) error {
	if len(data) < 32 {
		return fmt.Errorf("truncated replication snapshot")
	}
	sum := sha256.Sum256(data[32:])
	if !bytes.Equal(sum[:], data[:32]) {
		return fmt.Errorf("replication snapshot checksum mismatch")
	}
	compressed, err := gzip.NewReader(bytes.NewReader(data[32:]))
	if err != nil {
		return err
	}
	defer compressed.Close()
	archive := tar.NewReader(io.LimitReader(compressed, maxBytes+1))
	objects := make(map[string][]byte)
	var checkpoint ReplicationCheckpoint
	var total int64
	metadataFound := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		header, err := archive.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg || header.Size < 0 || header.Size > maxBytes-total {
			return fmt.Errorf("invalid or oversized replication snapshot entry")
		}
		body, err := io.ReadAll(io.LimitReader(archive, header.Size))
		if err != nil || int64(len(body)) != header.Size {
			return fmt.Errorf("truncated snapshot object")
		}
		total += header.Size
		if header.Name == "checkpoint.json" {
			if metadataFound {
				return fmt.Errorf("duplicate snapshot checkpoint")
			}
			metadataFound = true
			if err := json.Unmarshal(body, &checkpoint); err != nil {
				return err
			}
			continue
		}
		if !strings.HasPrefix(header.Name, "objects/") {
			return fmt.Errorf("invalid snapshot entry %s", header.Name)
		}
		key := strings.TrimPrefix(header.Name, "objects/")
		if err := validateObjectKey(key); err != nil {
			return err
		}
		for _, segment := range strings.Split(key, "/") {
			if strings.HasPrefix(segment, ".") {
				return fmt.Errorf("reserved snapshot object %s", key)
			}
		}
		if _, exists := objects[key]; exists {
			return fmt.Errorf("duplicate snapshot object")
		}
		objects[key] = body
	}
	if !metadataFound || checkpoint.Index != index {
		return fmt.Errorf("snapshot applied position mismatch")
	}
	_, err = s.ApplyReplicated(ctx, index, "snapshot", time.Time{}, func(applyCtx context.Context) ([]byte, error) {
		existing, err := s.List(applyCtx, "")
		if err != nil {
			return nil, err
		}
		for _, object := range existing {
			if _, keep := objects[object.Key]; !keep {
				if err := s.Delete(applyCtx, object.Key); err != nil {
					return nil, err
				}
			}
		}
		keys := make([]string, 0, len(objects))
		for key := range objects {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := s.Put(applyCtx, key, objects[key]); err != nil {
				return nil, err
			}
		}
		return checkpoint.Response, nil
	})
	return err
}
