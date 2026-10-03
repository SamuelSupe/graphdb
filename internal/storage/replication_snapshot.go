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
	return replicationSnapshotBytes(ctx, s.root, checkpoint, budget)
}

func replicationSnapshotBytes(ctx context.Context, root string, checkpoint ReplicationCheckpoint, budget int64) ([]byte, error) {
	var raw bytes.Buffer
	if err := writeSnapshotArchive(ctx, root, checkpoint, budget, &raw); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw.Bytes())
	return append(sum[:], raw.Bytes()...), nil
}

func writeSnapshotArchive(ctx context.Context, root string, checkpoint ReplicationCheckpoint, budget int64, output io.Writer) error {
	compressed := gzip.NewWriter(output)
	archive := tar.NewWriter(&snapshotBudgetWriter{writer: compressed, remaining: budget})
	metadata, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	if err := archive.WriteHeader(&tar.Header{Name: "checkpoint.json", Mode: 0600, Size: int64(len(metadata))}); err != nil {
		return err
	}
	if _, err := archive.Write(metadata); err != nil {
		return err
	}
	err = filepath.WalkDir(root, func(filename string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if filename == root {
			return nil
		}
		relative, err := filepath.Rel(root, filename)
		if err != nil {
			return err
		}
		if fileStoreInternalPath(filepath.ToSlash(relative)) || (!entry.IsDir() && isFileStoreTemp(filename)) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("replication snapshot contains non-regular file %s", filename)
		}
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if err := archive.WriteHeader(&tar.Header{Name: "objects/" + filepath.ToSlash(relative), Mode: 0600, Size: info.Size()}); err != nil {
			return err
		}
		_, err = io.Copy(archive, io.NewSectionReader(file, 0, info.Size()))
		return err
	})
	if err != nil {
		return err
	}
	if err := archive.Close(); err != nil {
		return err
	}
	return compressed.Close()
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

func (s *FileStore) InstallReplicationSnapshot(ctx context.Context, index uint64, data []byte, maxBytes int64, tenantPrefix ...string) error {
	return s.InstallReplicationSnapshotReader(ctx, index, bytes.NewReader(data), maxBytes, tenantPrefix...)
}

// InstallReplicationSnapshotReader validates the archive before changing live
// objects. An optional tenant prefix also cold-validates published graphs and
// relation schemas in the decoded input; an empty prefix retains generic storage.
func (s *FileStore) InstallReplicationSnapshotReader(ctx context.Context, index uint64, source io.ReadSeeker, maxBytes int64, tenantPrefix ...string) error {
	var checksum [sha256.Size]byte
	if _, err := io.ReadFull(source, checksum[:]); err != nil {
		return fmt.Errorf("truncated replication snapshot: %w", err)
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, io.LimitReader(source, maxBytes+(32<<20)+1)); err != nil {
		return err
	}
	if !bytes.Equal(digest.Sum(nil), checksum[:]) {
		return fmt.Errorf("replication snapshot checksum mismatch")
	}
	if _, err := source.Seek(sha256.Size, io.SeekStart); err != nil {
		return err
	}
	compressed, err := gzip.NewReader(source)
	if err != nil {
		return err
	}
	defer compressed.Close()
	limited := &io.LimitedReader{R: compressed, N: maxBytes + 1}
	archive := tar.NewReader(limited)
	staging, err := os.MkdirTemp(s.root, ".graphdb-replication-snapshot-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	objects := make(map[string]string)
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
		total += header.Size
		if header.Name == "checkpoint.json" {
			if metadataFound {
				return fmt.Errorf("duplicate snapshot checkpoint")
			}
			metadataFound = true
			body, err := io.ReadAll(io.LimitReader(archive, header.Size))
			if err != nil || int64(len(body)) != header.Size {
				return fmt.Errorf("truncated snapshot checkpoint")
			}
			if err := json.Unmarshal(body, &checkpoint); err != nil {
				return err
			}
			continue
		}
		if !strings.HasPrefix(header.Name, "objects/") {
			return fmt.Errorf("invalid snapshot entry %s", header.Name)
		}
		key := strings.TrimPrefix(header.Name, "objects/")
		if err := validateFileStoreKey(key); err != nil {
			return err
		}
		if fileStoreInternalPath(key) || isFileStoreTemp(key) {
			return fmt.Errorf("reserved snapshot object %s", key)
		}
		if _, exists := objects[key]; exists {
			return fmt.Errorf("duplicate snapshot object")
		}
		filename := filepath.Join(staging, fmt.Sprint(len(objects)))
		file, err := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		written, copyErr := io.Copy(file, archive)
		closeErr := file.Close()
		if copyErr != nil || written != header.Size {
			return fmt.Errorf("truncated snapshot object")
		}
		if closeErr != nil {
			return closeErr
		}
		objects[key] = filename
	}
	// Consume padding and verify gzip's checksum before changing live data.
	if _, err := io.Copy(io.Discard, limited); err != nil {
		return err
	}
	if limited.N == 0 {
		return ErrReplicationSnapshotTooLarge
	}
	if !metadataFound || checkpoint.Index != index {
		return fmt.Errorf("snapshot applied position mismatch")
	}
	if len(tenantPrefix) > 0 && tenantPrefix[0] != "" {
		view := NewFileStore(filepath.Join(staging, "view"))
		for key, filename := range objects {
			destination := filepath.Join(view.root, key)
			if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
				return err
			}
			if err := os.Link(filename, destination); err != nil {
				return err
			}
		}
		if err := validateReplicationSnapshotTenantGraphs(ctx, view, tenantPrefix[0]); err != nil {
			return err
		}
	}
	_, err = s.ApplyReplicated(ctx, index, "snapshot", time.Time{}, func(applyCtx context.Context) ([]byte, error) {
		existing, err := s.List(applyCtx, "")
		if err != nil {
			return nil, err
		}
		for _, object := range existing {
			if fileStoreInternalPath(object.Key) {
				continue
			}
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
			file, err := os.Open(objects[key])
			if err != nil {
				return nil, err
			}
			putErr := s.putReader(applyCtx, key, file)
			closeErr := file.Close()
			if err := errors.Join(putErr, closeErr); err != nil {
				return nil, err
			}
		}
		return checkpoint.Response, nil
	})
	return err
}
