package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ReplicationSnapshotSource struct {
	// TenantPrefix enables cold graph validation in the captured view before
	// encoding. It does not limit which objects the replication snapshot includes.
	Validate       func(context.Context, ObjectStore) error
	TenantPrefix   string
	view           *ObjectView
	checkpoint     ReplicationCheckpoint
	budget         int64
	estimatedBytes int64
}

// CaptureReplicationSnapshot pins immutable inodes at one application position.
// The caller excludes application until capture returns; compression can then
// run while later publications replace or unlink those inodes.
func (s *FileStore) CaptureReplicationSnapshot(ctx context.Context, budget int64) (*ReplicationSnapshotSource, error) {
	// Directory cleanup can run between applications. Keep its removals out
	// of this walk until every live inode has been linked into the view.
	if s.runtime != nil {
		s.runtime.replicationMu.Lock()
		defer s.runtime.replicationMu.Unlock()
	}
	view, err := s.newObjectView(ctx)
	if err != nil {
		return nil, err
	}
	checkpoint, err := s.ReplicationCheckpoint()
	if err != nil {
		view.Close()
		return nil, err
	}
	dir := view.Store.root
	if err := os.Mkdir(dir, 0700); err != nil {
		view.Close()
		return nil, err
	}
	source := &ReplicationSnapshotSource{view: view, checkpoint: checkpoint, budget: budget}
	var total int64
	preparedParents := make(map[string]bool)
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
			return fmt.Errorf("snapshot object is not a regular file: %s", filename)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if info.Size() > budget-total {
			return ErrReplicationSnapshotTooLarge
		}
		total += info.Size()
		source.estimatedBytes += 512 + (info.Size()+511)/512*512
		destination := filepath.Join(dir, relative)
		parent := filepath.Dir(filename)
		if !preparedParents[parent] {
			// GC leaves empty version directories behind. Archives contain files,
			// so only validate and recreate parents that still contain an object.
			if err := s.verifySafeParent(filename); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
				return err
			}
			preparedParents[parent] = true
		}
		return os.Link(filename, destination)
	})
	if err != nil {
		source.Close()
		return nil, err
	}
	if err := s.checkDiskSpace(ctx, max(8<<20, total+(1<<20)), false); err != nil {
		source.Close()
		return nil, err
	}
	return source, nil
}

func (s *ReplicationSnapshotSource) SnapshotBytes() int64 { return s.estimatedBytes + (1 << 20) }

// Bytes encodes the captured application position in the legacy snapshot format.
func (s *ReplicationSnapshotSource) Bytes(ctx context.Context) ([]byte, error) {
	if err := s.validateTenantGraphs(ctx); err != nil {
		return nil, err
	}
	return replicationSnapshotBytes(ctx, s.view.Store.root, s.checkpoint, s.budget)
}

func (s *ReplicationSnapshotSource) WriteTo(ctx context.Context, output io.WriteSeeker) error {
	if err := s.validateTenantGraphs(ctx); err != nil {
		return err
	}
	if _, err := output.Write(make([]byte, sha256.Size)); err != nil {
		return err
	}
	digest := sha256.New()
	if err := writeSnapshotArchive(ctx, s.view.Store.root, s.checkpoint, s.budget, io.MultiWriter(output, digest)); err != nil {
		return err
	}
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		return err
	}
	_, err := output.Write(digest.Sum(nil))
	return err
}

func (s *ReplicationSnapshotSource) Close() error {
	return s.view.Close()
}

func (s *FileStore) putReader(ctx context.Context, key string, reader io.Reader) (err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	if err := s.checkReplicatedWrite(ctx); err != nil {
		return err
	}
	release, err := s.beginOperation(ctx, key)
	if err != nil {
		return err
	}
	defer release()
	if err := validateObjectKey(key); err != nil {
		return err
	}
	filename, err := s.path(key)
	if err != nil {
		return err
	}
	unlock := s.lockObject(key)
	defer unlock()
	journal := s.replicationJournal(ctx)
	if err := s.ensureSafeParent(filename, journal); err != nil {
		return err
	}
	if err := s.journalObject(ctx, key); err != nil {
		return err
	}
	digest := sha256.New()
	if err := writeReaderAtomicContext(ctx, filename, io.TeeReader(reader, digest), journal, false); err != nil {
		return err
	}
	s.changed(key, fmt.Sprintf("%x", digest.Sum(nil)), false)
	return ctx.Err()
}
