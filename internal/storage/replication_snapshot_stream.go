package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
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
	_, err := s.WriteToWithPayloadDigest(ctx, output)
	return err
}

// WriteToWithPayloadDigest returns the SHA-256 of the compressed archive after
// storing it in the archive's fixed-size prefix.
func (s *ReplicationSnapshotSource) WriteToWithPayloadDigest(ctx context.Context, output io.WriteSeeker) ([sha256.Size]byte, error) {
	var checksum [sha256.Size]byte
	if err := s.validateTenantGraphs(ctx); err != nil {
		return checksum, err
	}
	if _, err := output.Write(checksum[:]); err != nil {
		return checksum, err
	}
	digest := sha256.New()
	if err := writeSnapshotArchive(ctx, s.view.Store.root, s.checkpoint, s.budget, io.MultiWriter(output, digest)); err != nil {
		return checksum, err
	}
	if _, err := output.Seek(0, io.SeekStart); err != nil {
		return checksum, err
	}
	checksum = [sha256.Size]byte(digest.Sum(checksum[:0]))
	_, err := output.Write(checksum[:])
	return checksum, err
}

func (s *ReplicationSnapshotSource) Close() error {
	return s.view.Close()
}

type stagedSnapshotObject struct {
	path   string
	digest [sha256.Size]byte
}

func (s *FileStore) publishSnapshotObject(ctx context.Context, key string, object stagedSnapshotObject) error {
	return s.putReplicatedFile(ctx, key, func(filename string, journal *replicationJournal) (string, error) {
		etag := hex.EncodeToString(object.digest[:])
		if err := os.Chmod(object.path, 0o644); err != nil {
			return "", err
		}
		if err := os.Rename(object.path, filename); err != nil {
			if !errors.Is(err, syscall.EXDEV) {
				return "", err
			}
			// A mounted tenant directory may be on another filesystem.
			file, err := os.Open(object.path)
			if err != nil {
				return "", err
			}
			return etag, errors.Join(writeReaderAtomicContext(ctx, filename, file, journal, false), file.Close())
		}
		journal.deferFileSync(filename)
		journal.deferDirectorySync(filepath.Dir(filename))
		return etag, nil
	})
}

func (s *FileStore) putReader(ctx context.Context, key string, reader io.Reader) error {
	return s.putReplicatedFile(ctx, key, func(filename string, journal *replicationJournal) (string, error) {
		digest := sha256.New()
		if err := writeReaderAtomicContext(ctx, filename, io.TeeReader(reader, digest), journal, false); err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", digest.Sum(nil)), nil
	})
}

func (s *FileStore) putReplicatedFile(ctx context.Context, key string, publish func(string, *replicationJournal) (string, error)) (err error) {
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
	etag, err := publish(filename, journal)
	if err != nil {
		return err
	}
	s.changed(key, etag, false)
	return ctx.Err()
}
