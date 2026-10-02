package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

type FileStore struct {
	diskPolicy           DiskSpacePolicy
	diskProbe            func(string) (DiskSpaceStatus, error)
	replicatedWrites     atomic.Bool
	directoryMu          sync.Mutex
	pendingDirectorySync string
	runtime              *fileRuntime
	root                 string
	lockMu               sync.Mutex
	objectLocks          map[string]*fileObjectLock
}

func (s *FileStore) Probe(ctx context.Context) error {
	// Probe only touches its own temporary file, never a published object.
	// Waiting for directory publication would turn slow maintenance into an
	// apparent disk outage and remove every healthy replica from the gateway.
	releaseOperation, operationErr := s.beginLifecycleOperation(ctx)
	if operationErr != nil {
		err := operationErr
		return err
	}
	defer releaseOperation()
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	root, err := s.path("")
	if err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("object directory %q is a symlink", root)
	}
	if !info.IsDir() {
		return fmt.Errorf("object root %q is not a directory", root)
	}
	file, err := os.CreateTemp(root, ".tmp-probe-*")
	if err != nil {
		return err
	}
	_, writeErr := file.Write([]byte{0})
	if writeErr == nil {
		writeErr = file.Sync()
	}
	return errors.Join(writeErr, file.Close(), os.Remove(file.Name()), ctx.Err())
}

func NewFileStore(root string) *FileStore {
	return &FileStore{root: root, objectLocks: map[string]*fileObjectLock{}}
}

func (s *FileStore) Get(ctx context.Context, key string) (result []byte, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	releaseOperation, operationErr := s.beginOperation(ctx, key)
	if operationErr != nil {
		err := operationErr
		return nil, err
	}
	defer releaseOperation()
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	if err := validateObjectKey(key); err != nil {
		return nil, err
	}
	path, err := s.path(key)
	if err != nil {
		return nil, err
	}
	if err := s.verifySafeParent(path); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, ErrNotFound
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return data, err
}

func (s *FileStore) GetWithMeta(ctx context.Context, key string) ([]byte, ObjectMeta, error) {
	data, err := s.Get(ctx, key)
	if err != nil {
		return nil, ObjectMeta{Key: key}, err
	}
	return data, ObjectMeta{Key: key, ETag: sha256Hex(data), Exists: true}, nil
}

func (s *FileStore) Head(ctx context.Context, key string) (result ObjectMeta, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	releaseOperation, operationErr := s.beginOperation(ctx, key)
	if operationErr != nil {
		err := operationErr
		return ObjectMeta{Key: key}, err
	}
	defer releaseOperation()
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	path, err := s.path(key)
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := s.verifySafeParent(path); err != nil {
		return ObjectMeta{Key: key}, err
	}
	return s.readMeta(ctx, key, path)
}

func (s *FileStore) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.PutConditional(ctx, key, data, PutCondition{})
	return err
}

func (s *FileStore) PutConditional(ctx context.Context, key string, data []byte, condition PutCondition) (result ObjectMeta, err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	if err := s.checkReplicatedWrite(ctx); err != nil {
		return ObjectMeta{}, err
	}
	releaseOperation, operationErr := s.beginOperation(ctx, key)
	if operationErr != nil {
		err := operationErr
		return ObjectMeta{Key: key}, err
	}
	defer releaseOperation()
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	if err := validateObjectKey(key); err != nil {
		return ObjectMeta{Key: key}, err
	}
	path, err := s.path(key)
	if err != nil {
		return ObjectMeta{}, err
	}
	unlock := s.lockObject(key)
	defer unlock()
	if err := s.ensureSafeParent(path, s.replicationJournal(ctx)); err != nil {
		return ObjectMeta{}, err
	}
	currentETag, exists, err := s.objectState(key, path, fileStorePutNeedsCurrentETag(condition))
	if err != nil {
		return ObjectMeta{}, err
	}
	if err := checkCondition(condition, currentETag, exists); err != nil {
		return ObjectMeta{Key: key, ETag: currentETag, Exists: exists}, err
	}
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key, ETag: currentETag, Exists: exists}, err
	}
	if _, batched := ctx.Value(fileBatchKey{}).(*FileStore); !batched {
		// A reused immutable file may belong to another in-flight batch. Every
		// standalone publication drains pending renames, including that batch.
		if err := s.syncPendingDirectories(); err != nil {
			return ObjectMeta{}, err
		}
	}
	etag := ""
	defer func() { s.changed(key, etag) }()
	if err := s.journalObject(ctx, key); err != nil {
		return ObjectMeta{}, err
	}
	journal := s.replicationJournal(ctx)
	if err := writeFileAtomicContext(ctx, path, data, journal); err != nil {
		return ObjectMeta{}, err
	}
	etag = sha256Hex(data)
	return ObjectMeta{Key: key, ETag: etag, Exists: true}, nil
}

func (s *FileStore) Delete(ctx context.Context, key string) error {
	return s.DeleteConditional(ctx, key, PutCondition{})
}

func (s *FileStore) DeleteConditional(ctx context.Context, key string, condition PutCondition) (err error) {
	defer func() { recordReplicationFailure(ctx, err) }()
	if err := s.checkReplicatedWrite(ctx); err != nil {
		return err
	}
	releaseOperation, operationErr := s.beginOperation(ctx, key)
	if operationErr != nil {
		err := operationErr
		return err
	}
	defer releaseOperation()
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if err := validateObjectKey(key); err != nil {
		return err
	}
	path, err := s.path(key)
	if err != nil {
		return err
	}
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	unlock := s.lockObject(key)
	defer unlock()
	if err := s.verifySafeParent(path); err != nil {
		if errors.Is(err, ErrNotFound) {
			if checkErr := checkCondition(condition, "", false); checkErr != nil {
				return checkErr
			}
			return nil
		}
		return err
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if checkErr := checkCondition(condition, "", false); checkErr != nil {
			return checkErr
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		if checkErr := checkCondition(condition, "", false); checkErr != nil {
			return checkErr
		}
		return nil
	}
	currentETag, exists, err := s.objectState(key, path, condition.IfMatch != "")
	if err != nil {
		return err
	}
	if err := checkCondition(condition, currentETag, exists); err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := objectContextErr(ctx); err != nil {
		return err
	}
	if view, ok := ctx.Value(gcViewKey{}).(*gcView); ok && !view.canDelete(key) {
		return errGCViewPinned
	}
	defer s.changed(key, "")
	journal := s.replicationJournal(ctx)
	files, batched := ctx.Value(fileBatchKey{}).(*FileStore)
	batched = batched && files == s && journal == nil
	if batched {
		s.runtime.publicationMu.Lock()
		defer s.runtime.publicationMu.Unlock()
	}
	if err := s.journalObject(ctx, key); err != nil {
		return err
	}
	err = os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err == nil {
		if journal != nil {
			journal.forgetFileSync(path)
			journal.deferDirectorySync(filepath.Dir(path))
			return nil
		}
		if batched {
			if s.runtime.pendingDirectories == nil {
				s.runtime.pendingDirectories = make(map[string]struct{})
			}
			s.runtime.pendingDirectories[filepath.Dir(path)] = struct{}{}
			return nil
		}
		return syncDir(filepath.Dir(path))
	}
	return err
}

func (s *FileStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	items, _, err := s.ListPage(ctx, prefix, "", 0)
	return items, err
}

func (s *FileStore) listWalkRoot(root string, prefix string) (string, error) {
	if prefix == "" {
		return root, nil
	}
	if strings.Contains(prefix, "\\") || filepath.IsAbs(filepath.FromSlash(prefix)) {
		return "", fmt.Errorf("invalid object prefix %q", prefix)
	}
	cleanPrefix := strings.Trim(prefix, "/")
	if cleanPrefix == "" {
		return root, nil
	}
	parts := strings.Split(cleanPrefix, "/")
	for _, part := range parts {
		if part == "." || part == ".." || part == "" {
			return "", fmt.Errorf("invalid object prefix %q", prefix)
		}
	}
	base := cleanPrefix
	if !strings.HasSuffix(prefix, "/") {
		base = pathPrefixDir(cleanPrefix)
	}
	if base == "" {
		return root, nil
	}
	return filepath.Join(root, filepath.FromSlash(base)), nil
}

func pathPrefixDir(prefix string) string {
	index := strings.LastIndex(prefix, "/")
	if index < 0 {
		return ""
	}
	return prefix[:index]
}

func (s *FileStore) path(key string) (string, error) {
	if err := validateFileStoreKey(key); err != nil {
		return "", err
	}
	clean := filepath.Clean(filepath.FromSlash(key))
	if clean == "." {
		clean = ""
	}
	if filepath.IsAbs(clean) {
		return "", fmt.Errorf("invalid object key %q", key)
	}
	return filepath.Join(s.root, clean), nil
}

func validateFileStoreKey(key string) error {
	if key == "" {
		return nil
	}
	if key == fileRestoreDirectory || strings.HasPrefix(key, fileRestoreDirectory+"/") || key == replicationDirectory || strings.HasPrefix(key, replicationDirectory+"/") || key == ".graphdb-raft" || strings.HasPrefix(key, ".graphdb-raft/") {
		return fmt.Errorf("reserved object key %q", key)
	}
	if strings.Contains(key, "\\") || filepath.IsAbs(filepath.FromSlash(key)) {
		return fmt.Errorf("invalid object key %q", key)
	}
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("invalid object key %q", key)
		}
	}
	return nil
}

func writeFileAtomic(path string, data []byte) error {
	return writeFileAtomicContext(context.Background(), path, data, nil)
}

func writeFileAtomicContext(ctx context.Context, path string, data []byte, journal *replicationJournal) error {
	if bytes, ok := ctx.Value(fileBatchBytesKey{}).(*atomic.Int64); ok {
		bytes.Add(int64(len(data)))
	}
	return writeReaderAtomicContext(ctx, path, bytes.NewReader(data), journal)
}

func writeReaderAtomicContext(ctx context.Context, path string, reader io.Reader, journal *replicationJournal) error {
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(path)+"-")
	if err != nil {
		return err
	}
	temp := file.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(temp)
		}
	}()
	if _, err := io.Copy(file, reader); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Chmod(0o644); err != nil {
		_ = file.Close()
		return err
	}
	if journal == nil {
		if err := syncStorageFile(file); err != nil {
			_ = file.Close()
			return err
		}
	}
	if err := file.Close(); err != nil {
		return err
	}
	files, batched := ctx.Value(fileBatchKey{}).(*FileStore)
	batched = batched && journal == nil
	if batched {
		files.runtime.publicationMu.Lock()
		defer files.runtime.publicationMu.Unlock()
	}
	if err := os.Rename(temp, path); err != nil {
		return err
	}
	cleanup = false
	if journal != nil {
		journal.deferFileSync(path)
		journal.deferDirectorySync(dir)
		return nil
	}
	if batched {
		if files.runtime.pendingDirectories == nil {
			files.runtime.pendingDirectories = make(map[string]struct{})
		}
		files.runtime.pendingDirectories[dir] = struct{}{}
		return nil
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return syncStorageFile(file)
}

// Dot-prefixed business names are valid. Only runtime-owned root paths and
// unfinished atomic writes are excluded from a complete application snapshot.
func fileStoreInternalPath(key string) bool {
	root, _, _ := strings.Cut(key, "/")
	if root == replicationDirectory || root == fileRestoreDirectory || root == ".graphdb-raft" || root == ".graphdb.lock" || root == ".graphdb-runtime-restore.json" {
		return true
	}
	for _, prefix := range []string{".snapshot-view-", ".graphdb-replication-snapshot-", ".maintenance-", ".runtime-restore-"} {
		if strings.HasPrefix(root, prefix) {
			return true
		}
	}
	return false
}

func isFileStoreTemp(path string) bool {
	name := filepath.Base(path)
	return (strings.HasPrefix(name, ".tmp-") && !strings.HasSuffix(name, ".parquet")) || name == ".graphdb.lock"
}

func fileStorePutNeedsCurrentETag(condition PutCondition) bool {
	return condition.IfMatch != "" || condition.IfNoneMatch
}
