package storage

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"golang.org/x/sync/semaphore"
)

var ErrDataDirectoryLocked = errors.New("data directory is in use")
var ErrFileStoreClosed = errors.New("file store is closed")

// OpenFileStore owns the entire directory until Close. All processes, including
// offline tools, must acquire this lock before accessing a live database.
func OpenFileStore(root string) (*FileStore, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := ensureDurableDirectory(root); err != nil {
		return nil, err
	}
	s := NewFileStore(root)
	if err := s.verifySafeParent(filepath.Join(root, ".graphdb.lock")); err != nil {
		return nil, err
	}
	fd, err := syscall.Open(filepath.Join(root, ".graphdb.lock"), syscall.O_CREAT|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), ".graphdb.lock")
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("%w: %s: %v", ErrDataDirectoryLocked, root, err)
	}
	s.runtime = &fileRuntime{lock: f, ioGate: semaphore.NewWeighted(directoryIOCapacity), etags: make(map[string]string), views: make(map[string]*localViewGate)}
	if err := s.recoverRestoreDirectories(); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("recover local restore: %w", err)
	}
	return s, nil
}

// Ordinary file operations take one permit; directory publication takes all
// permits. Unlike RWMutex, queued operations can honor request cancellation.
const directoryIOCapacity = math.MaxInt64

type fileRuntime struct {
	ioGate             *semaphore.Weighted
	ingestAdmissions   map[string]*localViewGate
	walGenerations     map[string]int64
	publicationMu      sync.Mutex
	pendingDirectories map[string]struct{}
	generation         uint64
	manifests          map[string]localManifestEntry
	manifestBytes      int
	closed             bool
	restoreErr         error
	active             sync.WaitGroup
	views              map[string]*localViewGate
	lock               *os.File
	mu                 sync.Mutex
	etags              map[string]string
	listeners          []func(string)
	closeOnce          sync.Once
	closeErr           error
}

func (s *FileStore) Close() error {
	if s.runtime == nil {
		return nil
	}
	s.runtime.closeOnce.Do(func() {
		s.runtime.mu.Lock()
		s.runtime.closed = true
		s.runtime.mu.Unlock()
		s.runtime.active.Wait()
		s.runtime.closeErr = errors.Join(syscall.Flock(int(s.runtime.lock.Fd()), syscall.LOCK_UN), s.runtime.lock.Close())
	})
	return s.runtime.closeErr
}

func (s *FileStore) Exclusive() bool { return s.runtime != nil }

func (s *FileStore) OnChange(listener func(string)) {
	if s.runtime == nil {
		return
	}
	s.runtime.mu.Lock()
	s.runtime.listeners = append(s.runtime.listeners, listener)
	s.runtime.mu.Unlock()
}

func (s *FileStore) changed(key, etag string) {
	if s.runtime == nil {
		return
	}
	s.runtime.mu.Lock()
	s.runtime.generation++
	delete(s.runtime.walGenerations, key)
	delete(s.runtime.etags, key)
	if entry, ok := s.runtime.manifests[key]; ok {
		s.runtime.manifestBytes -= entry.bytes
		delete(s.runtime.manifests, key)
	}
	if etag != "" {
		if len(s.runtime.etags) >= 4096 {
			clear(s.runtime.etags)
		}
		s.runtime.etags[key] = etag
	}
	listeners := append([]func(string){}, s.runtime.listeners...)
	s.runtime.mu.Unlock()
	for _, listener := range listeners {
		listener(key)
	}
}

// The cache is valid only while this process owns the directory. Every local
// mutation invalidates it while holding the same per-key lock as metadata reads.
func (s *FileStore) objectState(key, path string, hash bool) (string, bool, error) {
	if !hash || s.runtime == nil {
		return readFileStoreObjectState(path, hash)
	}
	s.runtime.mu.Lock()
	etag, ok := s.runtime.etags[key]
	s.runtime.mu.Unlock()
	if ok {
		return etag, true, nil
	}
	etag, exists, err := readFileStoreObjectState(path, true)
	if err == nil && exists {
		s.runtime.mu.Lock()
		if len(s.runtime.etags) >= 4096 {
			clear(s.runtime.etags)
		}
		s.runtime.etags[key] = etag
		s.runtime.mu.Unlock()
	}
	return etag, exists, err
}

func exclusiveFileStore(objects ObjectStore) *FileStore {
	for objects != nil {
		if file, ok := objects.(*FileStore); ok {
			if file.Exclusive() {
				return file
			}
			return nil
		}
		u, ok := objects.(objectStoreUnwrapper)
		if !ok {
			return nil
		}
		next := u.UnwrapObjectStore()
		if next == objects {
			return nil
		}
		objects = next
	}
	return nil
}

func ensureDurableDirectory(dir string) error {
	return ensureDurableDirectoryMode(dir, 0755)
}

func ensureDurableDirectoryMode(dir string, mode os.FileMode) error {
	info, err := os.Lstat(dir)
	if err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe data directory %q", dir)
		}
		if parent := filepath.Dir(dir); parent != dir {
			if err := ensureDurableDirectoryMode(parent, mode); err != nil {
				return err
			}
			return syncDir(parent)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	parent := filepath.Dir(dir)
	if err := ensureDurableDirectoryMode(parent, mode); err != nil {
		return err
	}
	if err := os.Mkdir(dir, mode); err != nil && !os.IsExist(err) {
		return err
	}
	return syncDir(parent)
}

func (s *FileStore) readMeta(ctx context.Context, key, path string) (ObjectMeta, error) {
	unlock := s.lockObject(key)
	defer unlock()
	if err := objectContextErr(ctx); err != nil {
		return ObjectMeta{Key: key}, err
	}
	etag, exists, err := s.objectState(key, path, true)
	if err != nil {
		return ObjectMeta{Key: key}, err
	}
	if !exists {
		return ObjectMeta{Key: key}, ErrNotFound
	}
	return ObjectMeta{Key: key, ETag: etag, Exists: true}, nil
}

func (s *TenantStore) localFileStore() *FileStore {
	if s == nil {
		return nil
	}
	return exclusiveFileStore(s.Objects)
}

func (s *FileStore) beginOperation(ctx context.Context) (func(), error) {
	release, err := s.beginLifecycleOperation(ctx)
	if err != nil || s.runtime == nil {
		return release, err
	}
	unlock, err := s.lockDirectoryIO(ctx)
	if err != nil {
		release()
		return nil, err
	}
	return func() { unlock(); release() }, nil
}

func (s *FileStore) lockDirectoryIO(ctx context.Context) (func(), error) {
	return s.lockDirectoryIOWeight(ctx, 1)
}

func (s *FileStore) lockDirectoryIOWeight(ctx context.Context, weight int64) (func(), error) {
	if s.runtime == nil {
		return func() {}, nil
	}
	if err := s.runtime.ioGate.Acquire(ctx, weight); err != nil {
		return nil, err
	}
	s.runtime.mu.Lock()
	operationErr := s.runtime.restoreErr
	s.runtime.mu.Unlock()
	if err := errors.Join(ctx.Err(), operationErr); err != nil {
		s.runtime.ioGate.Release(weight)
		return nil, err
	}
	return func() { s.runtime.ioGate.Release(weight) }, nil
}

func (s *FileStore) beginLifecycleOperation(ctx context.Context) (func(), error) {
	if err := objectContextErr(ctx); err != nil {
		return nil, err
	}
	if s.runtime == nil {
		return func() {}, nil
	}
	s.runtime.mu.Lock()
	defer s.runtime.mu.Unlock()
	if s.runtime.closed {
		return nil, ErrFileStoreClosed
	}
	if s.runtime.restoreErr != nil {
		return nil, s.runtime.restoreErr
	}
	s.runtime.active.Add(1)
	return s.runtime.active.Done, nil
}
