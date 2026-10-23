package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var errFileStoreKey = fmt.Errorf("%w: invalid local object path", ErrInvalidArgument)

type preparedFileParentsKey struct{}

type preparedFileParents struct {
	files   *FileStore
	mu      sync.Mutex
	checked map[string]bool
}

func (s *FileStore) verifySafeMutationParent(ctx context.Context, filename string) error {
	parents, ok := ctx.Value(preparedFileParentsKey{}).(*preparedFileParents)
	if !ok || parents.files != s {
		return s.verifySafeParent(filename)
	}
	parents.mu.Lock()
	defer parents.mu.Unlock()
	parent := filepath.Dir(filename)
	if parents.checked[parent] {
		return nil
	}
	if err := s.verifySafeParent(filename); err != nil {
		return err
	}
	parents.checked[parent] = true
	return nil
}

func (s *FileStore) verifySafeParent(filePath string) error {
	return s.walkSafeDir(filepath.Dir(filePath), false, nil)
}

func (s *FileStore) ensureSafeParent(filePath string, journal *replicationJournal) error {
	return s.ensureSafeDirectory(filepath.Dir(filePath), journal)
}

func (s *FileStore) ensureSafeDirectory(dir string, journal *replicationJournal) error {
	// Standalone writers require an immediate parent barrier. Replicated
	// writers share the journal's rollback and final publication barrier.
	s.directoryMu.RLock()
	if s.pendingDirectorySync == "" {
		err := s.walkSafeDir(dir, false, nil)
		if !errors.Is(err, ErrNotFound) {
			s.directoryMu.RUnlock()
			return err
		}
	}
	s.directoryMu.RUnlock()
	// Creation and failed directory barriers stay exclusive: another writer
	// must not publish beneath a mkdir whose parent has not been synced yet.
	s.directoryMu.Lock()
	defer s.directoryMu.Unlock()
	if s.pendingDirectorySync != "" {
		if err := syncDir(s.pendingDirectorySync); err != nil {
			return err
		}
		s.pendingDirectorySync = ""
	}
	return s.walkSafeDir(dir, true, journal)
}

func (s *FileStore) walkSafeDir(dir string, create bool, journal *replicationJournal) error {
	rootAbs, err := filepath.Abs(s.root)
	if err != nil {
		return err
	}
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(rootAbs, dirAbs)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("object directory %q is outside root %q", dirAbs, rootAbs)
	}
	if err := s.ensureSafeDirComponent(rootAbs, create, journal); err != nil {
		return err
	}
	if rel == "." {
		return nil
	}
	if err := s.detectDirectoryCase(rootAbs); err != nil {
		return err
	}
	current := rootAbs
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		if s.caseMode.Load() == 2 {
			if err := s.checkDirectorySpelling(current, part); err != nil {
				return err
			}
		}

		current = filepath.Join(current, part)
		if err := s.ensureSafeDirComponent(current, create, journal); err != nil {
			return err
		}
	}
	return nil
}

// Cache the filesystem capability, not tenant names: aliases must also be
// rejected for legacy records and after directory replacement or creation.
func (s *FileStore) detectDirectoryCase(root string) error {
	if s.caseMode.Load() != 0 {
		return nil
	}
	s.caseMu.Lock()
	defer s.caseMu.Unlock()
	if s.caseMode.Load() != 0 {
		return nil
	}
	probe, err := os.CreateTemp(root, ".tmp-case-A-*")
	if err != nil {
		return err
	}
	info, statErr := probe.Stat()
	alias, aliasErr := os.Stat(filepath.Join(root, strings.ToLower(filepath.Base(probe.Name()))))
	err = errors.Join(statErr, probe.Close(), os.Remove(probe.Name()))
	if aliasErr != nil && !os.IsNotExist(aliasErr) {
		err = errors.Join(err, aliasErr)
	}
	if err != nil {
		return err
	}
	mode := int32(1)
	if aliasErr == nil && os.SameFile(info, alias) {
		mode = 2
	}
	s.caseMode.Store(mode)
	return nil
}

func (s *FileStore) ensureSafeDirComponent(path string, create bool, journal *replicationJournal) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		if !create {
			return ErrNotFound
		}
		if err := os.Mkdir(path, 0o755); err != nil && !os.IsExist(err) {
			return err
		}
		if journal != nil {
			journal.deferDirectorySync(filepath.Dir(path))
		} else {
			s.pendingDirectorySync = filepath.Dir(path)
			if err := syncDir(s.pendingDirectorySync); err != nil {
				return err
			}
			s.pendingDirectorySync = ""
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("object directory %q is a symlink", path)
	}
	if !info.IsDir() {
		return fmt.Errorf("object path component %q is not a directory", path)
	}
	return nil
}

func readFileStoreObjectState(path string, includeETag bool) (string, bool, error) {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if !info.Mode().IsRegular() {
		return "", false, fmt.Errorf("object path %q is not a regular file", path)
	}
	if !includeETag {
		return "", true, nil
	}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return sha256Hex(data), true, nil
}
