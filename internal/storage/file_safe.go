package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func (s *FileStore) verifySafeParent(filePath string) error {
	return s.walkSafeDir(filepath.Dir(filePath), false, nil)
}

func (s *FileStore) ensureSafeParent(filePath string, journal *replicationJournal) error {
	return s.ensureSafeDirectory(filepath.Dir(filePath), journal)
}

func (s *FileStore) ensureSafeDirectory(dir string, journal *replicationJournal) error {
	// Standalone writers require an immediate parent barrier. Replicated
	// writers share the journal's rollback and final publication barrier.
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
	current := rootAbs
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		if part == "" || part == "." {
			continue
		}
		current = filepath.Join(current, part)
		if err := s.ensureSafeDirComponent(current, create, journal); err != nil {
			return err
		}
	}
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
