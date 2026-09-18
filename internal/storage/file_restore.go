package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const fileRestoreDirectory = ".graphdb-restores"

type fileRestoreJournal struct {
	Target    string `json:"target"`
	Committed bool   `json:"committed"`
}

func (s *FileStore) newRestoreDirectory() (string, error) {
	parent := filepath.Join(s.root, fileRestoreDirectory)
	if err := s.ensureSafeDirectory(parent); err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp(parent, "restore-")
	if err != nil {
		return "", err
	}
	return dir, syncDir(parent)
}

// Readers are excluded by the tenant view; ioGate excludes task/control writes
// during the short directory switch. The two renames are NOT a transaction:
// a durable journal rolls back an interrupted switch before the store opens.
func (s *FileStore) publishRestoreDirectory(ctx context.Context, dir, targetKey string) (err error) {
	target, err := s.path(targetKey)
	if err != nil {
		return err
	}
	if err := s.walkSafeDir(target, false); err != nil {
		return err
	}
	journal := fileRestoreJournal{Target: targetKey}
	data, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := writeFileAtomic(filepath.Join(dir, "journal.json"), data); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.recoverRestoreDirectory(dir))
		}
	}()
	if err = s.restoreRename(target, filepath.Join(dir, "old")); err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	incoming := filepath.Join(dir, "build", filepath.FromSlash(targetKey))
	if err = s.restoreRename(incoming, target); err != nil {
		return err
	}
	journal.Committed = true
	data, err = json.Marshal(journal)
	if err != nil {
		return err
	}
	// Once the commit record is durable, startup retains the new directory.
	if err = writeFileAtomic(filepath.Join(dir, "journal.json"), data); err != nil {
		return err
	}
	return s.recoverRestoreDirectory(dir)
}

func (s *FileStore) restoreRename(from, to string) error {
	if err := s.walkSafeDir(from, false); err != nil {
		return err
	}
	if err := s.verifySafeParent(to); err != nil {
		return err
	}
	if info, err := os.Lstat(to); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("restore destination is a symlink: %s", to)
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	return errors.Join(syncDir(filepath.Dir(from)), syncDir(filepath.Dir(to)))
}

func (s *FileStore) recoverRestoreDirectories() error {
	parent := filepath.Join(s.root, fileRestoreDirectory)
	if err := s.walkSafeDir(parent, false); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	entries, err := os.ReadDir(parent)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "restore-") {
			return fmt.Errorf("invalid restore staging entry %q", entry.Name())
		}
		if err := s.recoverRestoreDirectory(filepath.Join(parent, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStore) recoverRestoreDirectory(dir string) error {
	journalPath := filepath.Join(dir, "journal.json")
	if err := s.verifySafeParent(journalPath); err != nil {
		return err
	}
	info, err := os.Lstat(journalPath)
	if os.IsNotExist(err) {
		return removeRestoreDirectory(dir)
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 4096 {
		return fmt.Errorf("invalid restore journal %q", journalPath)
	}
	data, err := os.ReadFile(journalPath)
	if err != nil {
		return err
	}
	var journal fileRestoreJournal
	if err := json.Unmarshal(data, &journal); err != nil {
		return err
	}
	parts := strings.Split(journal.Target, "/")
	if len(parts) < 2 || parts[len(parts)-2] != "tenants" || ValidateTenantID(parts[len(parts)-1]) != nil {
		return fmt.Errorf("invalid restore target %q", journal.Target)
	}
	target, err := s.path(journal.Target)
	if err != nil {
		return err
	}
	if err := s.walkSafeDir(target, false); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	old := filepath.Join(dir, "old")
	if err := s.walkSafeDir(old, false); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	if !journal.Committed {
		if _, err := os.Stat(old); err == nil {
			if _, err := os.Stat(target); err == nil {
				incoming := filepath.Join(dir, "build", filepath.FromSlash(journal.Target))
				if err := s.restoreRename(target, incoming); err != nil {
					return err
				}
			} else if !os.IsNotExist(err) {
				return err
			}
			if err := s.restoreRename(old, target); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if info, err := os.Stat(target); err != nil || !info.IsDir() {
		return fmt.Errorf("restore target is unavailable: %s: %v", target, err)
	}
	// Remove the journal durably before deleting the old data. A crash while
	// cleaning up an unjournaled staging directory never changes the live one.
	if err := os.Remove(journalPath); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	return removeRestoreDirectory(dir)
}

func removeRestoreDirectory(dir string) error {
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	return syncDir(filepath.Dir(dir))
}

func linkRestoreTree(ctx context.Context, from, to string) error {
	return linkTenantTree(ctx, from, to, false)
}

func linkTenantTree(ctx context.Context, from, to string, keepExisting bool) error {
	if _, err := os.Lstat(from); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	return filepath.WalkDir(from, func(name string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(from, name)
		if err != nil {
			return err
		}
		target := filepath.Join(to, rel)
		if entry.IsDir() {
			return ensureDurableDirectory(target)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("restore cannot preserve non-regular file %q", name)
		}
		if keepExisting {
			if existing, err := os.Lstat(target); err == nil {
				if !existing.Mode().IsRegular() {
					return fmt.Errorf("staged tenant file is not regular: %q", target)
				}
				return nil
			} else if !os.IsNotExist(err) {
				return err
			}
		}
		if err := os.Link(name, target); err != nil {
			return err
		}
		return syncDir(filepath.Dir(target))
	})
}
