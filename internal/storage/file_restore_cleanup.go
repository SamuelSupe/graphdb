package storage

import (
	"os"
	"path/filepath"
)

// Private, unjournaled staging trees are garbage. Keep recursive unlink off the
// request path and tenant locks; a full queue leaves work for startup recovery.
// Close drains the single worker before releasing ownership of the data root.
func (s *FileStore) scheduleRestoreCleanup(dir string) {
	if _, err := os.Lstat(filepath.Join(dir, "journal.json")); !os.IsNotExist(err) {
		return
	}
	if s.runtime == nil {
		_ = removeRestoreDirectory(dir)
		return
	}
	r := s.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if r.restoreCleanup == nil {
		r.restoreCleanup = make(chan string, 16)
		r.active.Add(1)
		go func() {
			defer r.active.Done()
			for dir := range r.restoreCleanup {
				if _, err := os.Lstat(filepath.Join(dir, "journal.json")); os.IsNotExist(err) {
					_ = removeRestoreDirectory(dir)
				}
			}
		}()
	}
	select {
	case r.restoreCleanup <- dir:
	default:
	}
}
