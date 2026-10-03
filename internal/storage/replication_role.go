package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// A replica cannot drop its shard fence by restarting without its role settings.
// Snapshot installation retains this local identity of the target replica.
func (s *FileStore) ConfigureReplicationRole(role string) error {
	filename := filepath.Join(s.root, replicationDirectory, "role")
	saved, err := os.ReadFile(filename)
	if err == nil {
		if string(saved) != role {
			return fmt.Errorf("Raft directory role is %q; restore its original shard/catalog configuration", saved)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if role == "" {
		return nil
	}
	return writeFileAtomic(filename, []byte(role))
}

// ConfigureReplicationPrefix binds this replica's directory to its data namespace.
// Older directories are checked before the identity is first persisted.
func (s *FileStore) ConfigureReplicationPrefix(ctx context.Context, prefix string) error {
	if prefix == "" {
		return fmt.Errorf("Raft data prefix must not be empty")
	}
	filename := filepath.Join(s.root, replicationDirectory, "prefix")
	saved, err := os.ReadFile(filename)
	if err == nil {
		if string(saved) != prefix {
			return fmt.Errorf("Raft directory prefix is %q, configured %q; restore its original GRAPHDB_PREFIX", saved, prefix)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	if err := s.validateReplicationPrefix(ctx, prefix); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return err
	}
	return writeFileAtomic(filename, []byte(prefix))
}

func (s *FileStore) validateReplicationPrefix(ctx context.Context, prefix string) error {
	for after := ""; ; {
		objects, next, err := s.ListPage(ctx, "", after, 4096)
		if err != nil {
			return err
		}
		for _, object := range objects {
			if !strings.HasPrefix(object.Key, prefix+"/") {
				return fmt.Errorf("Raft object %q is outside configured GRAPHDB_PREFIX %q", object.Key, prefix)
			}
		}
		if next == "" {
			return nil
		}
		after = next
	}
}
