package storage

import (
	"fmt"
	"os"
	"path/filepath"
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
