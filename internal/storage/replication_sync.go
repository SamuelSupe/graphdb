package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
)

// Every mutation has a durable before-image, including an absence marker for
// new objects. Sync each final file once before directories and the commit
// marker; an interrupted barrier still rolls back the whole application.
func (j *replicationJournal) deferFileSync(path string) {
	j.fileMu.Lock()
	defer j.fileMu.Unlock()
	if j.fileSyncs == nil {
		j.fileSyncs = make(map[string]struct{})
	}
	j.fileSyncs[path] = struct{}{}
}

func (j *replicationJournal) forgetFileSync(path string) {
	j.fileMu.Lock()
	defer j.fileMu.Unlock()
	delete(j.fileSyncs, path)
}

func (j *replicationJournal) syncFiles() error {
	j.fileMu.Lock()
	defer j.fileMu.Unlock()
	paths := make([]string, 0, len(j.fileSyncs))
	for path := range j.fileSyncs {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	workers := min(2, len(paths))
	err := runIndexWriteJobs(context.Background(), workers, func(ctx context.Context, worker int) error {
		for index := worker; index < len(paths); index += workers {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := j.syncFile(paths[index]); err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil {
		clear(j.fileSyncs)
	}
	return err
}

func (j *replicationJournal) syncFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("replication object %q is not a regular file", path)
	}
	if err := j.files.verifySafeParent(path); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	return errors.Join(syncStorageFile(file), file.Close())
}
