package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestReplicationApplicationCrashRecovery(t *testing.T) {
	if root := os.Getenv("GRAPHDB_TEST_REPLICA_CRASH_ROOT"); root != "" {
		files, err := OpenFileStore(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := files.RequireReplicatedWrites(); err != nil {
			t.Fatal(err)
		}
		_, err = files.ApplyReplicated(context.Background(), 2, "crashing-command", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
			if os.Getenv("GRAPHDB_TEST_REPLICA_CRASH_STAGE") == "empty" {
				os.Exit(0)
			}
			if err := files.Put(ctx, "graphdb/manifest", []byte("new-version")); err != nil {
				return nil, err
			}
			if err := files.Put(ctx, "graphdb/new-data", []byte("new")); err != nil {
				return nil, err
			}
			if err := files.Put(ctx, "graphdb/new/child/data", []byte("new")); err != nil {
				return nil, err
			}
			if err := files.Put(ctx, "graphdb/manifest", []byte("newer-version")); err != nil {
				return nil, err
			}
			if err := files.Delete(ctx, "graphdb/new-data"); err != nil {
				return nil, err
			}
			if err := files.Put(ctx, "graphdb/new-data", []byte("recreated")); err != nil {
				return nil, err
			}
			os.Exit(0)
			return nil, nil
		})
		t.Fatal(err)
	}
	for _, stage := range []string{"empty", "partial"} {
		t.Run(stage, func(t *testing.T) {
			root := t.TempDir()
			files, err := OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			if err := files.RequireReplicatedWrites(); err != nil {
				t.Fatal(err)
			}
			_, err = files.ApplyReplicated(context.Background(), 1, "initial", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
				return []byte("old-response"), files.Put(ctx, "graphdb/manifest", []byte("old-version"))
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(os.Args[0], "-test.run=^TestReplicationApplicationCrashRecovery$")
			command.Env = append(os.Environ(), "GRAPHDB_TEST_REPLICA_CRASH_ROOT="+root, "GRAPHDB_TEST_REPLICA_CRASH_STAGE="+stage)
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("crash helper: %v: %s", err, output)
			}
			files, err = OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			if err := files.RequireReplicatedWrites(); err != nil {
				t.Fatal(err)
			}
			data, err := files.Get(context.Background(), "graphdb/manifest")
			if err != nil || string(data) != "old-version" {
				t.Fatalf("partial publication was not rolled back: %q, %v", data, err)
			}
			for _, key := range []string{"graphdb/new-data", "graphdb/new/child/data"} {
				if _, err := files.Get(context.Background(), key); !errors.Is(err, ErrNotFound) {
					t.Fatalf("uncommitted file %q survived: %v", key, err)
				}
			}
			checkpoint, err := files.ReplicationCheckpoint()
			if err != nil || checkpoint.Index != 1 {
				t.Fatalf("applied position advanced across crash: %+v, %v", checkpoint, err)
			}
			for i := 0; i < 2; i++ {
				_, err = files.ApplyReplicated(context.Background(), 2, "replay", time.Unix(2, 0), func(ctx context.Context) ([]byte, error) {
					for _, key := range []string{"graphdb/manifest", "graphdb/pages/a", "graphdb/commits/b"} {
						if err := files.Put(ctx, key, []byte("committed")); err != nil {
							return nil, err
						}
					}
					return []byte("new-response"), nil
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			checkpoint, err = files.ReplicationCheckpoint()
			if err != nil || checkpoint.Index != 2 || string(checkpoint.Response) != "new-response" {
				t.Fatalf("replay checkpoint mismatch: %+v, %v", checkpoint, err)
			}
			if err := files.Close(); err != nil {
				t.Fatal(err)
			}
			files, err = OpenFileStore(root)
			if err != nil {
				t.Fatal(err)
			}
			defer files.Close()
			for _, key := range []string{"graphdb/manifest", "graphdb/pages/a", "graphdb/commits/b"} {
				data, err := files.Get(context.Background(), key)
				if err != nil || string(data) != "committed" {
					t.Fatalf("checkpointed object %q did not survive reopen: %q, %v", key, data, err)
				}
			}
		})
	}
}

func TestReplicationApplicationDirectorySyncFailure(t *testing.T) {
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	_, err = files.ApplyReplicated(context.Background(), 1, "directory-failure", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		if err := files.Put(ctx, "graphdb/pages/a", []byte("new")); err != nil {
			return nil, err
		}
		// Simulate a directory disappearing before the publication barrier.
		return []byte("response"), os.RemoveAll(filepath.Join(root, "graphdb", "pages"))
	})
	if err == nil {
		t.Fatal("failed publication directory sync was checkpointed")
	}
	checkpoint, err := files.ReplicationCheckpoint()
	if err != nil || checkpoint.Index != 0 {
		t.Fatalf("failed directory sync advanced checkpoint: %+v, %v", checkpoint, err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if _, err := files.Get(context.Background(), "graphdb/pages/a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed publication was not rolled back: %v", err)
	}
}

func TestReplicationApplicationDirectoryReplacement(t *testing.T) {
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.RequireReplicatedWrites(); err != nil {
		t.Fatal(err)
	}
	const target = "graphdb/tenants/a"
	_, err = files.ApplyReplicated(context.Background(), 1, "replacement", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		if err := files.Put(ctx, target+"/old-only/object", []byte("old")); err != nil {
			return nil, err
		}
		dir, err := files.newRestoreDirectory()
		if err != nil {
			return nil, err
		}
		stage := NewFileStore(filepath.Join(dir, "build"))
		if err := stage.Put(ctx, target+"/manifest", []byte("replacement")); err != nil {
			return nil, err
		}
		return nil, files.publishRestoreDirectory(ctx, dir, target)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	data, err := files.Get(context.Background(), target+"/manifest")
	if err != nil || string(data) != "replacement" {
		t.Fatalf("replacement did not survive reopen: %q, %v", data, err)
	}
	if _, err := files.Get(context.Background(), target+"/old-only/object"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replaced object survived: %v", err)
	}
}

func TestReplicationApplicationDoesNotCheckpointHiddenIOFailure(t *testing.T) {
	files, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	if err := files.RequireReplicatedWrites(); err != nil {
		t.Fatal(err)
	}
	_, err = files.ApplyReplicated(context.Background(), 1, "io-failure", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		if err := files.Put(ctx, "graphdb/original", []byte("partial")); err != nil {
			return nil, err
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		_ = files.Put(canceled, "graphdb/next", []byte("never-written"))
		return []byte("handler-translated-error"), nil
	})
	if err == nil {
		t.Fatal("translated filesystem failure was checkpointed")
	}
	checkpoint, err := files.ReplicationCheckpoint()
	if err != nil || checkpoint.Index != 0 {
		t.Fatalf("failed I/O advanced checkpoint: %+v, %v", checkpoint, err)
	}
	if _, err := files.Get(context.Background(), "graphdb/original"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("failed transaction retained partial data: %v", err)
	}
}

func TestReplicationSnapshotValidatesArchiveBeforePublication(t *testing.T) {
	ctx := context.Background()
	source, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	_, err = source.ApplyReplicated(ctx, 2, "source", time.Unix(2, 0), func(ctx context.Context) ([]byte, error) {
		return nil, source.Put(ctx, "graphdb/data", bytes.Repeat([]byte("payload"), 16384))
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.ReplicationSnapshot(ctx, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	target, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	_, err = target.ApplyReplicated(ctx, 1, "target", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		return nil, target.Put(ctx, "graphdb/old", []byte("old"))
	})
	if err != nil {
		t.Fatal(err)
	}
	invalid := bytes.Clone(snapshot)
	invalid[len(invalid)-1] ^= 1
	sum := sha256.Sum256(invalid[32:])
	copy(invalid[:32], sum[:])
	if err := target.InstallReplicationSnapshot(ctx, 2, invalid, 1<<20); err == nil {
		t.Fatal("archive with a corrupt gzip trailer was installed")
	}
	if data, err := target.Get(ctx, "graphdb/old"); err != nil || string(data) != "old" {
		t.Fatalf("rejected snapshot changed live data: %q, %v", data, err)
	}
	if err := target.InstallReplicationSnapshot(ctx, 2, snapshot, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	target, err = OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	checkpoint, err := target.ReplicationCheckpoint()
	if err != nil || checkpoint.Index != 2 {
		t.Fatalf("snapshot checkpoint: %v, %v", checkpoint, err)
	}
	if data, err := target.Get(ctx, "graphdb/data"); err != nil || !bytes.Equal(data, bytes.Repeat([]byte("payload"), 16384)) {
		t.Fatalf("installed snapshot did not survive reopen: %v", err)
	}
	if _, err := target.Get(ctx, "graphdb/old"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot retained old object: %v", err)
	}
}
