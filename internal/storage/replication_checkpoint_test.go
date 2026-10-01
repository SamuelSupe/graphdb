package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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
			if os.Getenv("GRAPHDB_TEST_REPLICA_CRASH_STAGE") == "concurrent" {
				jobs := make([]func() error, 8)
				for worker := range jobs {
					jobs[worker] = func() error {
						old := fmt.Sprintf("graphdb/concurrent/old/%d", worker)
						fresh := fmt.Sprintf("graphdb/concurrent/new/%d", worker)
						if err := files.Put(ctx, old, []byte("updated")); err != nil {
							return err
						}
						if err := files.Put(ctx, fresh, []byte("new")); err != nil {
							return err
						}
						return files.Put(ctx, old, []byte("updated-again"))
					}
				}
				if err := runIngestMetadataJobs(jobs); err != nil {
					return nil, err
				}
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
			if err := files.Delete(ctx, "graphdb/manifest"); err != nil {
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
	for _, stage := range []string{"empty", "partial", "concurrent"} {
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
				if stage == "concurrent" {
					for worker := range 8 {
						if err := files.Put(ctx, fmt.Sprintf("graphdb/concurrent/old/%d", worker), []byte("old")); err != nil {
							return nil, err
						}
					}
				}
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
			if stage == "concurrent" {
				for worker := range 8 {
					data, err := files.Get(context.Background(), fmt.Sprintf("graphdb/concurrent/old/%d", worker))
					if err != nil || string(data) != "old" {
						t.Fatalf("concurrent before-image was not restored: %q, %v", data, err)
					}
					if _, err := files.Get(context.Background(), fmt.Sprintf("graphdb/concurrent/new/%d", worker)); !errors.Is(err, ErrNotFound) {
						t.Fatalf("concurrent new object survived interrupted application: %v", err)
					}
				}
			}
			checkpoint, err := files.ReplicationCheckpoint()
			if err != nil || checkpoint.Index != 1 {
				t.Fatalf("applied position advanced across crash: %+v, %v", checkpoint, err)
			}
			for i := 0; i < 2; i++ {
				_, err = files.ApplyReplicated(context.Background(), 2, "replay", time.Unix(2, 0), func(ctx context.Context) ([]byte, error) {
					if err := files.Delete(ctx, "graphdb/manifest"); err != nil {
						return nil, err
					}
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

func TestReplicationApplicationFileSyncFailure(t *testing.T) {
	root := t.TempDir()
	files, err := OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	ctx := context.Background()
	if _, err := files.ApplyReplicated(ctx, 1, "initial", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		return nil, files.Put(ctx, "graphdb/manifest", []byte("old"))
	}); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("outside"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = files.ApplyReplicated(ctx, 2, "file-failure", time.Unix(2, 0), func(ctx context.Context) ([]byte, error) {
		if err := files.Put(ctx, "graphdb/manifest", []byte("new")); err != nil {
			return nil, err
		}
		if err := files.Put(ctx, "graphdb/new-data", []byte("new")); err != nil {
			return nil, err
		}
		path := filepath.Join(root, "graphdb", "manifest")
		if err := os.Remove(path); err != nil {
			return nil, err
		}
		// The final data barrier must reject a replaced leaf before checkpointing.
		return []byte("response"), os.Symlink(outside, path)
	})
	if err == nil {
		t.Fatal("failed final object sync was checkpointed")
	}
	checkpoint, err := files.ReplicationCheckpoint()
	if err != nil || checkpoint.Index != 1 {
		t.Fatalf("failed file sync advanced checkpoint: %+v, %v", checkpoint, err)
	}
	if err := files.Close(); err != nil {
		t.Fatal(err)
	}
	files, err = OpenFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	defer files.Close()
	data, err := files.Get(ctx, "graphdb/manifest")
	if err != nil || string(data) != "old" {
		t.Fatalf("failed final object sync was not rolled back: %q, %v", data, err)
	}
	if _, err := files.Get(ctx, "graphdb/new-data"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("new object survived a failed final barrier: %v", err)
	}
	data, err = os.ReadFile(outside)
	if err != nil || string(data) != "outside" {
		t.Fatalf("replaced leaf modified an outside file: %q, %v", data, err)
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
	_, err = files.ApplyReplicated(context.Background(), 1, "initial", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
		if err := files.Put(ctx, target+"/old-only/object", []byte("old")); err != nil {
			return nil, err
		}
		return nil, files.Put(ctx, target+"/kept/object", []byte("old"))
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = files.ApplyReplicated(context.Background(), 2, "replacement", time.Unix(2, 0), func(ctx context.Context) ([]byte, error) {
		for _, key := range []string{target + "/old-only/object", target + "/kept/object"} {
			if err := files.Put(ctx, key, []byte("updated")); err != nil {
				return nil, err
			}
		}
		dir, err := files.newRestoreDirectory()
		if err != nil {
			return nil, err
		}
		stage := NewFileStore(filepath.Join(dir, "build"))
		if err := stage.Put(ctx, target+"/manifest", []byte("replacement")); err != nil {
			return nil, err
		}
		if err := os.Link(filepath.Join(root, target, "kept", "object"), filepath.Join(dir, "build", target, "kept-copy")); err != nil {
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
	data, err = files.Get(context.Background(), target+"/kept-copy")
	if err != nil || string(data) != "updated" {
		t.Fatalf("updated hard-linked object did not survive replacement: %q, %v", data, err)
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

func BenchmarkReplicationJournalWrites(b *testing.B) {
	for _, workers := range []int{1, 4, 8} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			files, err := OpenFileStore(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			defer files.Close()
			if err := files.RequireReplicatedWrites(); err != nil {
				b.Fatal(err)
			}
			payload := make([]byte, 1024)
			var writes int64
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := range b.N {
				_, err := files.ApplyReplicated(context.Background(), uint64(iteration+1), "bench", time.Unix(1, 0), func(ctx context.Context) ([]byte, error) {
					jobs := make([]func() error, workers)
					for worker := range workers {
						jobs[worker] = func() error {
							for object := 0; object < 16/workers; object++ {
								key := fmt.Sprintf("graphdb/%d/%d", worker, object)
								if err := files.Put(ctx, key, payload); err != nil {
									return err
								}
								if err := files.Put(ctx, key, payload); err != nil {
									return err
								}
							}
							return nil
						}
					}
					err := runIngestMetadataJobs(jobs)
					stats := files.replicationJournal(ctx).db.Stats().TxStats
					writes += int64(stats.GetWrite())
					return nil, err
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(writes)/float64(b.N), "journal_write_calls/op")
		})
	}
}
