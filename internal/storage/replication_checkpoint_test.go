package storage

import (
	"context"
	"errors"
	"os"
	"os/exec"
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
			if err := files.Put(ctx, "graphdb/manifest", []byte("new-version")); err != nil {
				return nil, err
			}
			if err := files.Put(ctx, "graphdb/new-data", []byte("new")); err != nil {
				return nil, err
			}
			os.Exit(0)
			return nil, nil
		})
		t.Fatal(err)
	}
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
	command.Env = append(os.Environ(), "GRAPHDB_TEST_REPLICA_CRASH_ROOT="+root)
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
	if _, err := files.Get(context.Background(), "graphdb/new-data"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("uncommitted file survived: %v", err)
	}
	checkpoint, err := files.ReplicationCheckpoint()
	if err != nil || checkpoint.Index != 1 {
		t.Fatalf("applied position advanced across crash: %+v, %v", checkpoint, err)
	}
	for i := 0; i < 2; i++ {
		_, err = files.ApplyReplicated(context.Background(), 2, "replay", time.Unix(2, 0), func(ctx context.Context) ([]byte, error) {
			return []byte("new-response"), files.Put(ctx, "graphdb/manifest", []byte("committed"))
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	checkpoint, err = files.ReplicationCheckpoint()
	if err != nil || checkpoint.Index != 2 || string(checkpoint.Response) != "new-response" {
		t.Fatalf("replay checkpoint mismatch: %+v, %v", checkpoint, err)
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
