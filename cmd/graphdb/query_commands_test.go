package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/storage"
)

func TestRunGQLCommandExecutesQueryFile(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	if _, err := store.Commit(context.Background(), "tenant-a", graph.Mutations{
		UpsertEntities: []graph.Entity{
			{ID: "host:app-01", Kind: "host", Source: "agent", SourceRank: 100, Fields: graph.Fields{"hostname": "app-01", "cpu": 8}},
			{ID: "host:app-02", Kind: "host", Source: "manual", SourceRank: 1000, Fields: graph.Fields{"hostname": "app-02", "cpu": 16}},
		},
	}, storage.CommitOptions{}); err != nil {
		t.Fatalf("seed commit: %v", err)
	}
	queryPath := filepath.Join(t.TempDir(), "query.gql")
	if err := os.WriteFile(queryPath, []byte(`FIND host WHERE source_priority >= 100 PROJECT id, hostname ORDER BY hostname LIMIT 10`), 0o600); err != nil {
		t.Fatalf("write query file: %v", err)
	}

	output := captureStdout(t, func() {
		if err := runGQL([]string{"tenant-a", queryPath}, store); err != nil {
			t.Fatalf("run gql: %v", err)
		}
	})
	if !strings.Contains(output, `"id": "host:app-01"`) || !strings.Contains(output, `"id": "host:app-02"`) {
		t.Fatalf("output = %s", output)
	}
}

func TestRunGQLCommandRejectsBadSyntax(t *testing.T) {
	store := storage.NewTenantStore(storage.NewMemoryStore(), "test")
	queryPath := filepath.Join(t.TempDir(), "bad.gql")
	if err := os.WriteFile(queryPath, []byte(`FIND host WHERE cpu BETWEEN 1 AND 2`), 0o600); err != nil {
		t.Fatalf("write query file: %v", err)
	}
	if err := runGQL([]string{"tenant-a", queryPath}, store); err == nil {
		t.Fatal("expected bad gql syntax error")
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	old := os.Stdout
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = write
	defer func() {
		os.Stdout = old
	}()
	fn()
	if err := write.Close(); err != nil {
		t.Fatalf("close write pipe: %v", err)
	}
	data, err := io.ReadAll(read)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	return string(data)
}
