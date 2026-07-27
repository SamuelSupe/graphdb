package storage

import (
	"fmt"
	"testing"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

func TestPostgresCoordinatorAlternatingWritersCrossCommitSegment(t *testing.T) {
	ctx, coordinator := newPostgresIntegrationCoordinator(
		t, "commit-segment",
	)
	objects := NewMemoryStore()
	writers := []*TenantStore{
		NewTenantStore(objects, "test"),
		NewTenantStore(objects, "test"),
	}
	for i, writer := range writers {
		writer.InstanceID = fmt.Sprintf("writer-%d", i)
		writer.SetCoordinator(coordinator)
	}

	const commits = commitSegmentTargetCount + 6
	for i := 0; i < commits; i++ {
		if _, err := writers[i%len(writers)].Commit(
			ctx,
			"tenant-a",
			graph.Mutations{UpsertEntities: []graph.Entity{{
				ID: fmt.Sprintf("host:%03d", i), Kind: "host",
			}}},
			CommitOptions{},
		); err != nil {
			t.Fatalf("commit %d: %v", i, err)
		}
	}

	writers[0].deleteWriteCache("tenant-a")
	g, manifest, err := writers[0].Load(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("cold load: %v", err)
	}
	if manifest.Version != commits || len(g.Entities) != commits {
		t.Fatalf(
			"loaded version/entities = %d/%d, want %d/%d",
			manifest.Version, len(g.Entities), commits, commits,
		)
	}
	if len(manifest.CommitSegments) != 1 ||
		manifest.CommitSegments[0].Count != commitSegmentTargetCount ||
		len(manifest.CommitKeys) != commits-commitSegmentTargetCount {
		t.Fatalf(
			"manifest segments/keys = %#v/%d",
			manifest.CommitSegments, len(manifest.CommitKeys),
		)
	}
}
