package graph

import (
	"sync"
	"testing"
	"time"
)

func TestContentFingerprintTracksMutationSizedChanges(t *testing.T) {
	g := New()
	now := time.Now().UTC()
	apply := func(version int64, mutations Mutations) ApplyReport {
		t.Helper()
		next, report, err := g.ApplyCommitStorageCopyWithOptions(Commit{
			ID: "commit", Version: version, CreatedAt: now.Add(time.Duration(version) * time.Second), Mutations: mutations,
		}, ApplyOptions{})
		if err != nil {
			t.Fatalf("apply version %d: %v", version, err)
		}
		g = next
		assertFingerprintMatchesRebuild(t, g)
		return report
	}

	seed := Mutations{
		UpsertCITypes: []CIType{{Name: "host"}},
		UpsertRelationTypes: []RelationType{{
			Name: "connected", FromKind: "host", ToKind: "host", Directed: true, Cardinality: ManyToMany,
		}},
		UpsertEntities: []Entity{
			{ID: "host:a", Kind: "host", Fields: Fields{"state": "ready"}},
			{ID: "host:b", Kind: "host"},
			{ID: "host:c", Kind: "host", Fields: Fields{"zone": "east"}},
		},
		UpsertEdges: []Edge{
			{ID: "edge:a-b", Type: "connected", From: "host:a", To: "host:b"},
			{ID: "edge:c-b", Type: "connected", From: "host:c", To: "host:b"},
		},
	}
	if report := apply(1, seed); !report.Changed {
		t.Fatal("seed commit was not marked changed")
	}
	before, _ := g.ContentFingerprint()
	if report := apply(2, Mutations{UpsertEntities: []Entity{{ID: "host:a", Kind: "host", Fields: Fields{"state": "ready"}}}}); report.Changed {
		t.Fatal("logical no-op was marked changed")
	}
	after, _ := g.ContentFingerprint()
	if after != before {
		t.Fatalf("no-op fingerprint changed from %q to %q", before, after)
	}
	if report := apply(3, Mutations{MergeEntities: []MergeRequest{{TargetID: "host:a", SourceIDs: []string{"host:c"}}}}); !report.Changed {
		t.Fatal("merge was not marked changed")
	}
	if report := apply(4, Mutations{SplitEntities: []SplitRequest{{
		SourceID: "host:a", Entities: []Entity{{ID: "host:x", Kind: "host"}, {ID: "host:y", Kind: "host"}},
	}}}); !report.Changed {
		t.Fatal("split was not marked changed")
	}
	if report := apply(5, Mutations{DeleteRelationTypes: []string{"connected"}}); !report.Changed {
		t.Fatal("relation deletion was not marked changed")
	}
	if report := apply(6, Mutations{DeleteEntities: []string{"missing"}}); report.Changed {
		t.Fatal("missing entity deletion was marked changed")
	}
}

func TestContentFingerprintConcurrentFirstRead(t *testing.T) {
	g := New()
	g.Entities["host:a"] = Entity{ID: "host:a", Kind: "host", Fields: Fields{"name": "app"}}
	const readers = 32
	values := make(chan string, readers)
	errs := make(chan error, readers)
	var wg sync.WaitGroup
	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := g.ContentFingerprint()
			values <- value
			errs <- err
		}()
	}
	wg.Wait()
	close(values)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("content fingerprint: %v", err)
		}
	}
	want := ""
	for value := range values {
		if want == "" {
			want = value
		}
		if value != want {
			t.Fatalf("concurrent fingerprint = %q, want %q", value, want)
		}
	}
}

func assertFingerprintMatchesRebuild(t *testing.T, g *Graph) {
	t.Helper()
	want, err := g.ContentFingerprint()
	if err != nil {
		t.Fatalf("incremental fingerprint: %v", err)
	}
	rebuilt, err := FromSnapshot(g.Snapshot())
	if err != nil {
		t.Fatalf("rebuild graph: %v", err)
	}
	recomputed, err := rebuilt.ContentFingerprint()
	if err != nil {
		t.Fatalf("recomputed fingerprint: %v", err)
	}
	if recomputed != want {
		t.Fatalf("incremental fingerprint = %q, recomputed = %q", want, recomputed)
	}
}
