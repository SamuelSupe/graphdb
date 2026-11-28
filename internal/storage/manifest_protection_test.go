package storage

import "testing"

func TestManifestProtectedSnapshotSelectorAndReachableKeys(t *testing.T) {
	t.Run("sharded_only_manifest_protects_version_and_cursor", func(t *testing.T) {
		store := NewTenantStore(NewMemoryStore(), "prefix")
		catKey := store.snapshotCatalogKey("tenant-a", 8)
		m := Manifest{
			TenantID:           "tenant-a",
			Version:            10,
			SnapshotVersion:    8,
			SnapshotCatalogKey: catKey,
			CommitSegments:     []CommitSegmentRef{{Key: "prefix/tenants/tenant-a/commits/segments/s1.parquet"}},
			CommitKeys:         []string{"prefix/tenants/tenant-a/commits/00000000000000000010-c10.parquet"},
		}
		sel := m.ProtectedSnapshotSelector()
		if !sel.HasSnapshot {
			t.Fatalf("expected sharded-only manifest to report HasSnapshot=true")
		}
		if sel.ProtectsVersion(7) {
			t.Fatalf("version 7 < SnapshotVersion 8 should not be protected")
		}
		if !sel.ProtectsVersion(8) || !sel.ProtectsVersion(9) {
			t.Fatalf("versions >= SnapshotVersion 8 must be protected")
		}
		if !sel.ProtectsSnapshotKey(m.SnapshotCatalogKey, 8) {
			t.Fatalf("SnapshotCatalogKey must be protected")
		}
		if !sel.ProtectsSnapshotKey("other", 8) {
			t.Fatalf("same snapshot version must be protected")
		}

		if err := store.validateGCSnapshotCursor("tenant-a", m, m.SnapshotCatalogKey); err == nil {
			t.Fatalf("validateGCSnapshotCursor must reject current sharded snapshot cursor")
		}
		older := store.snapshotCatalogKey("tenant-a", 5)
		if err := store.validateGCSnapshotCursor("tenant-a", m, older); err != nil {
			t.Fatalf("validateGCSnapshotCursor rejected older snapshot cursor: %v", err)
		}
	})

	t.Run("empty_manifest_has_no_snapshot", func(t *testing.T) {
		m := Manifest{TenantID: "tenant-a"}
		sel := m.ProtectedSnapshotSelector()
		if sel.HasSnapshot || sel.ProtectsVersion(1) {
			t.Fatalf("empty manifest should not protect snapshots: %+v", sel)
		}
	})

	t.Run("collect_reachable_keys_unifies_all_catalogs", func(t *testing.T) {
		m := Manifest{
			Version:            5,
			SnapshotVersion:    4,
			SnapshotKey:        "snap-4",
			SnapshotCatalogKey: "snap-cat-4",
			CommitSegments:     []CommitSegmentRef{{Key: "seg-5"}},
			CommitKeys:         []string{"commit-5"},
		}
		snapCat := &ShardedSnapshotCatalog{
			Key:         "snap-cat-4",
			Schema:      SnapshotSchemaSpec{Key: "snap-schema-4"},
			EntityPages: []SnapshotEntityPageSpec{{Key: "snap-ent-00"}},
			EdgeShards:  []SnapshotEdgeShardSpec{{Key: "snap-edge-00"}},
		}
		idxCat := &IndexCatalog{
			Indexes: []IndexSpec{{Objects: []IndexObject{{Key: "sec-1"}}}},
			EdgeShards: []EdgeShard{{
				Objects:       []IndexObject{{Key: "edge-1"}},
				AdjacencyRoot: &AdjacencyRef{Key: "adj-fwd-root"},
			}},
			EntityPages: []EntityPageSpec{{Objects: []IndexObject{{Key: "ent-1"}}}},
		}
		revCat := &ReverseIndexCatalog{
			EdgeShards: []EdgeShard{{
				Objects:       []IndexObject{{Key: "rev-edge-1"}},
				AdjacencyRoot: &AdjacencyRef{Key: "adj-rev-root"},
			}},
		}
		reachable := CollectReachableKeys(m, snapCat, idxCat, revCat, "adj-leaf-1")
		for _, want := range []string{
			"snap-4", "snap-cat-4", "seg-5", "commit-5",
			"snap-schema-4", "snap-ent-00", "snap-edge-00",
			"sec-1", "edge-1", "adj-fwd-root", "ent-1",
			"rev-edge-1", "adj-rev-root", "adj-leaf-1",
		} {
			if !reachable.Contains(want) {
				t.Fatalf("expected reachable set to contain %q", want)
			}
		}
		if reachable.Contains("orphan-key") {
			t.Fatalf("unexpected orphan-key in reachable set")
		}
	})
}
