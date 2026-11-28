// Package catalog owns the versioned Manifest contract and the single
// pure-function object protection & reachability truth layer shared across
// GC, Repair, and Integrity Audit.
package catalog

import "time"

type Manifest struct {
	LayoutVersion      int                `json:"layout_version,omitempty"`
	TenantID           string             `json:"tenant_id"`
	Version            int64              `json:"version"`
	HeadCommitID       string             `json:"head_commit_id,omitempty"`
	SnapshotKey        string             `json:"snapshot_key,omitempty"`
	SnapshotCatalogKey string             `json:"snapshot_catalog_key,omitempty"`
	SnapshotVersion    int64              `json:"snapshot_version"`
	CommitSegments     []CommitSegmentRef `json:"commit_segments,omitempty"`
	CommitKeys         []string           `json:"commit_keys,omitempty"`
	UpdatedAt          time.Time          `json:"updated_at"`
	WriterFence        string             `json:"-"`
	WriterFenceEpoch   int64              `json:"-"`
	DataHash           string             `json:"-"`
}

type CommitSegmentRef struct {
	Key          string `json:"key"`
	Codec        string `json:"codec"`
	FirstVersion int64  `json:"first_version"`
	LastVersion  int64  `json:"last_version"`
	Count        int    `json:"count"`
	ContentHash  string `json:"content_hash"`
}

// ProtectedSnapshotSelector is the single source of truth describing which
// snapshot objects and snapshot version watermarks a Manifest protects from
// deletion across GC, orphan cleanup, repair, and integrity auditing.
type ProtectedSnapshotSelector struct {
	HasSnapshot        bool
	SnapshotVersion    int64
	SnapshotKey        string
	SnapshotCatalogKey string
}

// ProtectedSnapshotSelector returns the pure, deterministic snapshot protection
// descriptor derived from this Manifest.
func (m Manifest) ProtectedSnapshotSelector() ProtectedSnapshotSelector {
	return ProtectedSnapshotSelector{
		HasSnapshot:        m.SnapshotKey != "" || m.SnapshotCatalogKey != "",
		SnapshotVersion:    m.SnapshotVersion,
		SnapshotKey:        m.SnapshotKey,
		SnapshotCatalogKey: m.SnapshotCatalogKey,
	}
}

// ProtectsVersion reports whether snapshot objects belonging to version must be
// retained because the manifest points at this version or an older base watermark.
func (p ProtectedSnapshotSelector) ProtectsVersion(version int64) bool {
	return p.HasSnapshot && version >= p.SnapshotVersion
}

// ProtectsSnapshotKey reports whether a snapshot object identified by key and
// version is protected by the manifest.
func (p ProtectedSnapshotSelector) ProtectsSnapshotKey(key string, version int64) bool {
	if key != "" && (key == p.SnapshotKey || key == p.SnapshotCatalogKey) {
		return true
	}
	return p.ProtectsVersion(version)
}

// ProtectsShardedOrphanVersion reports whether a sharded snapshot object at
// version is at or ahead of the manifest's snapshot watermark.
func (p ProtectedSnapshotSelector) ProtectsShardedOrphanVersion(version int64) bool {
	return version >= p.SnapshotVersion
}

// ProtectedObjectSet holds the complete set of object keys reachable from a
// manifest and its associated snapshot/index catalogs.
type ProtectedObjectSet struct {
	ManifestVersion int64
	Snapshot        ProtectedSnapshotSelector
	Keys            map[string]struct{}
}

// Contains reports whether key is directly referenced by the protected set.
func (p ProtectedObjectSet) Contains(key string) bool {
	if key == "" {
		return false
	}
	_, ok := p.Keys[key]
	return ok
}

// NewProtectedObjectSet builds a ProtectedObjectSet from manifest roots,
// commit tail references, and caller-supplied catalog/adjacency keys.
func NewProtectedObjectSet(manifest Manifest, extraKeys ...string) ProtectedObjectSet {
	keys := make(map[string]struct{}, len(manifest.CommitSegments)+len(manifest.CommitKeys)+len(extraKeys)+2)
	add := func(k string) {
		if k != "" {
			keys[k] = struct{}{}
		}
	}
	add(manifest.SnapshotKey)
	add(manifest.SnapshotCatalogKey)
	for _, seg := range manifest.CommitSegments {
		add(seg.Key)
	}
	for _, k := range manifest.CommitKeys {
		add(k)
	}
	for _, k := range extraKeys {
		add(k)
	}
	return ProtectedObjectSet{
		ManifestVersion: manifest.Version,
		Snapshot:        manifest.ProtectedSnapshotSelector(),
		Keys:            keys,
	}
}
