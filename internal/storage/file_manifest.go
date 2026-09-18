package storage

import "context"

type localManifestEntry struct {
	manifest Manifest
	meta     ObjectMeta
	bytes    int
}

// Cache decoded heads only while the directory is exclusively owned. A file
// change drops the entry before notifying readers; old reads cannot republish
// across that change, even when a restore reuses the same graph version.
func (s *FileStore) cachedManifest(ctx context.Context, key string) (Manifest, ObjectMeta, uint64, bool, error) {
	if err := objectContextErr(ctx); err != nil {
		return Manifest{}, ObjectMeta{}, 0, false, err
	}
	r := s.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return Manifest{}, ObjectMeta{}, 0, false, ErrFileStoreClosed
	}
	if r.restoreErr != nil {
		return Manifest{}, ObjectMeta{}, 0, false, r.restoreErr
	}
	entry, ok := r.manifests[key]
	return cloneManifest(entry.manifest), entry.meta, r.generation, ok, nil
}

func (s *FileStore) cacheManifest(key string, manifest Manifest, meta ObjectMeta, generation uint64, published bool) {
	r := s.runtime
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || (!published && generation != r.generation) || (published && r.etags[key] != meta.ETag) {
		return
	}
	// A wrapping byte cache can still serve the previous head while its Put is
	// returning. Generation alone cannot distinguish that delayed read from a
	// read of the newly published file.
	if current, known := r.etags[key]; known && current != meta.ETag {
		return
	}
	size := 512 + len(key) + len(meta.ETag) + len(manifest.TenantID) + len(manifest.HeadCommitID) + len(manifest.SnapshotKey) + len(manifest.SnapshotCatalogKey) + len(manifest.WriterFence) + len(manifest.DataMD5)
	for _, key := range manifest.CommitKeys {
		size += 16 + len(key)
	}
	for _, segment := range manifest.CommitSegments {
		size += 80 + len(segment.Key) + len(segment.Codec) + len(segment.ContentHash)
	}
	if size > 8<<20 {
		return
	}
	if r.manifests == nil {
		r.manifests = make(map[string]localManifestEntry)
	}
	if old, ok := r.manifests[key]; ok {
		r.manifestBytes -= old.bytes
	}
	if r.manifestBytes+size > 8<<20 || len(r.manifests) >= 4096 {
		clear(r.manifests)
		r.manifestBytes = 0
	}
	if published {
		manifest.LayoutVersion = CurrentObjectLayoutVersion
	}
	r.manifests[key] = localManifestEntry{manifest: cloneManifest(manifest), meta: meta, bytes: size}
	r.manifestBytes += size
}

func cloneManifest(manifest Manifest) Manifest {
	manifest.CommitKeys = append([]string(nil), manifest.CommitKeys...)
	manifest.CommitSegments = append([]CommitSegmentRef(nil), manifest.CommitSegments...)
	return manifest
}
