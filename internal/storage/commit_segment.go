package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
)

const (
	commitSegmentCodecParquet = "commit-segment-arrow-parquet-v1"
	commitSegmentTargetCount  = 64
)

type storedCommitSegment struct {
	LayoutVersion int       `json:"layout_version,omitempty"`
	Kind          string    `json:"kind"`
	Codec         string    `json:"codec"`
	TenantID      string    `json:"tenant_id"`
	FirstVersion  int64     `json:"first_version"`
	LastVersion   int64     `json:"last_version"`
	Count         int       `json:"count"`
	ContentHash   string    `json:"content_hash"`
	PayloadBytes  int       `json:"payload_bytes"`
	CreatedAt     time.Time `json:"created_at"`
}

type commitSegmentItem struct {
	Key    string       `json:"key"`
	Commit graph.Commit `json:"commit"`
}

type commitSegmentObject struct {
	Ref   CommitSegmentRef
	Items []commitSegmentItem
}

func manifestCommitTailLength(manifest Manifest) int {
	total := len(manifest.CommitKeys)
	for _, segment := range manifest.CommitSegments {
		total += segment.Count
	}
	return total
}

func ManifestCommitTailLength(manifest Manifest) int {
	return manifestCommitTailLength(manifest)
}

func (s *TenantStore) segmentCommitTailIfNeeded(
	ctx context.Context,
	tenantID string,
	manifest *Manifest,
	tail *commitTailCache,
) error {
	if len(manifest.CommitKeys) < commitSegmentTargetCount {
		return nil
	}
	var (
		ref CommitSegmentRef
		err error
	)
	if tail != nil && tail.matches(manifest.CommitKeys) {
		ref, err = s.putNormalizedCommitSegment(
			ctx, tenantID, tail.items,
		)
	} else {
		ref, err = s.putCommitSegmentFromKeys(
			ctx, tenantID, manifest.CommitKeys,
		)
	}
	if err != nil {
		return err
	}
	manifest.CommitSegments = append(append([]CommitSegmentRef(nil), manifest.CommitSegments...), ref)
	manifest.CommitKeys = nil
	if tail != nil {
		*tail = emptyCommitTailCache()
	}
	return nil
}

func (s *TenantStore) putCommitSegmentFromKeys(ctx context.Context, tenantID string, keys []string) (CommitSegmentRef, error) {
	items := make([]commitSegmentItem, 0, len(keys))
	for _, key := range keys {
		if err := s.validateTenantObjectKey(tenantID, key); err != nil {
			return CommitSegmentRef{}, err
		}
		commit, err := s.getCommitObject(ctx, key)
		if err != nil {
			return CommitSegmentRef{}, err
		}
		if commit.TenantID != tenantID {
			return CommitSegmentRef{}, errTenantCommitMismatch(tenantID, key, commit.TenantID)
		}
		if err := validateCommitObjectIdentity(key, commit); err != nil {
			return CommitSegmentRef{}, err
		}
		items = append(items, commitSegmentItem{Key: key, Commit: commit})
	}
	return s.putCommitSegment(ctx, tenantID, items)
}

func (s *TenantStore) putCommitSegment(ctx context.Context, tenantID string, items []commitSegmentItem) (CommitSegmentRef, error) {
	return s.putCommitSegmentWithNormalization(
		ctx, tenantID, items, true,
	)
}

func (s *TenantStore) putNormalizedCommitSegment(
	ctx context.Context,
	tenantID string,
	items []commitSegmentItem,
) (CommitSegmentRef, error) {
	return s.putCommitSegmentWithNormalization(
		ctx, tenantID, items, false,
	)
}

func (s *TenantStore) putCommitSegmentWithNormalization(
	ctx context.Context,
	tenantID string,
	items []commitSegmentItem,
	normalize bool,
) (CommitSegmentRef, error) {
	if len(items) == 0 {
		return CommitSegmentRef{}, fmt.Errorf("empty commit segment")
	}
	logicalPayload, err := marshalCommitSegmentPayload(items)
	if err != nil {
		return CommitSegmentRef{}, err
	}
	hash := objectContentHash(logicalPayload)
	first, last := items[0].Commit.Version, items[len(items)-1].Commit.Version
	key := s.commitSegmentKey(tenantID, first, last, hash)
	var data []byte
	if normalize {
		data, err = marshalParquetCommitSegment(ctx, tenantID, items)
	} else {
		data, err = marshalNormalizedParquetCommitSegment(
			ctx, tenantID, items,
		)
	}
	if err != nil {
		return CommitSegmentRef{}, err
	}
	if _, err := s.Objects.PutConditional(ctx, key, data, PutCondition{IfNoneMatch: true}); err != nil {
		if !errors.Is(err, ErrConflict) {
			return CommitSegmentRef{}, err
		}
		existing, loadErr := s.loadCommitSegment(ctx, tenantID, CommitSegmentRef{Key: key, ContentHash: hash, Count: len(items), FirstVersion: first, LastVersion: last, Codec: commitSegmentCodecParquet})
		if loadErr != nil || len(existing) != len(items) {
			return CommitSegmentRef{}, err
		}
	}
	return CommitSegmentRef{Key: key, Codec: commitSegmentCodecParquet, FirstVersion: first, LastVersion: last, Count: len(items), ContentHash: hash}, nil
}

func (s *TenantStore) loadCommitSegment(ctx context.Context, tenantID string, ref CommitSegmentRef) ([]commitSegmentItem, error) {
	if err := s.validateTenantObjectKey(tenantID, ref.Key); err != nil {
		return nil, err
	}
	data, err := s.Objects.Get(ctx, ref.Key)
	if err != nil {
		return nil, err
	}
	_, items, err := decodeCommitSegmentObject(ctx, data, tenantID, ref, s)
	return items, err
}

func (s *TenantStore) loadCommitSegmentObjects(
	ctx context.Context,
	tenantID string,
	referenced map[string]struct{},
	afterVersion int64,
) ([]commitSegmentObject, []string, error) {
	out := make([]commitSegmentObject, 0)
	invalid := []string{}
	err := scanObjectPrefixFresh(
		ctx,
		s.Objects,
		s.commitSegmentPrefix(tenantID),
		func(objects []ObjectInfo) error {
			for _, object := range objects {
				if _, ok := referenced[object.Key]; ok {
					continue
				}
				if _, last, ok := commitSegmentIdentityFromKey(
					object.Key,
				); afterVersion > 0 && ok && last <= afterVersion {
					continue
				}
				data, err := s.Objects.Get(ctx, object.Key)
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				segment, items, err := decodeCommitSegmentObject(
					ctx,
					data,
					tenantID,
					CommitSegmentRef{Key: object.Key},
					s,
				)
				if err != nil {
					invalid = append(invalid, object.Key)
					continue
				}
				out = append(out, commitSegmentObject{
					Ref:   commitSegmentRefFromObject(object.Key, segment),
					Items: items,
				})
			}
			return nil
		},
	)
	if err != nil {
		return nil, nil, err
	}
	sortCommitSegmentObjects(out)
	sort.Strings(invalid)
	return out, invalid, nil
}

func decodeCommitSegmentObject(ctx context.Context, data []byte, tenantID string, ref CommitSegmentRef, store *TenantStore) (storedCommitSegment, []commitSegmentItem, error) {
	if !isParquetBytes(data) {
		return storedCommitSegment{}, nil, fmt.Errorf("unsupported commit segment object: only parquet segments are readable")
	}
	segment, items, err := decodeParquetCommitSegmentObject(ctx, data, tenantID, ref)
	if err != nil {
		return storedCommitSegment{}, nil, err
	}
	return validateCommitSegmentObject(segment, items, tenantID, ref, store)
}

func validateCommitSegmentObject(object storedCommitSegment, items []commitSegmentItem, tenantID string, ref CommitSegmentRef, store *TenantStore) (storedCommitSegment, []commitSegmentItem, error) {
	if _, err := readableLayoutVersion("commit segment", object.LayoutVersion); err != nil {
		return storedCommitSegment{}, nil, err
	}
	if object.Kind != "commit-segment" {
		return storedCommitSegment{}, nil, fmt.Errorf("unsupported commit segment object")
	}
	if object.Codec != commitSegmentCodecParquet {
		return storedCommitSegment{}, nil, fmt.Errorf("unsupported commit segment object")
	}
	if object.TenantID != tenantID {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment tenant mismatch: key tenant %q contains tenant %q", tenantID, object.TenantID)
	}
	if ref.Codec != "" && ref.Codec != object.Codec {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment codec mismatch")
	}
	payload, err := marshalCommitSegmentPayload(items)
	if err != nil {
		return storedCommitSegment{}, nil, err
	}
	hash := objectContentHash(payload)
	if object.ContentHash == "" || object.ContentHash != hash || (ref.ContentHash != "" && ref.ContentHash != hash) {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment content hash mismatch")
	}
	if object.Count != len(items) || (ref.Count > 0 && ref.Count != len(items)) {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment count mismatch")
	}
	if len(items) == 0 {
		return storedCommitSegment{}, nil, fmt.Errorf("empty commit segment")
	}
	if object.FirstVersion != items[0].Commit.Version || object.LastVersion != items[len(items)-1].Commit.Version {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment version range mismatch")
	}
	if ref.FirstVersion > 0 && ref.FirstVersion != object.FirstVersion {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment first version mismatch")
	}
	if ref.LastVersion > 0 && ref.LastVersion != object.LastVersion {
		return storedCommitSegment{}, nil, fmt.Errorf("commit segment last version mismatch")
	}
	for _, item := range items {
		if store != nil {
			if err := store.validateTenantObjectKey(tenantID, item.Key); err != nil {
				return storedCommitSegment{}, nil, err
			}
		}
		if item.Commit.TenantID != tenantID {
			return storedCommitSegment{}, nil, errTenantCommitMismatch(tenantID, item.Key, item.Commit.TenantID)
		}
		if err := validateCommitObjectIdentity(item.Key, item.Commit); err != nil {
			return storedCommitSegment{}, nil, err
		}
	}
	return object, items, nil
}

func marshalCommitSegmentPayload(items []commitSegmentItem) ([]byte, error) {
	var buf bytes.Buffer
	for _, item := range items {
		item.Commit.LayoutVersion = CurrentObjectLayoutVersion
		data, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

func commitSegmentsEqual(left []CommitSegmentRef, right []CommitSegmentRef) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func commitSegmentRefFromObject(key string, object storedCommitSegment) CommitSegmentRef {
	return CommitSegmentRef{
		Key:          key,
		Codec:        object.Codec,
		FirstVersion: object.FirstVersion,
		LastVersion:  object.LastVersion,
		Count:        object.Count,
		ContentHash:  object.ContentHash,
	}
}

func sortCommitSegmentObjects(items []commitSegmentObject) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Ref.FirstVersion == items[j].Ref.FirstVersion {
			return items[i].Ref.Key < items[j].Ref.Key
		}
		return items[i].Ref.FirstVersion < items[j].Ref.FirstVersion
	})
}
