package storage

import (
	"context"
	"errors"
)

type IndexInspection struct {
	TenantID string                  `json:"tenant_id"`
	Version  int64                   `json:"version"`
	Objects  []IndexInspectionObject `json:"objects"`
}

type IndexInspectionObject struct {
	Role            string `json:"role"`
	Key             string `json:"key"`
	ObjectKind      string `json:"object_kind,omitempty"`
	Format          string `json:"format,omitempty"`
	Codec           string `json:"codec,omitempty"`
	RowCount        int    `json:"row_count,omitempty"`
	FirstVersion    int64  `json:"first_version,omitempty"`
	LastVersion     int64  `json:"last_version,omitempty"`
	PayloadCodec    string `json:"payload_codec,omitempty"`
	PayloadBytes    int    `json:"payload_bytes,omitempty"`
	Size            int64  `json:"size"`
	ContentHash     string `json:"content_hash,omitempty"`
	ExpectedHash    string `json:"expected_hash,omitempty"`
	HashMatches     bool   `json:"hash_matches,omitempty"`
	SchemaHash      string `json:"schema_hash,omitempty"`
	InspectionError string `json:"inspection_error,omitempty"`
}

func (s *TenantStore) InspectIndex(ctx context.Context, tenantID string) (IndexInspection, error) {
	if err := ValidateTenantID(tenantID); err != nil {
		return IndexInspection{}, err
	}
	catalog, err := s.GetIndexCatalog(ctx, tenantID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return IndexInspection{}, err
	}
	if errors.Is(err, ErrNotFound) {
		catalog = IndexCatalog{TenantID: tenantID}
	}
	manifest, _, err := s.getManifest(ctx, tenantID)
	if err != nil {
		return IndexInspection{}, err
	}
	objects := s.catalogInspectionObjects(tenantID, catalog)
	objects = append(objects, commitSegmentInspectionObjects(manifest)...)
	version := catalog.Version
	if version == 0 {
		version = manifest.Version
	}
	out := IndexInspection{TenantID: tenantID, Version: version, Objects: make([]IndexInspectionObject, 0, len(objects))}
	for _, object := range objects {
		item := IndexInspectionObject{
			Role:         object.Role,
			Key:          object.Key,
			ObjectKind:   object.Role,
			Format:       specFormat(object.Format),
			Codec:        object.Codec,
			RowCount:     object.RowCount,
			ExpectedHash: object.ContentHash,
			SchemaHash:   object.SchemaHash,
		}
		data, err := s.Objects.Get(ctx, object.Key)
		if errors.Is(err, ErrNotFound) {
			item.InspectionError = "object not found"
			out.Objects = append(out.Objects, item)
			continue
		}
		if err != nil {
			item.InspectionError = err.Error()
			out.Objects = append(out.Objects, item)
			continue
		}
		item.Size = int64(len(data))
		if object.Role == "commit_segment" {
			s.inspectCommitSegmentObject(ctx, data, tenantID, object, &item)
		} else if item.Format == IndexFormatParquet {
			s.inspectParquetObject(ctx, data, tenantID, object, &item)
		} else {
			item.InspectionError = "unsupported non-parquet index object"
		}
		out.Objects = append(out.Objects, item)
	}
	return out, nil
}

func commitSegmentInspectionObjects(manifest Manifest) []IndexObject {
	out := make([]IndexObject, 0, len(manifest.CommitSegments))
	for _, segment := range manifest.CommitSegments {
		out = append(out, IndexObject{
			Role:        "commit_segment",
			Key:         segment.Key,
			Format:      IndexFormatParquet,
			Codec:       segment.Codec,
			RowCount:    segment.Count,
			ContentHash: segment.ContentHash,
		})
	}
	return out
}

func (s *TenantStore) inspectCommitSegmentObject(ctx context.Context, data []byte, tenantID string, object IndexObject, item *IndexInspectionObject) {
	ref := CommitSegmentRef{
		Key:         object.Key,
		Codec:       object.Codec,
		Count:       object.RowCount,
		ContentHash: object.ContentHash,
	}
	segment, _, err := decodeCommitSegmentObject(ctx, data, tenantID, ref, s)
	if err != nil {
		item.InspectionError = err.Error()
		return
	}
	item.ObjectKind = segment.Kind
	item.Codec = segment.Codec
	item.RowCount = segment.Count
	item.FirstVersion = segment.FirstVersion
	item.LastVersion = segment.LastVersion
	item.PayloadBytes = segment.PayloadBytes
	item.ContentHash = segment.ContentHash
	item.HashMatches = object.ContentHash != "" && object.ContentHash == segment.ContentHash
}

func (s *TenantStore) catalogInspectionObjects(tenantID string, catalog IndexCatalog) []IndexObject {
	seen := map[string]struct{}{}
	out := []IndexObject{}
	add := func(object IndexObject, identity string) {
		if object.Key == "" {
			return
		}
		if specFormat(object.Format) != IndexFormatParquet {
			return
		}
		if identity == "" {
			identity = object.Key
		}
		if _, ok := seen[identity]; ok {
			return
		}
		seen[identity] = struct{}{}
		out = append(out, object)
	}
	for _, index := range catalog.Indexes {
		if len(index.Objects) > 0 {
			for _, object := range index.Objects {
				add(object, object.Key)
			}
		}
	}
	for _, shard := range catalog.EdgeShards {
		if len(shard.Objects) > 0 {
			for _, object := range shard.Objects {
				object.inspectRelationType = shard.RelationType
				object.inspectShard = shard.Shard
				add(object, object.Key+"\x00edge\x00"+shard.RelationType+"\x00"+shard.Shard)
			}
		}
	}
	for _, page := range catalog.EntityPages {
		if len(page.Objects) > 0 {
			for _, object := range page.Objects {
				object.inspectShard = page.Shard
				add(object, object.Key+"\x00entity\x00"+page.Shard)
			}
		}
	}
	return out
}
