package storage

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

const (
	indexPageMaxItems              = 1024
	indexRangeRole                 = "range:v1:"
	indexPageMaxBytes        int64 = 8 << 20
	parquetEntityRangeCodec        = "arrow-parquet-entity-ranges-v1"
	parquetEdgeRangeCodec          = "arrow-parquet-edge-ranges-v1"
	parquetEntitySingleCodec       = "arrow-parquet-entity-single-v2"
	parquetEdgeSingleCodec         = "arrow-parquet-edge-single-v2"
)

// Logical shards and their scan order stay unchanged. A large shard owns
// immutable ID ranges, so changing one item rewrites only its bounded range.
// Each range has its own hash; the shard hash covers the ordered manifest.
// Old binaries cannot safely page this layout; writing it requires explicit
// enablement after the reader rollout. Keep parquet references for legacy GC.
// Range metadata lives in the versioned object role so older catalog writers
// preserve it, just as they preserve secondary-index shard roles. The routing
// manifest hash also covers the immutable object reference and its data hash.
type indexRange struct {
	Lower string `json:"lower"`
	Upper string `json:"upper"`
	Bytes int64  `json:"bytes"`
}

func (r indexRange) contains(id string) bool { return id >= r.Lower && (r.Upper == "" || id < r.Upper) }

func encodeRangeRole(r indexRange) string {
	data, _ := json.Marshal(r)
	return indexRangeRole + base64.RawURLEncoding.EncodeToString(data)
}
func rangeRole(lower string) string { return encodeRangeRole(indexRange{Lower: lower}) }
func decodeIndexRange(role string) (indexRange, error) {
	var result indexRange
	if !strings.HasPrefix(role, indexRangeRole) {
		return result, fmt.Errorf("unsupported index range role")
	}
	data, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(role, indexRangeRole))
	if err != nil {
		return result, err
	}
	err = json.Unmarshal(data, &result)
	return result, err
}
func rangeLower(role string) (string, error)               { r, err := decodeIndexRange(role); return r.Lower, err }
func boundedPageObjectID(shard, contentHash string) string { return shard + "_" + contentHash }
func rangeObjectsHash(objects []IndexObject) string {
	type digest struct {
		Role, Key, Format, Codec string
		Rows                     int
		ContentHash, SchemaHash  string
	}
	items := make([]digest, 0, len(objects))
	for _, o := range objects {
		items = append(items, digest{o.Role, o.Key, o.Format, o.Codec, o.RowCount, o.ContentHash, o.SchemaHash})
	}
	return indexContentHash(items)
}
func finishRangeBounds(objects []IndexObject) {
	for i := range objects {
		r, _ := decodeIndexRange(objects[i].Role)
		r.Upper = ""
		if i+1 < len(objects) {
			r.Upper, _ = rangeLower(objects[i+1].Role)
		}
		objects[i].Role = encodeRangeRole(r)
	}
}
func validateRangeObjects(objects []IndexObject, rows int, contentHash string) error {
	if len(objects) == 0 || rows <= 0 || rangeObjectsHash(objects) != contentHash {
		return fmt.Errorf("index range manifest hash mismatch")
	}
	count, previous := 0, ""
	for i, o := range objects {
		r, err := decodeIndexRange(o.Role)
		if err != nil || (i == 0 && r.Lower != "") || (i > 0 && r.Lower != previous) || (r.Upper != "" && r.Upper <= r.Lower) || (i+1 < len(objects) && r.Upper == "") || (i+1 == len(objects) && r.Upper != "") || o.Key == "" || o.RowCount <= 0 || o.RowCount > indexPageMaxItems || o.RowCount > rows-count || o.ContentHash == "" || o.SchemaHash == "" || o.Format != IndexFormatParquet || r.Bytes <= 0 || (r.Bytes > indexPageMaxBytes && o.RowCount != 1) {
			return fmt.Errorf("invalid index range manifest")
		}
		previous = r.Upper
		count += o.RowCount
	}
	if count != rows {
		return fmt.Errorf("index range manifest count mismatch")
	}
	return nil
}
func rangeObjectIndex(objects []IndexObject, id string) int {
	return max(0, sort.Search(len(objects), func(i int) bool { lower, _ := rangeLower(objects[i].Role); return lower > id })-1)
}
func splitBoundedEntityPages(pages []EntityPageData) []EntityPageData {
	var result []EntityPageData
	for _, page := range pages {
		if page.pageRole == "" {
			page.pageRole = rangeRole("")
		}
		if len(page.Entities) <= indexPageMaxItems {
			result = append(result, page)
			continue
		}
		role := page.pageRole
		if role == "" {
			role = rangeRole("")
		}
		parts := (len(page.Entities) + indexPageMaxItems - 1) / indexPageMaxItems
		width := (len(page.Entities) + parts - 1) / parts
		for start := 0; start < len(page.Entities); start += width {
			part := page
			part.Entities = page.Entities[start:min(start+width, len(page.Entities))]
			part.logicalContentHash = ""
			part.pageRole = role
			if start > 0 {
				part.pageRole = rangeRole(part.Entities[0].ID)
			}
			result = append(result, part)
		}
	}
	return result
}

func splitBoundedEdgeShards(shards []EdgeShardData) []EdgeShardData {
	var result []EdgeShardData
	for _, shard := range shards {
		if shard.pageRole == "" {
			shard.pageRole = rangeRole("")
		}
		if len(shard.Edges) <= indexPageMaxItems {
			result = append(result, shard)
			continue
		}
		role := shard.pageRole
		if role == "" {
			role = rangeRole("")
		}
		parts := (len(shard.Edges) + indexPageMaxItems - 1) / indexPageMaxItems
		width := (len(shard.Edges) + parts - 1) / parts
		for start := 0; start < len(shard.Edges); start += width {
			part := shard
			part.Edges = shard.Edges[start:min(start+width, len(shard.Edges))]
			part.logicalContentHash = ""
			part.pageRole = role
			if start > 0 {
				part.pageRole = rangeRole(part.Edges[0].ID)
			}
			result = append(result, part)
		}
	}
	return result
}

func entityRangeSpec(spec EntityPageSpec, object IndexObject) EntityPageSpec {
	object.Role = "page"
	spec.Format = IndexFormatParquet
	spec.Codec = parquetEntityPageCodec
	spec.Objects = []IndexObject{object}
	spec.EntityCount, spec.RowCount = object.RowCount, object.RowCount
	spec.ContentHash, spec.SchemaHash = object.ContentHash, object.SchemaHash
	return spec
}

func edgeRangeSpec(spec EdgeShard, object IndexObject) EdgeShard {
	object.Role = "shard"
	spec.Format = IndexFormatParquet
	spec.Codec = parquetEdgeShardCodec
	spec.Objects = []IndexObject{object}
	spec.EdgeCount, spec.RowCount = object.RowCount, object.RowCount
	spec.ContentHash, spec.SchemaHash = object.ContentHash, object.SchemaHash
	return spec
}

func entityPageSpecs(pages []EntityPageData) []EntityPageSpec {
	var specs []EntityPageSpec
	byShard := map[string]int{}
	for _, page := range pages {
		index, exists := byShard[page.Shard]
		if !exists {
			index = len(specs)
			byShard[page.Shard] = index
			specs = append(specs, EntityPageSpec{Shard: page.Shard, UpdatedAt: page.UpdatedAt})
		}
		spec := &specs[index]
		spec.EntityCount += len(page.Entities)
		spec.estimatedBytes += entityPagePackBytes(page)
		if page.pageRole == "" {
			spec.ContentHash = entityPageContentHash(page)
		} else {
			spec.Format = IndexFormatParquet
			spec.Objects = append(spec.Objects, IndexObject{Role: page.pageRole, RowCount: len(page.Entities), ContentHash: entityPageContentHash(page), SchemaHash: parquetEntityPageSchemaHash()})
		}
	}
	for i := range specs {
		if hasIndexRanges(specs[i].Objects) {
			specs[i].ContentHash = rangeObjectsHash(specs[i].Objects)
		}
	}
	return specs
}

func edgePageSpecs(shards []EdgeShardData, g *graph.Graph) []EdgeShard {
	var specs []EdgeShard
	byKey := map[string]int{}
	for _, shard := range shards {
		key := edgeShardTargetKey(shard.RelationType, shard.Shard)
		index, exists := byKey[key]
		if !exists {
			index = len(specs)
			byKey[key] = index
			specs = append(specs, EdgeShard{RelationType: shard.RelationType, ImpactDirection: relationImpactDirection(g, shard.RelationType), Shard: shard.Shard, UpdatedAt: shard.UpdatedAt})
		}
		spec := &specs[index]
		spec.EdgeCount += len(shard.Edges)
		if shard.pageRole == "" {
			spec.ContentHash = edgeShardContentHash(shard)
		} else {
			spec.Format = IndexFormatParquet
			spec.Objects = append(spec.Objects, IndexObject{Role: shard.pageRole, RowCount: len(shard.Edges), ContentHash: edgeShardContentHash(shard), SchemaHash: parquetEdgeShardSchemaHash()})
		}
	}
	for i := range specs {
		if hasIndexRanges(specs[i].Objects) {
			specs[i].ContentHash = rangeObjectsHash(specs[i].Objects)
		}
	}
	return specs
}

func hasIndexRanges(objects []IndexObject) bool {
	return len(objects) > 0 && strings.HasPrefix(objects[0].Role, "range:")
}
