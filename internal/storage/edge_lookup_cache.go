package storage

import "github.com/SamuelSupe/graphdb/v2/internal/graph"

const defaultEdgeLookupCacheMaxBytes = int64(256 << 20)

type edgeLookupCache = decodedLookupCache[map[string][]graph.Edge]

func newEdgeLookupCache(max int, maxBytes int64) *edgeLookupCache {
	return newDecodedLookupCache(max, maxBytes, estimateEdgeLookupBytes)
}
func configuredEdgeLookupCache(max int, rawMaxBytes int64) *edgeLookupCache {
	maxBytes := rawMaxBytes / 4
	if rawMaxBytes <= 0 {
		maxBytes = indexCacheByteLimit(max, defaultEdgeLookupCacheMaxBytes) / 4
	}
	return newEdgeLookupCache(max, maxBytes)
}
func estimateEdgeLookupBytes(byEntity map[string][]graph.Edge) int64 {
	size := int64(128)
	for entityID, edges := range byEntity {
		size += 64 + int64(len(entityID))
		for _, edge := range edges {
			size += 320 + int64(
				len(edge.ID)+len(edge.Type)+len(edge.From)+len(edge.To)+
					len(edge.Source)+len(edge.ExternalID),
			)
			size += estimateAnyMap(edge.Fields)
			for field, source := range edge.FieldSources {
				size += 96 + int64(len(field)+len(source.Source))
			}
			for _, source := range edge.Sources {
				size += 96 + int64(
					len(source.Source)+len(source.ExternalID)+len(source.EdgeID),
				)
			}
		}
	}
	return size
}
