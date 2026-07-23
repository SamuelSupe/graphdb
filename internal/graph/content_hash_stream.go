package graph

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
	"sort"
)

// ContentMD5 preserves the logical snapshot JSON encoding while hashing one
// item at a time. This avoids keeping a second full logical graph plus its
// encoded JSON in memory during every write.
func (g *Graph) ContentMD5() (string, error) {
	value, _, err := g.ContentMD5WithLogicalSize()
	return value, err
}

// ContentMD5WithLogicalSize also reports the encoded logical byte count. The
// writer cache uses that stable count as the basis for a conservative memory
// weight without walking or encoding the graph a second time.
func (g *Graph) ContentMD5WithLogicalSize() (string, int64, error) {
	g.logicalHashMu.Lock()
	defer g.logicalHashMu.Unlock()
	if g.logicalHashCache == nil {
		cache, err := buildLogicalHashCache(g)
		if err != nil {
			return "", 0, err
		}
		g.logicalHashCache = cache
	}
	if g.logicalHashCache.finalReady {
		return g.logicalHashCache.digest, g.logicalHashCache.logicalBytes, nil
	}

	digest := &countingHash{Hash: md5.New()}
	buffered := bufio.NewWriterSize(digest, 64*1024)
	_, _ = io.WriteString(buffered, "{")
	firstField := true
	cache := g.logicalHashCache
	for _, field := range []struct {
		name     string
		category logicalHashCategory
	}{
		{name: "ci_types", category: cache.ciTypes},
		{name: "entities", category: cache.entities},
		{name: "relation_types", category: cache.relationTypes},
		{name: "edges", category: cache.edges},
	} {
		writeLogicalHashArray(buffered, &firstField, field.name, field.category)
	}

	_, _ = io.WriteString(buffered, "}")
	if err := buffered.Flush(); err != nil {
		return "", 0, err
	}
	cache.digest = hex.EncodeToString(digest.Sum(nil))
	cache.logicalBytes = digest.written
	cache.finalReady = true
	return cache.digest, cache.logicalBytes, nil
}

type countingHash struct {
	hash.Hash
	written int64
}

func (h *countingHash) Write(data []byte) (int, error) {
	n, err := h.Hash.Write(data)
	h.written += int64(n)
	return n, err
}

func writeLogicalHashArray(digest io.Writer, firstField *bool, name string, category logicalHashCategory) {
	if len(category.keys) == 0 {
		return
	}
	if !*firstField {
		_, _ = io.WriteString(digest, ",")
	}
	*firstField = false
	_, _ = io.WriteString(digest, `"`+name+`":`)
	_, _ = io.WriteString(digest, "[")
	for i, key := range category.keys {
		if i > 0 {
			_, _ = io.WriteString(digest, ",")
		}
		_, _ = digest.Write(category.encoded[key])
	}
	_, _ = io.WriteString(digest, "]")
}

func logicalEntityForHash(entity Entity) logicalEntity {
	mergedFrom := append([]string(nil), entity.MergedFrom...)
	sort.Strings(mergedFrom)
	return logicalEntity{
		ID:              entity.ID,
		Kind:            entity.Kind,
		Fields:          entity.Fields,
		FieldSources:    logicalFieldSources(entity.FieldSources),
		ExistenceSource: logicalOwnerPtr(entity.ExistenceSource),
		Source:          entity.Source,
		ExternalID:      entity.ExternalID,
		Identity:        entity.Identity,
		Confidence:      entity.Confidence,
		SourceRank:      entity.SourceRank,
		Sources:         logicalEntitySources(entity.Sources),
		MergedFrom:      mergedFrom,
		SplitFrom:       entity.SplitFrom,
	}
}

func logicalEdgeForHash(edge Edge) logicalEdge {
	return logicalEdge{
		ID:              edge.ID,
		Type:            edge.Type,
		From:            edge.From,
		To:              edge.To,
		Fields:          edge.Fields,
		FieldSources:    logicalFieldSources(edge.FieldSources),
		Source:          edge.Source,
		ExternalID:      edge.ExternalID,
		Confidence:      edge.Confidence,
		SourceRank:      edge.SourceRank,
		Sources:         logicalEdgeSources(edge.Sources),
		ExistenceSource: logicalOwnerPtr(edge.ExistenceSource),
	}
}
