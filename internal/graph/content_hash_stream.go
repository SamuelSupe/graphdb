package graph

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
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
	digest := &countingHash{Hash: md5.New()}
	encoder := json.NewEncoder(trimJSONNewlineWriter{digest: digest})
	_, _ = io.WriteString(digest, "{")
	firstField := true

	ciTypeKeys := make([]string, 0, len(g.CITypes))
	for key := range g.CITypes {
		ciTypeKeys = append(ciTypeKeys, key)
	}
	sort.Slice(ciTypeKeys, func(i, j int) bool {
		return g.CITypes[ciTypeKeys[i]].Name < g.CITypes[ciTypeKeys[j]].Name
	})
	if err := writeLogicalHashArray(digest, &firstField, "ci_types", ciTypeKeys, func(key string) error {
		return encoder.Encode(g.CITypes[key])
	}); err != nil {
		return "", 0, err
	}

	entityKeys := make([]string, 0, len(g.Entities))
	for key := range g.Entities {
		entityKeys = append(entityKeys, key)
	}
	sort.Slice(entityKeys, func(i, j int) bool {
		return g.Entities[entityKeys[i]].ID < g.Entities[entityKeys[j]].ID
	})
	if err := writeLogicalHashArray(digest, &firstField, "entities", entityKeys, func(key string) error {
		return encoder.Encode(logicalEntityForHash(g.Entities[key]))
	}); err != nil {
		return "", 0, err
	}

	relationTypeKeys := make([]string, 0, len(g.RelationTypes))
	for key := range g.RelationTypes {
		relationTypeKeys = append(relationTypeKeys, key)
	}
	sort.Slice(relationTypeKeys, func(i, j int) bool {
		return g.RelationTypes[relationTypeKeys[i]].Name < g.RelationTypes[relationTypeKeys[j]].Name
	})
	if err := writeLogicalHashArray(digest, &firstField, "relation_types", relationTypeKeys, func(key string) error {
		return encoder.Encode(g.RelationTypes[key])
	}); err != nil {
		return "", 0, err
	}

	edgeKeys := make([]string, 0, len(g.Edges))
	for key := range g.Edges {
		edgeKeys = append(edgeKeys, key)
	}
	sort.Slice(edgeKeys, func(i, j int) bool {
		return g.Edges[edgeKeys[i]].ID < g.Edges[edgeKeys[j]].ID
	})
	if err := writeLogicalHashArray(digest, &firstField, "edges", edgeKeys, func(key string) error {
		return encoder.Encode(logicalEdgeForHash(g.Edges[key]))
	}); err != nil {
		return "", 0, err
	}

	_, _ = io.WriteString(digest, "}")
	return hex.EncodeToString(digest.Sum(nil)), digest.written, nil
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

func writeLogicalHashArray(digest hash.Hash, firstField *bool, name string, keys []string, encode func(string) error) error {
	if len(keys) == 0 {
		return nil
	}
	if !*firstField {
		_, _ = io.WriteString(digest, ",")
	}
	*firstField = false
	_, _ = io.WriteString(digest, `"`+name+`":`)
	_, _ = io.WriteString(digest, "[")
	for i, key := range keys {
		if i > 0 {
			_, _ = io.WriteString(digest, ",")
		}
		if err := encode(key); err != nil {
			return err
		}
	}
	_, _ = io.WriteString(digest, "]")
	return nil
}

// json.Encoder matches json.Marshal's compact encoding but appends one newline.
// The wrapper removes only that framing byte while reporting the full write.
type trimJSONNewlineWriter struct {
	digest hash.Hash
}

func (w trimJSONNewlineWriter) Write(data []byte) (int, error) {
	if len(data) > 0 && data[len(data)-1] == '\n' {
		_, _ = w.digest.Write(data[:len(data)-1])
		return len(data), nil
	}
	_, _ = w.digest.Write(data)
	return len(data), nil
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
