package codec

import (
	"iter"

	"github.com/SamuelSupe/graphdb/v2/internal/graph"
)

// CITypes visits decoded upserts in wire ordinal order. Missing ordinals stay
// absent so callers can reconstruct sparse ingest item positions.
func (b *CommitBuilder) CITypes() iter.Seq2[int, graph.CIType] {
	return func(yield func(int, graph.CIType) bool) {
		for _, ordinal := range SortedIntKeys(b.ciTypes) {
			if !yield(ordinal, b.ciTypes[ordinal].item) {
				return
			}
		}
	}
}

func (b *CommitBuilder) Relations() iter.Seq2[int, graph.RelationType] {
	return func(yield func(int, graph.RelationType) bool) {
		for _, ordinal := range SortedIntKeys(b.relations) {
			if !yield(ordinal, b.relations[ordinal].item) {
				return
			}
		}
	}
}

// Entities preserves wire ordinals and yields copies owned by the caller.
func (b *CommitBuilder) Entities() iter.Seq2[int, graph.Entity] {
	return func(yield func(int, graph.Entity) bool) {
		for _, ordinal := range SortedIntKeys(b.entities) {
			if !yield(ordinal, DecodedEntityCopy(b.entities[ordinal].item)) {
				return
			}
		}
	}
}

// Edges preserves wire ordinals and yields copies owned by the caller.
func (b *CommitBuilder) Edges() iter.Seq2[int, graph.Edge] {
	return func(yield func(int, graph.Edge) bool) {
		for _, ordinal := range SortedIntKeys(b.edges) {
			if !yield(ordinal, DecodedEdgeCopy(b.edges[ordinal].item)) {
				return
			}
		}
	}
}
