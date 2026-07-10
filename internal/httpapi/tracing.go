package httpapi

import (
	"errors"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func endHTTPSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func traceError(message string) error {
	return errors.New(message)
}

func commitRequestAttributes(request CommitRequest) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Bool("graphdb.commit.expected_version_present", request.ExpectedVersion != nil),
		attribute.Bool("graphdb.commit.idempotency_key_present", request.IdempotencyKey != ""),
	}
	if request.ExpectedVersion != nil {
		attrs = append(attrs, attribute.Int64("graphdb.commit.expected_version", *request.ExpectedVersion))
	}
	return append(attrs, mutationAttributes(request.Mutations)...)
}

func mutationAttributes(mutations graph.Mutations) []attribute.KeyValue {
	total := len(mutations.UpsertCITypes) +
		len(mutations.DeleteCITypes) +
		len(mutations.UpsertRelationTypes) +
		len(mutations.DeleteRelationTypes) +
		len(mutations.UpsertEntities) +
		len(mutations.DeleteEntities) +
		len(mutations.DeleteEntityRequests) +
		len(mutations.MarkSourceStale) +
		len(mutations.UpsertEdges) +
		len(mutations.DeleteEdges) +
		len(mutations.DeleteEdgeRequests) +
		len(mutations.MergeEntities) +
		len(mutations.SplitEntities)
	return []attribute.KeyValue{
		attribute.Int("graphdb.mutations.total", total),
		attribute.Int("graphdb.mutations.upsert_ci_types", len(mutations.UpsertCITypes)),
		attribute.Int("graphdb.mutations.delete_ci_types", len(mutations.DeleteCITypes)),
		attribute.Int("graphdb.mutations.upsert_relation_types", len(mutations.UpsertRelationTypes)),
		attribute.Int("graphdb.mutations.delete_relation_types", len(mutations.DeleteRelationTypes)),
		attribute.Int("graphdb.mutations.upsert_entities", len(mutations.UpsertEntities)),
		attribute.Int("graphdb.mutations.delete_entities", len(mutations.DeleteEntities)),
		attribute.Int("graphdb.mutations.delete_entity_requests", len(mutations.DeleteEntityRequests)),
		attribute.Int("graphdb.mutations.mark_source_stale", len(mutations.MarkSourceStale)),
		attribute.Int("graphdb.mutations.upsert_edges", len(mutations.UpsertEdges)),
		attribute.Int("graphdb.mutations.delete_edges", len(mutations.DeleteEdges)),
		attribute.Int("graphdb.mutations.delete_edge_requests", len(mutations.DeleteEdgeRequests)),
		attribute.Int("graphdb.mutations.merge_entities", len(mutations.MergeEntities)),
		attribute.Int("graphdb.mutations.split_entities", len(mutations.SplitEntities)),
	}
}
