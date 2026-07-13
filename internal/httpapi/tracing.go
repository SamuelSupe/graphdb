package httpapi

import (
	"errors"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"
	"gitlab.jiagouyun.com/guance/graphdb/internal/query"

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

func queryRequestTraceAttributes(request query.Request) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("graphdb.query.op", request.Op),
		attribute.String("graphdb.query.target_op", request.TargetOp),
		attribute.Int("graphdb.query.filters", len(request.Filters)+len(request.Where)),
		attribute.Int("graphdb.query.edge_filters", len(request.EdgeWhere)),
		attribute.Int("graphdb.query.project_fields", len(request.Project)),
		attribute.Int("graphdb.query.aggregations", len(request.Aggregate)),
		attribute.Int("graphdb.query.group_by_fields", len(request.GroupBy)),
		attribute.Int("graphdb.query.limit", request.Limit),
		attribute.Int("graphdb.query.depth", request.Depth),
		attribute.Int("graphdb.query.timeout_ms", request.TimeoutMS),
		attribute.Int("graphdb.query.cost_limit", request.CostLimit),
		attribute.Int64("graphdb.query.min_version", request.MinVersion),
		attribute.Bool("graphdb.query.allow_stale", request.AllowStale),
		attribute.Bool("graphdb.query.cursor_present", request.Cursor != ""),
		attribute.Bool("graphdb.query.profile", request.Profile),
	}
}

func queryResponseTraceAttributes(response query.Response) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("graphdb.query.version", response.Version),
		attribute.Int("graphdb.query.results", len(response.Results)),
		attribute.Int("graphdb.query.scanned", response.Stats.Scanned),
		attribute.Int("graphdb.query.visited", response.Stats.Visited),
		attribute.Int("graphdb.query.returned", response.Stats.Returned),
		attribute.Int("graphdb.query.cost", response.Stats.Cost),
		attribute.Bool("graphdb.query.timed_out", response.Stats.TimedOut),
		attribute.Bool("graphdb.query.truncated", response.Stats.Truncated),
		attribute.Bool("graphdb.query.next_cursor_present", response.NextCursor != ""),
	}
}
