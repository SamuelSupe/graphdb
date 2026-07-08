package storage

import (
	"context"
	"strings"

	"gitlab.jiagouyun.com/guance/graphdb/internal/graph"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func startStorageSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	return otel.Tracer("graphdb/storage").Start(ctx, name, trace.WithAttributes(attrs...))
}

func endStorageSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

func tenantTraceAttr(tenantID string) attribute.KeyValue {
	return attribute.String("graphdb.tenant", tenantID)
}

func commitOptionTraceAttrs(opts CommitOptions) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Bool("graphdb.commit.expected_version_present", opts.ExpectedVersion != nil),
		attribute.Bool("graphdb.commit.idempotency_key_present", opts.IdempotencyKey != ""),
	}
	if opts.ExpectedVersion != nil {
		attrs = append(attrs, attribute.Int64("graphdb.commit.expected_version", *opts.ExpectedVersion))
	}
	return attrs
}

func mutationTraceAttrs(mutations graph.Mutations) []attribute.KeyValue {
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

func manifestTraceAttrs(prefix string, manifest Manifest) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64(prefix+".version", manifest.Version),
		attribute.Int64(prefix+".snapshot_version", manifest.SnapshotVersion),
		attribute.Bool(prefix+".snapshot_present", manifest.SnapshotKey != "" || manifest.SnapshotCatalogKey != ""),
		attribute.Bool(prefix+".snapshot_catalog_present", manifest.SnapshotCatalogKey != ""),
		attribute.Int(prefix+".commit_keys", len(manifest.CommitKeys)),
		attribute.Int(prefix+".commit_segments", len(manifest.CommitSegments)),
		attribute.Int(prefix+".commit_tail_length", manifestCommitTailLength(manifest)),
	}
}

func graphTraceAttrs(prefix string, g *graph.Graph) []attribute.KeyValue {
	if g == nil {
		return []attribute.KeyValue{
			attribute.Int(prefix+".entities", 0),
			attribute.Int(prefix+".edges", 0),
		}
	}
	return []attribute.KeyValue{
		attribute.Int64(prefix+".version", g.Version),
		attribute.Int(prefix+".entities", len(g.Entities)),
		attribute.Int(prefix+".edges", len(g.Edges)),
		attribute.Int(prefix+".ci_types", len(g.CITypes)),
		attribute.Int(prefix+".relation_types", len(g.RelationTypes)),
	}
}

func objectKeyKind(key string) string {
	switch {
	case key == "":
		return "unknown"
	case strings.Contains(key, "/manifests/"):
		return "manifest"
	case strings.Contains(key, "/commits/segments/"):
		return "commit_segment"
	case strings.Contains(key, "/commits/"):
		return "commit"
	case strings.Contains(key, "/snapshots/sharded/") && strings.HasSuffix(key, "/catalog.parquet"):
		return "snapshot_catalog"
	case strings.Contains(key, "/snapshots/sharded/") && strings.Contains(key, "/entities/pages/"):
		return "snapshot_entity_page"
	case strings.Contains(key, "/snapshots/sharded/") && strings.Contains(key, "/edges/"):
		return "snapshot_edge_shard"
	case strings.Contains(key, "/snapshots/"):
		return "snapshot"
	case strings.Contains(key, "/idempotency/"):
		return "idempotency"
	case strings.Contains(key, "/indexes/"):
		return "index"
	case strings.Contains(key, "/tasks/"):
		return "task"
	case strings.Contains(key, "/reader-heartbeats/"):
		return "reader_heartbeat"
	case strings.Contains(key, "/config/"):
		return "config"
	default:
		return "other"
	}
}
