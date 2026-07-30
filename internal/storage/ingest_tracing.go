package storage

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type walTraceContext struct {
	TraceID    string `json:"trace_id,omitempty"`
	SpanID     string `json:"span_id,omitempty"`
	TraceFlags byte   `json:"trace_flags,omitempty"`
	TraceState string `json:"trace_state,omitempty"`
}

func captureWALTraceContext(ctx context.Context) walTraceContext {
	spanContext := trace.SpanContextFromContext(ctx)
	if !spanContext.IsValid() {
		return walTraceContext{}
	}
	return walTraceContext{
		TraceID:    spanContext.TraceID().String(),
		SpanID:     spanContext.SpanID().String(),
		TraceFlags: byte(spanContext.TraceFlags()),
		TraceState: spanContext.TraceState().String(),
	}
}

func (c walTraceContext) spanContext() trace.SpanContext {
	traceID, traceErr := trace.TraceIDFromHex(c.TraceID)
	spanID, spanErr := trace.SpanIDFromHex(c.SpanID)
	traceState, stateErr := trace.ParseTraceState(c.TraceState)
	if traceErr != nil || spanErr != nil || stateErr != nil {
		return trace.SpanContext{}
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.TraceFlags(c.TraceFlags),
		TraceState: traceState,
		Remote:     true,
	})
}

func startIngestFlushSpan(
	ctx context.Context,
	tenantID string,
	items []*ingestPending,
) (context.Context, trace.Span) {
	firstLSN, lastLSN := ingestPendingLSNRange(items)
	links := make([]trace.Link, 0, len(items))
	for _, item := range items {
		spanContext := item.envelope.Trace.spanContext()
		if spanContext.IsValid() {
			links = append(links, trace.Link{SpanContext: spanContext})
		}
	}
	return otel.Tracer("graphdb/storage").Start(
		ctx,
		"graphdb.storage.ingest.flush",
		trace.WithLinks(links...),
		trace.WithAttributes(
			tenantTraceAttr(tenantID),
			attribute.Int("graphdb.ingest.flush.requests", len(items)),
			attribute.Int64("graphdb.ingest.flush.first_lsn", int64(firstLSN)),
			attribute.Int64("graphdb.ingest.flush.last_lsn", int64(lastLSN)),
		),
	)
}

func ingestPendingLSNRange(items []*ingestPending) (uint64, uint64) {
	if len(items) == 0 {
		return 0, 0
	}
	first := items[0].acceptedLSN
	last := first
	for _, item := range items[1:] {
		first = min(first, item.acceptedLSN)
		last = max(last, item.acceptedLSN)
	}
	return first, last
}

func startIngestWALGroupSpan(
	requests []ingestWALAppendRequest,
	bytes int,
	durability string,
) trace.Span {
	links := make([]trace.Link, 0, len(requests))
	for _, request := range requests {
		spanContext := trace.SpanContextFromContext(request.ctx)
		if spanContext.IsValid() {
			links = append(links, trace.Link{SpanContext: spanContext})
		}
	}
	_, span := otel.Tracer("graphdb/storage").Start(
		context.Background(),
		"graphdb.storage.ingest_wal.write_group",
		trace.WithLinks(links...),
		trace.WithAttributes(
			attribute.Int("graphdb.ingest.wal.group_records", len(requests)),
			attribute.Int("graphdb.ingest.wal.group_bytes", bytes),
			attribute.String("graphdb.ingest.wal.durability", durability),
		),
	)
	return span
}

func ingestTraceLogFields(envelope walIngestEnvelope) map[string]any {
	fields := map[string]any{}
	if envelope.Trace.TraceID != "" {
		fields["trace_id"] = envelope.Trace.TraceID
	}
	if envelope.Trace.SpanID != "" {
		fields["span_id"] = envelope.Trace.SpanID
	}
	return fields
}
