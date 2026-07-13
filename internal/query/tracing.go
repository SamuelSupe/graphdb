package query

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func startQueryOperatorSpan(ctx context.Context, name string, detail string, cost int) (context.Context, trace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	resource := strings.ReplaceAll(name, "-", "_")
	attrs := []attribute.KeyValue{
		attribute.String("graphdb.query.operator", name),
		attribute.Int("graphdb.query.operator.estimated_cost", cost),
	}
	if detail != "" {
		attrs = append(attrs, attribute.String("graphdb.query.operator.detail", detail))
	}
	return otel.Tracer("graphdb/query").Start(ctx, "graphdb.query.operator."+resource, trace.WithAttributes(attrs...))
}

func endQueryOperatorSpan(span trace.Span, err error) {
	if span == nil {
		return
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
