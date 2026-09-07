package badgerbox

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
	"sync"
	"time"
)

type messageTracesKey struct{}
type messageTraces map[MessageID]*messageTrace
type messageTrace struct {
	span    trace.Span
	baggage baggage.Baggage
	ended   sync.Once
}

func newMessageTrace(ctx context.Context, span trace.Span) *messageTrace {
	return &messageTrace{span: span, baggage: baggage.FromContext(ctx)}
}
func (m *messageTrace) end(outcome string, at time.Time) {
	m.ended.Do(func() {
		if outcome != "" {
			m.span.SetAttributes(attribute.String("badgerbox.outcome", outcome))
		}
		m.span.End(trace.WithTimestamp(at))
	})
}

// ContextForMessage attaches the message's processing span and persisted baggage
// to ctx. Batch callbacks should call it before starting per-message work. It
// preserves ctx's values, deadline and cancellation, including callback-added
// timeouts. Unknown IDs and contexts outside a processor are returned unchanged.
func ContextForMessage(ctx context.Context, id MessageID) context.Context {
	if ctx == nil {
		return nil
	}
	traces, _ := ctx.Value(messageTracesKey{}).(messageTraces)
	if m := traces[id]; m != nil {
		return trace.ContextWithSpan(baggage.ContextWithBaggage(ctx, m.baggage), m.span)
	}
	return ctx
}
