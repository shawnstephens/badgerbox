package badgerbox

import (
	"context"
	"go.opentelemetry.io/otel/metric"
	"time"
)

func (o *otelInstrumentation) initExtras(m metric.Meter) error {
	o.extraCounters = make(map[string]metric.Int64Counter)
	o.extraDurations = make(map[string]metric.Float64Histogram)
	for _, name := range []string{"process_batch_total", "batch_result_missing_total", "batch_result_invalid_total", "kafka_produce_total", "kafka_produce_error_total", "kafka_flush_error_total", "snapshot_error_total"} {
		v, err := m.Int64Counter("badgerbox_" + name)
		if err != nil {
			return err
		}
		o.extraCounters[name] = v
	}
	for _, name := range []string{"process_batch_duration_seconds", "process_batch_size", "kafka_promise_duration_seconds", "kafka_flush_duration_seconds", "snapshot_duration_seconds"} {
		v, err := m.Float64Histogram("badgerbox_" + name)
		if err != nil {
			return err
		}
		o.extraDurations[name] = v
	}
	return nil
}
func (o *otelInstrumentation) count(ctx context.Context, name string, n int) {
	if m := o.extraCounters[name]; m != nil {
		m.Add(ctx, int64(n), metric.WithAttributes(namespaceAttributes(o.namespace)...))
	}
}
func (o *otelInstrumentation) observe(ctx context.Context, name string, n float64) {
	if m := o.extraDurations[name]; m != nil {
		m.Record(ctx, n, metric.WithAttributes(namespaceAttributes(o.namespace)...))
	}
}
func (o *otelInstrumentation) recordProcessBatch(ctx context.Context, n int, d time.Duration) {
	o.count(ctx, "process_batch_total", 1)
	o.observe(ctx, "process_batch_size", float64(n))
	o.observe(ctx, "process_batch_duration_seconds", d.Seconds())
}
func (o *otelInstrumentation) recordProcessBatchResultInvalid(ctx context.Context, n int) {
	o.count(ctx, "batch_result_invalid_total", n)
}
func (o *otelInstrumentation) recordProcessBatchResultMissing(ctx context.Context, n int) {
	o.count(ctx, "batch_result_missing_total", n)
}

type instrumentationContextKey struct{}

func contextWithOTelInstrumentation(ctx context.Context, o *otelInstrumentation) context.Context {
	return context.WithValue(ctx, instrumentationContextKey{}, o)
}
