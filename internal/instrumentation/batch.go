package instrumentation

import (
	"context"

	"go.opentelemetry.io/otel/metric"
	"time"
)

func (o *Queue) InitExtras(m metric.Meter) error {
	o.extraCounters = make(map[string]metric.Int64Counter)
	o.extraDurations = make(map[string]metric.Float64Histogram)
	for _, name := range []string{"process_batch_total", "batch_result_missing_total", "batch_result_invalid_total", "kafka_produce_total", "kafka_produce_error_total", "snapshot_error_total"} {
		v, err := m.Int64Counter("badgerbox_" + name)
		if err != nil {
			return err
		}
		o.extraCounters[name] = v
	}
	for _, name := range []string{"process_batch_duration_seconds", "process_batch_size", "kafka_promise_duration_seconds", "snapshot_duration_seconds"} {
		v, err := m.Float64Histogram("badgerbox_" + name)
		if err != nil {
			return err
		}
		o.extraDurations[name] = v
	}
	return nil
}
func (o *Queue) Count(ctx context.Context, name string, n int) {
	if m := o.extraCounters[name]; m != nil {
		m.Add(ctx, int64(n), metric.WithAttributes(namespaceAttributes(o.namespace)...))
	}
}
func (o *Queue) Observe(ctx context.Context, name string, n float64) {
	if m := o.extraDurations[name]; m != nil {
		m.Record(ctx, n, metric.WithAttributes(namespaceAttributes(o.namespace)...))
	}
}
func (o *Queue) RecordProcessBatch(ctx context.Context, n int, d time.Duration) {
	o.Count(ctx, "process_batch_total", 1)
	o.Observe(ctx, "process_batch_size", float64(n))
	o.Observe(ctx, "process_batch_duration_seconds", d.Seconds())
}
func (o *Queue) RecordProcessBatchResultInvalid(ctx context.Context, n int) {
	o.Count(ctx, "batch_result_invalid_total", n)
}
func (o *Queue) RecordProcessBatchResultMissing(ctx context.Context, n int) {
	o.Count(ctx, "batch_result_missing_total", n)
}

func (o *Queue) WorkQueuedBatch(n int)   { o.workDepth.Add(int64(n)) }
func (o *Queue) WorkDequeuedBatch(n int) { o.workDepth.Add(-int64(n)) }
func (o *Queue) WorkStartedBatch(n int)  { o.WorkDequeuedBatch(n); o.activeWorkers.Add(1) }

func (o *Queue) RecordKafkaProduce(ctx context.Context, n, errors int) {
	o.Count(ctx, "kafka_produce_total", n)
	o.Count(ctx, "kafka_produce_error_total", errors)
}
func (o *Queue) RecordKafkaPromise(ctx context.Context, d time.Duration, err error) {
	o.Observe(ctx, "kafka_promise_duration_seconds", d.Seconds())
	if err != nil {
		o.Count(ctx, "kafka_produce_error_total", 1)
	}
}
