package badgerbox

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	nooptrace "go.opentelemetry.io/otel/trace/noop"
)

const (
	metricModeExpired = "expired"
	metricModeManual  = "manual"
	metricModeRetry   = "retry"
)

const (
	metricOutcomeCommitted  = "committed"
	metricOutcomeDeadLetter = "dead_letter"
	metricOutcomePrepared   = "prepared"
	metricOutcomeRetried    = "retried"
	metricOutcomeSuccess    = "success"
)

const defaultInstrumentationName = "github.com/shawnstephens/badgerbox"

const metricsPollInterval = 5 * time.Second

type ObservabilityOptions struct {
	MeterProvider  metric.MeterProvider
	TracerProvider oteltrace.TracerProvider
	Propagator     propagation.TextMapPropagator
	MeterName      string
	TracerName     string
}

type queueSnapshot struct {
	ReadyDepth          int64
	ProcessingDepth     int64
	DeadLetterDepth     int64
	OldestReadyAge      time.Duration
	OldestProcessingAge time.Duration
}

func (s *Store[M, D]) queueSnapshot(ctx context.Context) (queueSnapshot, error) {
	if err := s.ensureOpen(); err != nil {
		return queueSnapshot{}, err
	}
	if err := ctxErr(ctx); err != nil {
		return queueSnapshot{}, err
	}

	var snapshot queueSnapshot
	now := time.Now().UTC()
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		var oldestReadyCreated int64
		var oldestProcessingCreated int64

		for it.Seek(s.keys.readyPrefix); it.ValidForPrefix(s.keys.readyPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			availableAt, id, err := parseTimeAndIDKey(s.keys.readyPrefix, key)
			if err != nil {
				return err
			}

			record, err := s.loadRecord(txn, id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusPending || record.AvailableAtUnix != availableAt.UnixNano() {
				continue
			}

			snapshot.ReadyDepth++
			if oldestReadyCreated == 0 || record.CreatedAtUnix < oldestReadyCreated {
				oldestReadyCreated = record.CreatedAtUnix
			}
		}

		for it.Seek(s.keys.processingPrefix); it.ValidForPrefix(s.keys.processingPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			leaseUntil, id, err := parseTimeAndIDKey(s.keys.processingPrefix, key)
			if err != nil {
				return err
			}
			tokenBytes, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}

			record, err := s.loadRecord(txn, id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusProcessing || record.LeaseToken != string(tokenBytes) || record.LeaseUntilUnix != leaseUntil.UnixNano() {
				continue
			}

			snapshot.ProcessingDepth++
			if oldestProcessingCreated == 0 || record.CreatedAtUnix < oldestProcessingCreated {
				oldestProcessingCreated = record.CreatedAtUnix
			}
		}

		for it.Seek(s.keys.deadLetterPrefix); it.ValidForPrefix(s.keys.deadLetterPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			snapshot.DeadLetterDepth++
		}

		if oldestReadyCreated > 0 {
			snapshot.OldestReadyAge = now.Sub(time.Unix(0, oldestReadyCreated).UTC())
		}
		if oldestProcessingCreated > 0 {
			snapshot.OldestProcessingAge = now.Sub(time.Unix(0, oldestProcessingCreated).UTC())
		}
		return nil
	})
	return snapshot, err
}

func failureKind(err error) string {
	switch {
	case err == nil:
		return ""
	case IsPermanent(err):
		return "permanent"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "context"
	default:
		return "error"
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func normalizedAvailableAt(now time.Time, availableAt time.Time) time.Time {
	if availableAt.IsZero() {
		return now.UTC()
	}
	return availableAt.UTC()
}

func positiveDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}

type otelInstrumentation struct {
	namespace     string
	queueSnapshot func(context.Context) (queueSnapshot, error)

	tracer     oteltrace.Tracer
	propagator propagation.TextMapPropagator

	closeMu  sync.Mutex
	closeErr error
	cancel   context.CancelFunc
	done     chan struct{}

	activeWorkers atomic.Int64
	workDepth     atomic.Int64

	enqueueTotal       metric.Int64Counter
	claimTotal         metric.Int64Counter
	processTotal       metric.Int64Counter
	deadLetterTotal    metric.Int64Counter
	requeueTotal       metric.Int64Counter
	conflictRetryTotal metric.Int64Counter

	enqueueDuration metric.Float64Histogram
	processDuration metric.Float64Histogram
	scheduleLag     metric.Float64Histogram
	messageAge      metric.Float64Histogram
	retryDelay      metric.Float64Histogram
	claimBatchSize  metric.Int64Histogram

	readyDepth          metric.Int64Gauge
	processingDepth     metric.Int64Gauge
	deadLetterDepth     metric.Int64Gauge
	activeWorkersGauge  metric.Int64Gauge
	workChannelDepth    metric.Int64Gauge
	oldestReadyAge      metric.Float64Gauge
	oldestProcessingAge metric.Float64Gauge
}

func newOTelInstrumentation(opts ObservabilityOptions, namespace string, queueSnapshot func(context.Context) (queueSnapshot, error)) (*otelInstrumentation, error) {
	tracerName := opts.TracerName
	if tracerName == "" {
		tracerName = defaultInstrumentationName
	}
	tracerProvider := opts.TracerProvider
	if tracerProvider == nil {
		tracerProvider = nooptrace.NewTracerProvider()
	}

	propagator := opts.Propagator
	if propagator == nil {
		propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	}

	inst := &otelInstrumentation{
		namespace:     namespace,
		queueSnapshot: queueSnapshot,
		tracer:        tracerProvider.Tracer(tracerName),
		propagator:    propagator,
	}

	if opts.MeterProvider == nil {
		return inst, nil
	}

	meterName := opts.MeterName
	if meterName == "" {
		meterName = defaultInstrumentationName
	}
	meter := opts.MeterProvider.Meter(meterName)

	var err error
	if inst.enqueueTotal, err = meter.Int64Counter("badgerbox_enqueue_total"); err != nil {
		return nil, err
	}
	if inst.claimTotal, err = meter.Int64Counter("badgerbox_claim_total"); err != nil {
		return nil, err
	}
	if inst.processTotal, err = meter.Int64Counter("badgerbox_process_attempt_total"); err != nil {
		return nil, err
	}
	if inst.deadLetterTotal, err = meter.Int64Counter("badgerbox_dead_letter_total"); err != nil {
		return nil, err
	}
	if inst.requeueTotal, err = meter.Int64Counter("badgerbox_requeue_total"); err != nil {
		return nil, err
	}
	if inst.conflictRetryTotal, err = meter.Int64Counter("badgerbox_conflict_retry_total"); err != nil {
		return nil, err
	}
	if inst.enqueueDuration, err = meter.Float64Histogram("badgerbox_enqueue_duration_seconds"); err != nil {
		return nil, err
	}
	if inst.processDuration, err = meter.Float64Histogram("badgerbox_process_duration_seconds"); err != nil {
		return nil, err
	}
	if inst.scheduleLag, err = meter.Float64Histogram("badgerbox_schedule_lag_seconds"); err != nil {
		return nil, err
	}
	if inst.messageAge, err = meter.Float64Histogram("badgerbox_message_age_seconds"); err != nil {
		return nil, err
	}
	if inst.retryDelay, err = meter.Float64Histogram("badgerbox_retry_delay_seconds"); err != nil {
		return nil, err
	}
	if inst.claimBatchSize, err = meter.Int64Histogram("badgerbox_claim_batch_size"); err != nil {
		return nil, err
	}
	if inst.readyDepth, err = meter.Int64Gauge("badgerbox_queue_ready"); err != nil {
		return nil, err
	}
	if inst.processingDepth, err = meter.Int64Gauge("badgerbox_queue_processing"); err != nil {
		return nil, err
	}
	if inst.deadLetterDepth, err = meter.Int64Gauge("badgerbox_queue_dead_letter"); err != nil {
		return nil, err
	}
	if inst.activeWorkersGauge, err = meter.Int64Gauge("badgerbox_workers_active"); err != nil {
		return nil, err
	}
	if inst.workChannelDepth, err = meter.Int64Gauge("badgerbox_work_channel_depth"); err != nil {
		return nil, err
	}
	if inst.oldestReadyAge, err = meter.Float64Gauge("badgerbox_queue_oldest_ready_age_seconds"); err != nil {
		return nil, err
	}
	if inst.oldestProcessingAge, err = meter.Float64Gauge("badgerbox_queue_oldest_processing_age_seconds"); err != nil {
		return nil, err
	}

	if err := inst.recordSnapshot(context.Background()); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	inst.cancel = cancel
	inst.done = make(chan struct{})
	go inst.pollSnapshots(ctx)

	return inst, nil
}

func (o *otelInstrumentation) close() error {
	o.closeMu.Lock()
	cancel := o.cancel
	done := o.done
	o.cancel = nil
	o.closeMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	o.closeMu.Lock()
	defer o.closeMu.Unlock()
	return o.closeErr
}

func (o *otelInstrumentation) pollSnapshots(ctx context.Context) {
	defer close(o.done)

	ticker := time.NewTicker(metricsPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := o.recordSnapshot(ctx); err != nil {
				o.closeMu.Lock()
				o.closeErr = errors.Join(o.closeErr, err)
				o.closeMu.Unlock()
			}
		}
	}
}

func (o *otelInstrumentation) recordSnapshot(ctx context.Context) error {
	if o.readyDepth == nil {
		return nil
	}

	var recordErr error
	attrs := namespaceAttributes(o.namespace)

	if o.queueSnapshot != nil {
		snapshot, err := o.queueSnapshot(ctx)
		if err != nil {
			recordErr = errors.Join(recordErr, err)
		} else {
			o.readyDepth.Record(ctx, snapshot.ReadyDepth, metric.WithAttributes(attrs...))
			o.processingDepth.Record(ctx, snapshot.ProcessingDepth, metric.WithAttributes(attrs...))
			o.deadLetterDepth.Record(ctx, snapshot.DeadLetterDepth, metric.WithAttributes(attrs...))
			o.oldestReadyAge.Record(ctx, snapshot.OldestReadyAge.Seconds(), metric.WithAttributes(attrs...))
			o.oldestProcessingAge.Record(ctx, snapshot.OldestProcessingAge.Seconds(), metric.WithAttributes(attrs...))
		}
	}

	o.activeWorkersGauge.Record(ctx, o.activeWorkers.Load(), metric.WithAttributes(attrs...))
	o.workChannelDepth.Record(ctx, o.workDepth.Load(), metric.WithAttributes(attrs...))

	return recordErr
}

func (o *otelInstrumentation) recordConflictRetry(ctx context.Context) {
	if o.conflictRetryTotal == nil {
		return
	}
	o.conflictRetryTotal.Add(ctx, 1, metric.WithAttributes(namespaceAttributes(o.namespace)...))
}

func (o *otelInstrumentation) recordEnqueueCommitted(ctx context.Context, duration time.Duration) {
	o.recordEnqueue(ctx, metricOutcomeCommitted, duration)
}

func (o *otelInstrumentation) recordEnqueuePrepared(ctx context.Context, duration time.Duration) {
	o.recordEnqueue(ctx, metricOutcomePrepared, duration)
}

func (o *otelInstrumentation) recordEnqueue(ctx context.Context, outcome string, duration time.Duration) {
	if o.enqueueTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, outcome, "", "")
	o.enqueueTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	if duration > 0 {
		o.enqueueDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}
}

func (o *otelInstrumentation) recordClaimBatch(ctx context.Context, count int) {
	if o.claimTotal == nil || count <= 0 {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, "", "")
	o.claimTotal.Add(ctx, int64(count), metric.WithAttributes(attrs...))
	o.claimBatchSize.Record(ctx, int64(count), metric.WithAttributes(attrs...))
}

func (o *otelInstrumentation) recordClaimTiming(ctx context.Context, scheduleLag, messageAge time.Duration) {
	if o.claimTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, "", "")
	if scheduleLag > 0 {
		o.scheduleLag.Record(ctx, scheduleLag.Seconds(), metric.WithAttributes(attrs...))
	}
	if messageAge > 0 {
		o.messageAge.Record(ctx, messageAge.Seconds(), metric.WithAttributes(attrs...))
	}
}

func (o *otelInstrumentation) recordProcessSuccess(ctx context.Context, duration time.Duration) {
	o.recordProcessOutcome(ctx, metricOutcomeSuccess, "", duration)
}

func (o *otelInstrumentation) recordProcessRetried(ctx context.Context, processErr error, duration time.Duration) {
	o.recordProcessOutcome(ctx, metricOutcomeRetried, failureKind(processErr), duration)
}

func (o *otelInstrumentation) recordProcessDeadLetter(ctx context.Context, processErr error, duration time.Duration) {
	o.recordProcessOutcome(ctx, metricOutcomeDeadLetter, failureKind(processErr), duration)
}

func (o *otelInstrumentation) recordProcessOutcome(ctx context.Context, outcome, failure string, duration time.Duration) {
	if o.processTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, outcome, "", failure)
	o.processTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	if duration > 0 {
		o.processDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}
}

func (o *otelInstrumentation) recordRetryScheduled(ctx context.Context, processErr error, retryDelay time.Duration) {
	if o.requeueTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, metricModeRetry, failureKind(processErr))
	o.requeueTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	if retryDelay > 0 {
		o.retryDelay.Record(ctx, retryDelay.Seconds(), metric.WithAttributes(attrs...))
	}
}

func (o *otelInstrumentation) recordDeadLetter(ctx context.Context, processErr error) {
	if o.deadLetterTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, "", failureKind(processErr))
	o.deadLetterTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (o *otelInstrumentation) recordExpiredLeaseRequeue(ctx context.Context, count int) {
	if o.requeueTotal == nil || count <= 0 {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, metricModeExpired, "")
	o.requeueTotal.Add(ctx, int64(count), metric.WithAttributes(attrs...))
}

func (o *otelInstrumentation) recordManualRequeue(ctx context.Context) {
	if o.requeueTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, metricModeManual, "")
	o.requeueTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (o *otelInstrumentation) workQueued() {
	o.workDepth.Add(1)
}

func (o *otelInstrumentation) workStarted() {
	o.workDepth.Add(-1)
	o.activeWorkers.Add(1)
}

func (o *otelInstrumentation) workFinished() {
	o.activeWorkers.Add(-1)
}

func (o *otelInstrumentation) startEnqueueSpan(ctx context.Context, availableAt time.Time, maxAttempts int) (context.Context, oteltrace.Span, map[string]string) {
	ctx, span := o.tracer.Start(ctx, "badgerbox.enqueue", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	span.SetAttributes(
		attribute.String("badgerbox.namespace", o.namespace),
		attribute.String("badgerbox.attempt", "0"),
		attribute.String("badgerbox.max_attempts", strconv.Itoa(maxAttempts)),
	)
	if !availableAt.IsZero() {
		span.SetAttributes(attribute.String("badgerbox.available_at", availableAt.UTC().Format(time.RFC3339Nano)))
	}
	return ctx, span, cloneStringMap(o.injectCarrier(ctx))
}

func (o *otelInstrumentation) startProcessSpan(ctx context.Context, id MessageID, attempt, maxAttempts int, createdAt, availableAt time.Time, carrier map[string]string) (context.Context, oteltrace.Span) {
	if len(carrier) > 0 {
		ctx = o.propagator.Extract(ctx, propagation.MapCarrier(carrier))
	}
	ctx, span := o.tracer.Start(ctx, "badgerbox.process", oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))
	o.setMessageSpanAttributes(span, id, attempt, maxAttempts, createdAt, availableAt)
	return ctx, span
}

func (o *otelInstrumentation) setMessageSpanAttributes(span oteltrace.Span, id MessageID, attempt, maxAttempts int, createdAt, availableAt time.Time) {
	attrs := []attribute.KeyValue{
		attribute.String("badgerbox.namespace", o.namespace),
		attribute.String("badgerbox.attempt", strconv.Itoa(attempt)),
		attribute.String("badgerbox.max_attempts", strconv.Itoa(maxAttempts)),
	}
	if id != 0 {
		attrs = append(attrs, attribute.String("badgerbox.message_id", strconv.FormatUint(uint64(id), 10)))
	}
	if !createdAt.IsZero() {
		attrs = append(attrs, attribute.String("badgerbox.created_at", createdAt.UTC().Format(time.RFC3339Nano)))
	}
	if !availableAt.IsZero() {
		attrs = append(attrs, attribute.String("badgerbox.available_at", availableAt.UTC().Format(time.RFC3339Nano)))
	}
	span.SetAttributes(attrs...)
}

func (o *otelInstrumentation) endSpan(span oteltrace.Span, outcome string) {
	if outcome != "" {
		span.SetAttributes(attribute.String("badgerbox.outcome", outcome))
	}
	span.End()
}

func (o *otelInstrumentation) injectCarrier(ctx context.Context) map[string]string {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := propagation.MapCarrier{}
	o.propagator.Inject(ctx, carrier)
	return map[string]string(carrier)
}

func queueMetricAttributes(namespace, outcome, mode, failure string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 4)
	if namespace != "" {
		attrs = append(attrs, attribute.String("namespace", namespace))
	}
	if outcome != "" {
		attrs = append(attrs, attribute.String("outcome", outcome))
	}
	if mode != "" {
		attrs = append(attrs, attribute.String("mode", mode))
	}
	if failure != "" {
		attrs = append(attrs, attribute.String("failure_kind", failure))
	}
	return attrs
}

func namespaceAttributes(namespace string) []attribute.KeyValue {
	if namespace == "" {
		return nil
	}
	return []attribute.KeyValue{attribute.String("namespace", namespace)}
}
