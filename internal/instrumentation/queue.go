package instrumentation

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"github.com/shawnstephens/badgerbox/internal/metricconfig"
	"github.com/shawnstephens/badgerbox/pkg/telemetry"
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

const defaultObservabilityPollInterval = 5 * time.Second

type ObservabilityOptions = telemetry.Options

type QueueSnapshot = telemetry.QueueSnapshot

type queueSnapshot = QueueSnapshot

// QueueSnapshot reads lifecycle and creation index keys without decoding payloads.
// Depth collection is O(N); use Audit for row/index reconciliation.

func normalizeObservabilityOptions(opts ObservabilityOptions) ObservabilityOptions {
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultObservabilityPollInterval
	}
	return opts
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

func positiveDuration(value time.Duration) time.Duration {
	if value < 0 {
		return 0
	}
	return value
}

type Queue struct {
	extraCounters     map[string]metric.Int64Counter
	extraDurations    map[string]metric.Float64Histogram
	claimTooBig       metric.Int64Counter
	claimRetrySize    metric.Int64Histogram
	classify          func(error) string
	closeOnce         sync.Once
	closed            bool
	namespace         string
	Snapshot          func(context.Context) (queueSnapshot, error)
	AdmissionSnapshot func(context.Context) (AdmissionSnapshot, error)
	admission         admissionGauges

	tracer     oteltrace.Tracer
	propagator propagation.TextMapPropagator

	closeMu            sync.Mutex
	closeErr           error
	cancel             context.CancelFunc
	done               chan struct{}
	metricRegistration metric.Registration

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

	enqueueDurationMax metric.Float64ObservableGauge
	processDurationMax metric.Float64ObservableGauge

	readyDepth          metric.Int64Gauge
	processingDepth     metric.Int64Gauge
	deadLetterDepth     metric.Int64Gauge
	activeWorkersGauge  metric.Int64Gauge
	workChannelDepth    metric.Int64Gauge
	oldestReadyAge      metric.Float64Gauge
	oldestProcessingAge metric.Float64Gauge

	enqueueDurationMaxTracker durationMaxTracker
	processDurationMaxTracker durationMaxTracker
}

func NewQueue(opts ObservabilityOptions, namespace string, queueSnapshot func(context.Context) (queueSnapshot, error), classify func(error) string) (*Queue, error) {
	if opts.DurationMaxWindow < 0 {
		return nil, fmt.Errorf("badgerbox telemetry: duration maximum window must be nonnegative")
	}
	opts = normalizeObservabilityOptions(opts)
	if opts.DurationMaxWindow == 0 {
		opts.DurationMaxWindow = time.Minute
	}

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

	inst := &Queue{
		enqueueDurationMaxTracker: newDurationMaxTracker(opts.DurationMaxWindow, time.Now),
		processDurationMaxTracker: newDurationMaxTracker(opts.DurationMaxWindow, time.Now),
		namespace:                 namespace,
		classify:                  classify,
		Snapshot:                  queueSnapshot,
		tracer:                    tracerProvider.Tracer(tracerName),
		propagator:                propagator,
	}

	if opts.MeterProvider == nil {
		return inst, nil
	}

	meterName := opts.MeterName
	if meterName == "" {
		meterName = defaultInstrumentationName
	}
	meter := opts.MeterProvider.Meter(meterName)
	if err := inst.InitExtras(meter); err != nil {
		return nil, err
	}
	var extraErr error
	inst.admission, extraErr = newAdmissionGauges(meter)
	if extraErr != nil {
		return nil, extraErr
	}
	inst.claimTooBig, extraErr = meter.Int64Counter("badgerbox_claim_transaction_too_big_total")
	if extraErr != nil {
		return nil, extraErr
	}
	inst.claimRetrySize, extraErr = meter.Int64Histogram("badgerbox_claim_retry_size")
	if extraErr != nil {
		return nil, extraErr
	}

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
	if inst.enqueueDuration, err = metricconfig.DurationHistogram(meter, "badgerbox_enqueue_duration_seconds"); err != nil {
		return nil, err
	}
	if inst.processDuration, err = metricconfig.DurationHistogram(meter, "badgerbox_process_duration_seconds"); err != nil {
		return nil, err
	}
	if inst.scheduleLag, err = metricconfig.DurationHistogram(meter, "badgerbox_schedule_lag_seconds"); err != nil {
		return nil, err
	}
	if inst.messageAge, err = metricconfig.DurationHistogram(meter, "badgerbox_message_age_seconds"); err != nil {
		return nil, err
	}
	if inst.retryDelay, err = metricconfig.DurationHistogram(meter, "badgerbox_retry_delay_seconds"); err != nil {
		return nil, err
	}
	if inst.claimBatchSize, err = meter.Int64Histogram("badgerbox_claim_batch_size"); err != nil {
		return nil, err
	}
	if inst.enqueueDurationMax, err = meter.Float64ObservableGauge("badgerbox_enqueue_duration_seconds_max", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if inst.processDurationMax, err = meter.Float64ObservableGauge("badgerbox_process_duration_seconds_max", metric.WithUnit("s")); err != nil {
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
	if inst.oldestReadyAge, err = meter.Float64Gauge("badgerbox_queue_oldest_ready_age_seconds", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if inst.oldestProcessingAge, err = meter.Float64Gauge("badgerbox_queue_oldest_processing_age_seconds", metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if inst.metricRegistration, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		for _, point := range inst.enqueueDurationMaxTracker.snapshot() {
			observer.ObserveFloat64(inst.enqueueDurationMax, point.value, metric.WithAttributes(point.attrs...))
		}
		for _, point := range inst.processDurationMaxTracker.snapshot() {
			observer.ObserveFloat64(inst.processDurationMax, point.value, metric.WithAttributes(point.attrs...))
		}
		return nil
	}, inst.enqueueDurationMax, inst.processDurationMax); err != nil {
		return nil, err
	}

	return inst, nil
}

func (o *Queue) Close() error {
	o.closeOnce.Do(func() {
		o.closeMu.Lock()
		o.closed = true
		cancel, done, registration := o.cancel, o.done, o.metricRegistration
		o.closeMu.Unlock()
		if cancel != nil {
			cancel()
			<-done
		}
		if registration != nil {
			err := registration.Unregister()
			o.closeMu.Lock()
			o.closeErr = errors.Join(o.closeErr, err)
			o.closeMu.Unlock()
		}
	})
	o.closeMu.Lock()
	defer o.closeMu.Unlock()
	return o.closeErr
}

func (o *Queue) Start(ctx context.Context, newTicker func(time.Duration) (<-chan time.Time, func()), interval time.Duration) error {
	if ctx == nil {
		return fmt.Errorf("badgerbox telemetry: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.closeMu.Lock()
	defer o.closeMu.Unlock()
	if o.closed {
		return fmt.Errorf("badgerbox telemetry: closed")
	}
	if o.done != nil || o.readyDepth == nil {
		return nil
	}
	if interval <= 0 {
		interval = defaultObservabilityPollInterval
	}
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	o.cancel = cancel
	o.done = done
	go o.pollSnapshots(runCtx, newTicker, interval, done)
	return nil
}

func (o *Queue) pollSnapshots(ctx context.Context, newTicker func(time.Duration) (<-chan time.Time, func()), interval time.Duration, done chan struct{}) {
	defer close(done)
	ticks, stop := newTicker(interval)
	defer stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticks:
			if ctx.Err() != nil {
				return
			}
			if err := o.RecordSnapshot(ctx); err != nil && ctx.Err() == nil {
				o.closeMu.Lock()
				o.closeErr = err
				o.closeMu.Unlock()
			}
		}
	}
}

func (o *Queue) RecordSnapshot(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if o.readyDepth == nil {
		return nil
	}

	started := time.Now()
	defer func() { o.Observe(ctx, "snapshot_duration_seconds", time.Since(started).Seconds()) }()
	recordErr := o.recordAdmissionSnapshot(ctx)
	attrs := namespaceAttributes(o.namespace)

	if o.Snapshot != nil {
		snapshot, err := o.Snapshot(ctx)
		if err != nil {
			o.Count(ctx, "snapshot_error_total", 1)
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

func (o *Queue) RecordConflictRetry(ctx context.Context) {
	if o.conflictRetryTotal == nil {
		return
	}
	o.conflictRetryTotal.Add(ctx, 1, metric.WithAttributes(namespaceAttributes(o.namespace)...))
}

func (o *Queue) RecordEnqueueCommitted(ctx context.Context, duration time.Duration) {
	o.RecordEnqueue(ctx, metricOutcomeCommitted, duration)
}

func (o *Queue) RecordEnqueuePrepared(ctx context.Context, duration time.Duration) {
	o.RecordEnqueue(ctx, metricOutcomePrepared, duration)
}

func (o *Queue) RecordEnqueue(ctx context.Context, outcome string, duration time.Duration) {
	if o.enqueueTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, outcome, "", "")
	o.enqueueTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	if duration > 0 {
		o.enqueueDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
		o.enqueueDurationMaxTracker.record(queueMetricKey{
			namespace: o.namespace,
			outcome:   outcome,
		}, attrs, duration)
	}
}

func (o *Queue) RecordClaimBatch(ctx context.Context, count int) {
	if o.claimTotal == nil || count <= 0 {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, "", "")
	o.claimTotal.Add(ctx, int64(count), metric.WithAttributes(attrs...))
	o.claimBatchSize.Record(ctx, int64(count), metric.WithAttributes(attrs...))
}

func (o *Queue) RecordClaimTiming(ctx context.Context, scheduleLag, messageAge time.Duration) {
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

func (o *Queue) RecordProcessSuccess(ctx context.Context, duration time.Duration) {
	o.RecordProcessOutcome(ctx, metricOutcomeSuccess, "", duration)
}

func (o *Queue) RecordProcessRetried(ctx context.Context, processErr error, duration time.Duration) {
	o.RecordProcessOutcome(ctx, metricOutcomeRetried, o.classify(processErr), duration)
}

func (o *Queue) RecordProcessDeadLetter(ctx context.Context, processErr error, duration time.Duration) {
	o.RecordProcessOutcome(ctx, metricOutcomeDeadLetter, o.classify(processErr), duration)
}

func (o *Queue) RecordProcessOutcome(ctx context.Context, outcome, failure string, duration time.Duration) {
	if o.processTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, outcome, "", failure)
	o.processTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	if duration > 0 {
		o.processDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
		o.processDurationMaxTracker.record(queueMetricKey{
			namespace: o.namespace,
			outcome:   outcome,
			failure:   failure,
		}, attrs, duration)
	}
}

func (o *Queue) RecordRetryScheduled(ctx context.Context, processErr error, retryDelay time.Duration) {
	if o.requeueTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, metricModeRetry, o.classify(processErr))
	o.requeueTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	if retryDelay > 0 {
		o.retryDelay.Record(ctx, retryDelay.Seconds(), metric.WithAttributes(attrs...))
	}
}

func (o *Queue) RecordDeadLetter(ctx context.Context, processErr error) {
	if o.deadLetterTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, "", o.classify(processErr))
	o.deadLetterTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (o *Queue) RecordExpiredLeaseRequeue(ctx context.Context, count int) {
	if o.requeueTotal == nil || count <= 0 {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, metricModeExpired, "")
	o.requeueTotal.Add(ctx, int64(count), metric.WithAttributes(attrs...))
}

func (o *Queue) RecordManualRequeue(ctx context.Context) {
	if o.requeueTotal == nil {
		return
	}
	attrs := queueMetricAttributes(o.namespace, metricOutcomeSuccess, metricModeManual, "")
	o.requeueTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (o *Queue) WorkQueued() {
	o.workDepth.Add(1)
}

func (o *Queue) WorkStarted() {
	o.workDepth.Add(-1)
	o.activeWorkers.Add(1)
}

func (o *Queue) WorkFinished() {
	o.activeWorkers.Add(-1)
}

func (o *Queue) StartEnqueueSpan(ctx context.Context, availableAt time.Time, maxAttempts int) (context.Context, oteltrace.Span, map[string]string) {
	ctx, span := o.tracer.Start(ctx, "badgerbox.enqueue", oteltrace.WithSpanKind(oteltrace.SpanKindProducer))
	span.SetAttributes(
		attribute.String("badgerbox.namespace", o.namespace),
		attribute.String("badgerbox.attempt", "0"),
		attribute.String("badgerbox.max_attempts", strconv.Itoa(maxAttempts)),
	)
	if !availableAt.IsZero() {
		span.SetAttributes(attribute.String("badgerbox.available_at", availableAt.UTC().Format(time.RFC3339Nano)))
	}
	return ctx, span, cloneStringMap(o.InjectCarrier(ctx))
}

func (o *Queue) StartProcessSpan(ctx context.Context, id fmt.Stringer, attempt, maxAttempts int, createdAt, availableAt time.Time, carrier map[string]string, options ...oteltrace.SpanStartOption) (context.Context, oteltrace.Span) {
	if len(carrier) > 0 {
		ctx = o.propagator.Extract(ctx, propagation.MapCarrier(carrier))
	}
	ctx, span := o.tracer.Start(ctx, "badgerbox.process", append(options, oteltrace.WithSpanKind(oteltrace.SpanKindConsumer))...)
	o.SetMessageSpanAttributes(span, id, attempt, maxAttempts, createdAt, availableAt)
	return ctx, span
}

func (o *Queue) SetMessageSpanAttributes(span oteltrace.Span, id fmt.Stringer, attempt, maxAttempts int, createdAt, availableAt time.Time) {
	attrs := []attribute.KeyValue{
		attribute.String("badgerbox.namespace", o.namespace),
		attribute.String("badgerbox.attempt", strconv.Itoa(attempt)),
		attribute.String("badgerbox.max_attempts", strconv.Itoa(maxAttempts)),
	}
	if id != nil {
		attrs = append(attrs, attribute.String("badgerbox.message_id", id.String()))
	}
	if !createdAt.IsZero() {
		attrs = append(attrs, attribute.String("badgerbox.created_at", createdAt.UTC().Format(time.RFC3339Nano)))
	}
	if !availableAt.IsZero() {
		attrs = append(attrs, attribute.String("badgerbox.available_at", availableAt.UTC().Format(time.RFC3339Nano)))
	}
	span.SetAttributes(attrs...)
}

func (o *Queue) EndSpan(span oteltrace.Span, outcome string, options ...oteltrace.SpanEndOption) {
	if outcome != "" {
		span.SetAttributes(attribute.String("badgerbox.outcome", outcome))
	}
	span.End(options...)
}

func (o *Queue) InjectCarrier(ctx context.Context) map[string]string {
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

func (o *Queue) RecordClaimTransactionTooBig(ctx context.Context, size int) {
	if o.claimTooBig != nil {
		o.claimTooBig.Add(ctx, 1, metric.WithAttributes(namespaceAttributes(o.namespace)...))
		if size > 0 {
			o.claimRetrySize.Record(ctx, int64(size), metric.WithAttributes(namespaceAttributes(o.namespace)...))
		}
	}
}
