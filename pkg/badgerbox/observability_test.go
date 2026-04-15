package badgerbox

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	metricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestOTelInstrumentationExportsCoreMetrics(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	obs, err := newOTelInstrumentation(ObservabilityOptions{MeterProvider: provider}, "orders", func(context.Context) (queueSnapshot, error) {
		return queueSnapshot{
			ReadyDepth:          3,
			ProcessingDepth:     1,
			DeadLetterDepth:     2,
			OldestReadyAge:      4 * time.Second,
			OldestProcessingAge: 2 * time.Second,
		}, nil
	})
	if err != nil {
		t.Fatalf("new instrumentation: %v", err)
	}
	defer obs.close()

	obs.workQueued()
	obs.workQueued()
	obs.workQueued()
	obs.workQueued()
	obs.workQueued()
	obs.workStarted()
	obs.workStarted()
	if err := obs.recordSnapshot(context.Background()); err != nil {
		t.Fatalf("record snapshot: %v", err)
	}

	obs.recordEnqueueCommitted(context.Background(), 3*time.Millisecond)
	obs.recordProcessSuccess(context.Background(), 8*time.Millisecond)
	obs.recordRetryScheduled(context.Background(), errors.New("retry"), 5*time.Second)
	obs.recordDeadLetter(context.Background(), Permanent(errors.New("stop")))
	obs.recordConflictRetry(context.Background())
	obs.recordConflictRetry(context.Background())

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	if got := int64SumValueWithAttrs(collected, "badgerbox_enqueue_total", attribute.String("namespace", "orders"), attribute.String("outcome", metricOutcomeCommitted)); got != 1 {
		t.Fatalf("enqueue total = %d, want 1", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_process_attempt_total", attribute.String("namespace", "orders"), attribute.String("outcome", metricOutcomeSuccess)); got != 1 {
		t.Fatalf("process total = %d, want 1", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_dead_letter_total", attribute.String("namespace", "orders"), attribute.String("outcome", metricOutcomeSuccess)); got != 1 {
		t.Fatalf("dead letter total = %d, want 1", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_conflict_retry_total", attribute.String("namespace", "orders")); got != 2 {
		t.Fatalf("conflict retry total = %d, want 2", got)
	}
	if got := int64GaugeValueWithAttrs(collected, "badgerbox_queue_ready", attribute.String("namespace", "orders")); got != 3 {
		t.Fatalf("ready depth = %d, want 3", got)
	}
	if got := int64GaugeValueWithAttrs(collected, "badgerbox_workers_active", attribute.String("namespace", "orders")); got != 2 {
		t.Fatalf("active workers = %d, want 2", got)
	}
	if got := int64GaugeValueWithAttrs(collected, "badgerbox_work_channel_depth", attribute.String("namespace", "orders")); got != 3 {
		t.Fatalf("work channel depth = %d, want 3", got)
	}
	if count := float64HistogramCountWithAttrs(collected, "badgerbox_retry_delay_seconds", attribute.String("namespace", "orders"), attribute.String("mode", metricModeRetry)); count != 1 {
		t.Fatalf("retry delay count = %d, want 1", count)
	}
}

func TestQueueSnapshotCountsLifecycleStates(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "queue-snapshot", Serde[testPayload, testDestination]{})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "processing"},
		Destination: testDestination{Route: "/processing"},
	}); err != nil {
		t.Fatalf("enqueue processing: %v", err)
	}

	claimed, err := store.claimReadyBatch(context.Background(), time.Now().UTC(), 1, time.Minute, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claim processing: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("expected 1 claimed processing message, got %d", len(claimed))
	}

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "ready"},
		Destination: testDestination{Route: "/ready"},
		AvailableAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("enqueue ready: %v", err)
	}

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "dead"},
		Destination: testDestination{Route: "/dead"},
	}); err != nil {
		t.Fatalf("enqueue dead: %v", err)
	}

	deadClaimed, err := store.claimReadyBatch(context.Background(), time.Now().UTC(), 1, time.Minute, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claim dead: %v", err)
	}
	if len(deadClaimed) != 1 {
		t.Fatalf("expected 1 claimed dead-letter message, got %d", len(deadClaimed))
	}

	if _, err := store.failProcessing(context.Background(), deadClaimed[0].Message.ID, deadClaimed[0].LeaseToken, Permanent(errors.New("stop")), time.Second, time.Second); err != nil {
		t.Fatalf("dead-letter message: %v", err)
	}

	snapshot, err := store.queueSnapshot(context.Background())
	if err != nil {
		t.Fatalf("queue snapshot: %v", err)
	}

	if snapshot.ReadyDepth != 1 || snapshot.ProcessingDepth != 1 || snapshot.DeadLetterDepth != 1 {
		t.Fatalf("unexpected snapshot counts: %#v", snapshot)
	}
}

func TestObservabilityLifecycleEmitsMetricsAndTraces(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	recorder := tracetest.NewSpanRecorder()
	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	defer db.Close()

	store, err := New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{
		Namespace: "observed",
		Observability: ObservabilityOptions{
			MeterProvider:  meterProvider,
			TracerProvider: traceProvider,
		},
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	for _, name := range []string{"retry", "dead"} {
		if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
			Payload:     testPayload{Name: name},
			Destination: testDestination{Route: "/" + name},
		}); err != nil {
			t.Fatalf("enqueue %s: %v", name, err)
		}
	}

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		switch msg.Payload.Name {
		case "retry":
			if msg.Attempt == 1 {
				return errors.New("retry once")
			}
			return nil
		case "dead":
			return Permanent(errors.New("move to dlq"))
		default:
			return nil
		}
	}, ProcessorOptions{
		Concurrency:    1,
		ClaimBatchSize: 1,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  50 * time.Millisecond,
		RetryBaseDelay: 5 * time.Millisecond,
		RetryMaxDelay:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		snapshot, snapshotErr := store.queueSnapshot(context.Background())
		if snapshotErr != nil {
			t.Fatalf("queue snapshot: %v", snapshotErr)
		}
		return snapshot.ReadyDepth == 0 && snapshot.ProcessingDepth == 0 && snapshot.DeadLetterDepth == 1
	})

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	if got := int64SumValueWithAttrs(collected, "badgerbox_enqueue_total", attribute.String("namespace", "observed"), attribute.String("outcome", metricOutcomeCommitted)); got != 2 {
		t.Fatalf("enqueue total = %d, want 2", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_process_attempt_total", attribute.String("namespace", "observed"), attribute.String("outcome", metricOutcomeRetried), attribute.String("failure_kind", "error")); got != 1 {
		t.Fatalf("retried process total = %d, want 1", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_process_attempt_total", attribute.String("namespace", "observed"), attribute.String("outcome", metricOutcomeSuccess)); got != 1 {
		t.Fatalf("successful process total = %d, want 1", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_process_attempt_total", attribute.String("namespace", "observed"), attribute.String("outcome", metricOutcomeDeadLetter), attribute.String("failure_kind", "permanent")); got != 1 {
		t.Fatalf("dead-letter process total = %d, want 1", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_requeue_total", attribute.String("namespace", "observed"), attribute.String("outcome", metricOutcomeSuccess), attribute.String("mode", metricModeRetry), attribute.String("failure_kind", "error")); got != 1 {
		t.Fatalf("retry requeue total = %d, want 1", got)
	}
	if got := int64SumValueWithAttrs(collected, "badgerbox_dead_letter_total", attribute.String("namespace", "observed"), attribute.String("outcome", metricOutcomeSuccess), attribute.String("failure_kind", "permanent")); got != 1 {
		t.Fatalf("dead-letter counter total = %d, want 1", got)
	}

	ended := recorder.Ended()
	if len(ended) < 5 {
		t.Fatalf("expected enqueue and process spans, got %d", len(ended))
	}

	enqueueIDs := make(map[string]struct{})
	var foundRetried, foundSuccess, foundDeadLetter, foundLinkedProcess bool
	for _, span := range ended {
		if span.Name() == "badgerbox.enqueue" {
			enqueueIDs[span.SpanContext().SpanID().String()] = struct{}{}
			continue
		}
		if span.Name() != "badgerbox.process" {
			continue
		}

		switch {
		case spanHasAttribute(span, attribute.String("badgerbox.outcome", metricOutcomeRetried)):
			foundRetried = true
		case spanHasAttribute(span, attribute.String("badgerbox.outcome", metricOutcomeSuccess)):
			foundSuccess = true
		case spanHasAttribute(span, attribute.String("badgerbox.outcome", metricOutcomeDeadLetter)):
			foundDeadLetter = true
		}
		if _, ok := enqueueIDs[span.Parent().SpanID().String()]; ok {
			foundLinkedProcess = true
		}
	}

	if !foundRetried || !foundSuccess || !foundDeadLetter {
		t.Fatalf("missing expected process span outcomes: %#v", ended)
	}
	if !foundLinkedProcess {
		t.Fatalf("expected at least one process span linked to an enqueue span")
	}
}

func TestTraceCarrierPersistsAcrossReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	recorder := tracetest.NewSpanRecorder()
	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	store, err := New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{
		Namespace: "trace-persist",
		Observability: ObservabilityOptions{
			TracerProvider: traceProvider,
		},
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "persist"},
		Destination: testDestination{Route: "/persist"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var storedCarrier map[string]string
	err = db.View(func(txn *badger.Txn) error {
		record, err := store.loadRecord(txn, id)
		if err != nil {
			return err
		}
		storedCarrier = cloneStringMap(record.TraceCarrier)
		return nil
	})
	if err != nil {
		t.Fatalf("view record: %v", err)
	}
	if storedCarrier["traceparent"] == "" {
		t.Fatalf("expected traceparent in stored carrier, got %#v", storedCarrier)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db, err = badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("reopen badger: %v", err)
	}
	defer db.Close()

	store, err = New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{
		Namespace: "trace-persist",
		Observability: ObservabilityOptions{
			TracerProvider: traceProvider,
		},
	})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		return nil
	}, ProcessorOptions{PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		for _, span := range recorder.Ended() {
			if span.Name() == "badgerbox.process" {
				return true
			}
		}
		return false
	})

	ended := recorder.Ended()
	var enqueueSpan, processSpan sdktrace.ReadOnlySpan
	for _, span := range ended {
		switch span.Name() {
		case "badgerbox.enqueue":
			enqueueSpan = span
		case "badgerbox.process":
			processSpan = span
		}
	}
	if enqueueSpan == nil || processSpan == nil {
		t.Fatalf("missing enqueue/process spans: %#v", ended)
	}
	if processSpan.Parent().SpanID() != enqueueSpan.SpanContext().SpanID() {
		t.Fatalf("process span parent = %s, want %s", processSpan.Parent().SpanID(), enqueueSpan.SpanContext().SpanID())
	}
}

func TestStoreQueueSnapshotUsesRuntimeNow(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_700_000_000, 0).UTC()
	runtime := newFakeRuntime(base.Add(-10 * time.Minute))

	_, store, cleanup := openTestStoreWithOptions(t, "runtime-snapshot", Serde[testPayload, testDestination]{}, Options{Runtime: runtime})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "processing"},
		Destination: testDestination{Route: "/processing"},
	}); err != nil {
		t.Fatalf("enqueue processing: %v", err)
	}

	runtime.SetNow(base.Add(-9 * time.Minute))
	processingClaimed, err := store.claimReadyBatch(context.Background(), runtime.Now(), 1, 20*time.Minute, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claim processing: %v", err)
	}
	if len(processingClaimed) != 1 {
		t.Fatalf("processing claimed = %d, want 1", len(processingClaimed))
	}

	runtime.SetNow(base.Add(-7 * time.Minute))
	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "ready"},
		Destination: testDestination{Route: "/ready"},
		AvailableAt: base.Add(time.Hour),
	}); err != nil {
		t.Fatalf("enqueue ready: %v", err)
	}

	runtime.SetNow(base.Add(-5 * time.Minute))
	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "dead"},
		Destination: testDestination{Route: "/dead"},
	}); err != nil {
		t.Fatalf("enqueue dead: %v", err)
	}

	deadClaimed, err := store.claimReadyBatch(context.Background(), runtime.Now(), 1, time.Minute, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claim dead: %v", err)
	}
	if len(deadClaimed) != 1 {
		t.Fatalf("dead claimed = %d, want 1", len(deadClaimed))
	}
	if _, err := store.failProcessing(context.Background(), deadClaimed[0].Message.ID, deadClaimed[0].LeaseToken, Permanent(errors.New("stop")), time.Second, time.Second); err != nil {
		t.Fatalf("dead-letter message: %v", err)
	}

	runtime.SetNow(base)
	snapshot, err := store.queueSnapshot(context.Background())
	if err != nil {
		t.Fatalf("queueSnapshot: %v", err)
	}
	if snapshot.ReadyDepth != 1 || snapshot.ProcessingDepth != 1 || snapshot.DeadLetterDepth != 1 {
		t.Fatalf("unexpected snapshot counts: %#v", snapshot)
	}
	if snapshot.OldestReadyAge != 7*time.Minute {
		t.Fatalf("oldest ready age = %v, want 7m", snapshot.OldestReadyAge)
	}
	if snapshot.OldestProcessingAge != 10*time.Minute {
		t.Fatalf("oldest processing age = %v, want 10m", snapshot.OldestProcessingAge)
	}
}

func TestNewPureConstructorDoesNotStartObservability(t *testing.T) {
	t.Parallel()

	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	_, store, cleanup := openTestStoreWithOptions(t, "constructor-pure", Serde[testPayload, testDestination]{}, Options{
		Runtime: runtime,
		Observability: ObservabilityOptions{
			MeterProvider: provider,
		},
	})
	defer cleanup()

	if runtime.TickerCount() != 0 {
		t.Fatalf("constructor created %d tickers, want 0", runtime.TickerCount())
	}
	if store.obs.done != nil || store.obs.cancel != nil {
		t.Fatalf("constructor started observability: done=%v cancel=%v", store.obs.done, store.obs.cancel)
	}
}

func TestStartObservabilityStartsOnceAndStopsOnClose(t *testing.T) {
	t.Parallel()

	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	_, store, cleanup := openTestStoreWithOptions(t, "start-observability", Serde[testPayload, testDestination]{}, Options{
		Runtime: runtime,
		Observability: ObservabilityOptions{
			MeterProvider: provider,
			PollInterval:  time.Second,
		},
	})
	defer cleanup()

	var calls atomic.Int32
	store.obs.queueSnapshot = func(context.Context) (queueSnapshot, error) {
		calls.Add(1)
		return queueSnapshot{}, nil
	}

	if err := store.StartObservability(context.Background()); err != nil {
		t.Fatalf("StartObservability: %v", err)
	}
	if err := store.StartObservability(context.Background()); err != nil {
		t.Fatalf("StartObservability second call: %v", err)
	}
	waitFor(t, func() bool {
		return runtime.TickerCount() == 1
	})

	runtime.TickAll()
	waitFor(t, func() bool {
		return calls.Load() == 1
	})

	before := calls.Load()
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	runtime.TickAll()
	time.Sleep(10 * time.Millisecond)
	if calls.Load() != before {
		t.Fatalf("snapshot calls after close = %d, want %d", calls.Load(), before)
	}
}

func TestRecordObservabilitySnapshotReturnsErrorSync(t *testing.T) {
	t.Parallel()

	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	expected := errors.New("snapshot failed")

	_, store, cleanup := openTestStoreWithOptions(t, "snapshot-error", Serde[testPayload, testDestination]{}, Options{
		Runtime: runtime,
		Observability: ObservabilityOptions{
			MeterProvider: provider,
		},
	})
	defer cleanup()

	store.obs.queueSnapshot = func(context.Context) (queueSnapshot, error) {
		return queueSnapshot{}, expected
	}

	if err := store.RecordObservabilitySnapshot(context.Background()); !errors.Is(err, expected) {
		t.Fatalf("RecordObservabilitySnapshot err = %v, want %v", err, expected)
	}
}

func TestFailureKindAndPositiveDuration(t *testing.T) {
	t.Parallel()

	if got := positiveDuration(-time.Second); got != 0 {
		t.Fatalf("positiveDuration(-1s) = %v, want 0", got)
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "permanent", err: Permanent(errors.New("stop")), want: "permanent"},
		{name: "canceled", err: context.Canceled, want: "context"},
		{name: "deadline", err: context.DeadlineExceeded, want: "context"},
		{name: "generic", err: errors.New("boom"), want: "error"},
	}

	for _, tt := range tests {
		if got := failureKind(tt.err); got != tt.want {
			t.Fatalf("%s failureKind = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestRecordSnapshotReturnsQueueSnapshotError(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	expected := errors.New("snapshot failed")

	obs, err := newOTelInstrumentation(ObservabilityOptions{MeterProvider: provider}, "orders", func(context.Context) (queueSnapshot, error) {
		return queueSnapshot{}, expected
	})
	if err != nil {
		t.Fatalf("newOTelInstrumentation: %v", err)
	}
	defer obs.close()

	if err := obs.recordSnapshot(context.Background()); !errors.Is(err, expected) {
		t.Fatalf("recordSnapshot err = %v, want %v", err, expected)
	}
}

func int64SumValueWithAttrs(metrics metricdata.ResourceMetrics, name string, want ...attribute.KeyValue) int64 {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			if data, ok := metric.Data.(metricdata.Sum[int64]); ok {
				for _, point := range data.DataPoints {
					if attributeSetHasAll(point.Attributes, want...) {
						return point.Value
					}
				}
			}
		}
	}
	return 0
}

func float64SumValueWithAttrs(metrics metricdata.ResourceMetrics, name string, want ...attribute.KeyValue) float64 {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			if data, ok := metric.Data.(metricdata.Sum[float64]); ok {
				for _, point := range data.DataPoints {
					if attributeSetHasAll(point.Attributes, want...) {
						return point.Value
					}
				}
			}
		}
	}
	return 0
}

func int64GaugeValueWithAttrs(metrics metricdata.ResourceMetrics, name string, want ...attribute.KeyValue) int64 {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			if data, ok := metric.Data.(metricdata.Gauge[int64]); ok {
				for _, point := range data.DataPoints {
					if attributeSetHasAll(point.Attributes, want...) {
						return point.Value
					}
				}
			}
		}
	}
	return 0
}

func float64GaugeValueWithAttrs(metrics metricdata.ResourceMetrics, name string, want ...attribute.KeyValue) float64 {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			if data, ok := metric.Data.(metricdata.Gauge[float64]); ok {
				for _, point := range data.DataPoints {
					if attributeSetHasAll(point.Attributes, want...) {
						return point.Value
					}
				}
			}
		}
	}
	return 0
}

func float64HistogramCountWithAttrs(metrics metricdata.ResourceMetrics, name string, want ...attribute.KeyValue) uint64 {
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != name {
				continue
			}
			if data, ok := metric.Data.(metricdata.Histogram[float64]); ok {
				for _, point := range data.DataPoints {
					if attributeSetHasAll(point.Attributes, want...) {
						return point.Count
					}
				}
			}
		}
	}
	return 0
}

func attributeSetHasAll(set attribute.Set, want ...attribute.KeyValue) bool {
	for _, item := range want {
		found := false
		for _, attr := range set.ToSlice() {
			if attr.Key == item.Key && attr.Value == item.Value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func spanHasAttribute(span sdktrace.ReadOnlySpan, want attribute.KeyValue) bool {
	for _, attr := range span.Attributes() {
		if attr.Key == want.Key && attr.Value == want.Value {
			return true
		}
	}
	return false
}
