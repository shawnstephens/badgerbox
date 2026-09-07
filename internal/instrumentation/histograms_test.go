package instrumentation

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/maintenance"
	"github.com/shawnstephens/badgerbox/pkg/telemetry"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestDurationHistogramsResolveLatencyWithoutSDKViews(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(t.Context())
	q, err := NewQueue(ObservabilityOptions{MeterProvider: provider}, "orders", nil, func(error) string { return "error" })
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	gc, err := telemetry.NewBadgerMaintenanceMetrics(telemetry.BadgerMaintenanceMetricsOptions{MeterProvider: provider})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := telemetry.NewDeliveryObserver(telemetry.Options{MeterProvider: provider}, "kafka")
	if err != nil {
		t.Fatal(err)
	}
	for _, duration := range []time.Duration{2 * time.Millisecond, 200 * time.Millisecond, 2 * time.Second} {
		q.RecordEnqueueCommitted(t.Context(), duration)
		q.RecordProcessSuccess(t.Context(), duration)
		q.RecordClaimTiming(t.Context(), duration, duration)
		q.RecordRetryScheduled(t.Context(), errors.New("retry"), duration)
		q.RecordProcessBatch(t.Context(), 64, duration)
		q.RecordKafkaPromise(t.Context(), duration, nil)
		q.Observe(t.Context(), "snapshot_duration_seconds", duration.Seconds())
		gc.ObserveMaintenance(t.Context(), maintenance.Result{Operation: maintenance.OperationValueLogGC, Duration: duration})
		delivery.ObserveFlush(t.Context(), duration, nil)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool)
	for _, scope := range data.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if instrument.Name == "badgerbox_process_batch_size" {
				if instrument.Unit == "s" {
					t.Fatal("batch size was assigned a duration unit")
				}
				continue
			}
			if strings.HasSuffix(instrument.Name, "_seconds_max") && instrument.Unit != "s" {
				t.Errorf("%s unit=%q, want seconds", instrument.Name, instrument.Unit)
			}
			histogram, ok := instrument.Data.(metricdata.Histogram[float64])
			if !ok || !strings.HasSuffix(instrument.Name, "_seconds") {
				continue
			}
			seen[instrument.Name] = true
			if instrument.Unit != "s" {
				t.Errorf("%s unit=%q, want seconds", instrument.Name, instrument.Unit)
			}
			if len(histogram.DataPoints) != 1 {
				t.Fatalf("%s points=%d, want 1", instrument.Name, len(histogram.DataPoints))
			}
			point := histogram.DataPoints[0]
			if point.Count != 3 {
				t.Errorf("%s count=%d, want 3", instrument.Name, point.Count)
			}
			// An SDK with its default 0,5,10,... boundaries collapses these into
			// one bucket. Useful second-scale defaults preserve their separation.
			for _, seconds := range []float64{.002, .2, 2} {
				bucket, _ := slices.BinarySearch(point.Bounds, seconds)
				if bucket == len(point.Bounds) || point.BucketCounts[bucket] != 1 {
					t.Errorf("%s cannot resolve %gs: bounds=%v counts=%v", instrument.Name, seconds, point.Bounds, point.BucketCounts)
				}
			}
		}
	}
	for _, name := range []string{
		"enqueue_duration_seconds", "process_duration_seconds", "schedule_lag_seconds",
		"message_age_seconds", "retry_delay_seconds", "process_batch_duration_seconds",
		"kafka_promise_duration_seconds", "snapshot_duration_seconds",
		"badger_maintenance_duration_seconds", "delivery_flush_duration_seconds",
	} {
		if !seen["badgerbox_"+name] {
			t.Errorf("missing histogram %s", name)
		}
	}
}

func TestApplicationHistogramViewsOverrideDefaults(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	boundaries := []float64{.05, .5}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithView(sdkmetric.NewView(
			sdkmetric.Instrument{Name: "badgerbox_enqueue_duration_seconds"},
			sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{Boundaries: boundaries}},
		)),
	)
	defer provider.Shutdown(t.Context())
	q, err := NewQueue(ObservabilityOptions{MeterProvider: provider}, "orders", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	q.RecordEnqueueCommitted(t.Context(), 2*time.Millisecond)
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	for _, scope := range data.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if instrument.Name == "badgerbox_enqueue_duration_seconds" {
				point := instrument.Data.(metricdata.Histogram[float64]).DataPoints[0]
				if !slices.Equal(point.Bounds, boundaries) || !slices.Equal(point.BucketCounts, []uint64{1, 0, 0}) || instrument.Unit != "s" {
					t.Fatalf("application view not honored: %+v", instrument)
				}
				return
			}
		}
	}
	t.Fatal("missing enqueue histogram")
}
