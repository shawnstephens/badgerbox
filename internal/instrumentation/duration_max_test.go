package instrumentation

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func maxPoints(t *testing.T, reader *sdkmetric.ManualReader) map[string]float64 {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	result := map[string]float64{}
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			gauge, ok := m.Data.(metricdata.Gauge[float64])
			if !ok {
				continue
			}
			for _, p := range gauge.DataPoints {
				ns, _ := p.Attributes.Value(attribute.Key("namespace"))
				outcome, _ := p.Attributes.Value(attribute.Key("outcome"))
				failure, _ := p.Attributes.Value(attribute.Key("failure_kind"))
				result[fmt.Sprintf("%s/%s/%s/%s", m.Name, ns.AsString(), outcome.AsString(), failure.AsString())] = p.Value
			}
		}
	}
	return result
}

func TestDurationMaxWindowsAcrossReaders(t *testing.T) {
	for _, reverse := range []bool{false, true} {
		t.Run(fmt.Sprint("reverse=", reverse), func(t *testing.T) {
			readers := []*sdkmetric.ManualReader{sdkmetric.NewManualReader(), sdkmetric.NewManualReader()}
			provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(readers[0]), sdkmetric.WithReader(readers[1]))
			defer func() {
				if err := provider.Shutdown(t.Context()); err != nil {
					t.Error(err)
				}
			}()
			q, err := NewQueue(ObservabilityOptions{MeterProvider: provider, DurationMaxWindow: time.Second}, "orders", nil, func(error) string { return "error" })
			if err != nil {
				t.Fatal(err)
			}
			defer q.Close()
			var offset atomic.Int64
			now := func() time.Time { return time.Unix(100, 0).Add(time.Duration(offset.Load())) }
			q.enqueueDurationMaxTracker = newDurationMaxTracker(time.Second, now)
			q.processDurationMaxTracker = newDurationMaxTracker(time.Second, now)
			if reverse {
				readers[0], readers[1] = readers[1], readers[0]
			}
			q.RecordEnqueueCommitted(t.Context(), 7*time.Millisecond)
			q.RecordEnqueuePrepared(t.Context(), 5*time.Millisecond)
			q.RecordProcessOutcome(t.Context(), "success", "", 8*time.Millisecond)
			q.RecordProcessOutcome(t.Context(), "retried", "error", 11*time.Millisecond)
			q.RecordProcessOutcome(t.Context(), "dead_letter", "permanent", 6*time.Millisecond)
			expected := map[string]float64{
				"badgerbox_enqueue_duration_seconds_max/orders/committed/":            .007,
				"badgerbox_enqueue_duration_seconds_max/orders/prepared/":             .005,
				"badgerbox_process_duration_seconds_max/orders/success/":              .008,
				"badgerbox_process_duration_seconds_max/orders/retried/error":         .011,
				"badgerbox_process_duration_seconds_max/orders/dead_letter/permanent": .006,
			}
			check := func() {
				t.Helper()
				for _, reader := range readers {
					if got := maxPoints(t, reader); !reflect.DeepEqual(got, expected) {
						t.Fatalf("got=%v want=%v", got, expected)
					}
				}
			}
			check()
			check() // Collecting again must not consume the snapshot.
			offset.Store(int64(time.Second))
			q.RecordEnqueueCommitted(t.Context(), 4*time.Millisecond)
			check() // Previous window's larger maximum remains visible.
			offset.Store(int64(2 * time.Second))
			expected = map[string]float64{"badgerbox_enqueue_duration_seconds_max/orders/committed/": .004}
			check()
			offset.Store(int64(4 * time.Second))
			expected = map[string]float64{}
			check()
		})
	}
}

func TestDurationMaxConcurrentCollection(t *testing.T) {
	a, b := sdkmetric.NewManualReader(), sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(a), sdkmetric.WithReader(b))
	defer provider.Shutdown(t.Context())
	q, err := NewQueue(ObservabilityOptions{MeterProvider: provider}, "orders", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer q.Close()
	var wg sync.WaitGroup
	for _, reader := range []*sdkmetric.ManualReader{a, b} {
		wg.Go(func() {
			for range 100 {
				maxPoints(t, reader)
			}
		})
	}
	wg.Go(func() {
		for i := 1; i <= 100; i++ {
			q.RecordEnqueueCommitted(context.Background(), time.Duration(i)*time.Millisecond)
		}
	})
	wg.Wait()
	for _, reader := range []*sdkmetric.ManualReader{a, b} {
		if got := maxPoints(t, reader)["badgerbox_enqueue_duration_seconds_max/orders/committed/"]; got != .1 {
			t.Fatal(got)
		}
	}
}

func TestNegativeDurationWindowRejected(t *testing.T) {
	if _, err := NewQueue(ObservabilityOptions{DurationMaxWindow: -time.Second}, "orders", nil, nil); err == nil {
		t.Fatal("negative window accepted")
	}
}
