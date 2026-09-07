package instrumentation

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestAdmissionGaugesPreserveUnsignedRangeAndLimitedCardinality(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(t.Context())
	queue, err := NewQueue(ObservabilityOptions{MeterProvider: provider}, "orders", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	value := AdmissionSnapshot{RetainedMessages: math.MaxUint64, RetainedBytes: math.MaxUint64 - 1, MaxRetainedMessages: 0, MaxRetainedBytes: math.MaxUint64}
	queue.AdmissionSnapshot = func(context.Context) (AdmissionSnapshot, error) { return value, nil }
	for range 2 {
		if err := queue.RecordSnapshot(t.Context()); err != nil {
			t.Fatal(err)
		}
		var collected metricdata.ResourceMetrics
		if err := reader.Collect(t.Context(), &collected); err != nil {
			t.Fatal(err)
		}
		wants := map[string]float64{
			"badgerbox_retained_messages":       float64(value.RetainedMessages),
			"badgerbox_retained_bytes":          float64(value.RetainedBytes),
			"badgerbox_retained_messages_limit": float64(value.MaxRetainedMessages),
			"badgerbox_retained_bytes_limit":    float64(value.MaxRetainedBytes),
		}
		for _, scope := range collected.ScopeMetrics {
			for _, instrument := range scope.Metrics {
				want, ok := wants[instrument.Name]
				if !ok {
					continue
				}
				delete(wants, instrument.Name)
				gauge, ok := instrument.Data.(metricdata.Gauge[float64])
				if !ok || len(gauge.DataPoints) != 1 {
					t.Fatalf("%s data=%+v", instrument.Name, instrument.Data)
				}
				point := gauge.DataPoints[0]
				if point.Value != want || point.Value < 0 || math.IsInf(point.Value, 0) {
					t.Errorf("%s value=%g want=%g", instrument.Name, point.Value, want)
				}
				attrs := point.Attributes.ToSlice()
				if len(attrs) != 1 || attrs[0] != attribute.String("namespace", "orders") {
					t.Errorf("%s cardinality=%v", instrument.Name, attrs)
				}
				if strings.HasSuffix(instrument.Name, "_limit") && !strings.Contains(instrument.Description, "zero means unlimited") {
					t.Errorf("%s lacks zero semantics", instrument.Name)
				}
			}
		}
		if len(wants) != 0 {
			t.Fatalf("missing gauges: %v", wants)
		}
		// Disabling a limit must overwrite its earlier finite gauge value with zero.
		value = AdmissionSnapshot{}
	}
}

func TestAdmissionGaugesAreIndependentOfQueueIndexErrors(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(t.Context())
	indexError := errors.New("index failure")
	queue, err := NewQueue(ObservabilityOptions{MeterProvider: provider}, "orders", func(context.Context) (queueSnapshot, error) {
		return queueSnapshot{}, indexError
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	queue.AdmissionSnapshot = func(context.Context) (AdmissionSnapshot, error) {
		return AdmissionSnapshot{RetainedMessages: 3}, nil
	}
	if err := queue.RecordSnapshot(t.Context()); !errors.Is(err, indexError) {
		t.Fatalf("index error=%v", err)
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &collected); err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, scope := range collected.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if instrument.Name == "badgerbox_retained_messages" {
				seen = true
				gauge := instrument.Data.(metricdata.Gauge[float64])
				if len(gauge.DataPoints) != 1 || gauge.DataPoints[0].Value != 3 {
					t.Fatalf("usage suppressed by index error: %+v", gauge)
				}
			}
		}
	}
	if !seen {
		t.Fatal("usage gauge missing")
	}
}

func TestAdmissionSnapshotErrorsAreObservableWithoutInventingZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(t.Context())
	queue, err := NewQueue(ObservabilityOptions{MeterProvider: provider}, "orders", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	injected := errors.New("accounting unavailable")
	queue.AdmissionSnapshot = func(context.Context) (AdmissionSnapshot, error) { return AdmissionSnapshot{}, injected }
	if err := queue.RecordSnapshot(t.Context()); !errors.Is(err, injected) {
		t.Fatalf("snapshot error=%v", err)
	}
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &collected); err != nil {
		t.Fatal(err)
	}
	seen := false
	for _, scope := range collected.ScopeMetrics {
		for _, instrument := range scope.Metrics {
			if strings.HasPrefix(instrument.Name, "badgerbox_retained_") {
				t.Fatalf("failed snapshot invented a usage point: %s", instrument.Name)
			}
			if instrument.Name == "badgerbox_admission_snapshot_error_total" {
				seen = true
				counter := instrument.Data.(metricdata.Sum[int64])
				if len(counter.DataPoints) != 1 || counter.DataPoints[0].Value != 1 {
					t.Fatalf("error counter=%+v", counter)
				}
			}
		}
	}
	if !seen {
		t.Fatal("accounting read failure was not counted")
	}
}
