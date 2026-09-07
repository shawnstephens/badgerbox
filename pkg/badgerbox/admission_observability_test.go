package badgerbox

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestStoreRecordsPersistedAdmissionGauges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(t.Context())
	limits := AdmissionLimits{MaxRetainedMessages: 10, MaxRetainedBytes: 1 << 20}
	_, s, cleanup := openTestStoreWithOptions(t, "admission-metrics", Serde[string, string]{}, Options{AdmissionLimits: limits, Observability: ObservabilityOptions{MeterProvider: provider}})
	defer cleanup()
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "retained"}); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []AdmissionLimits{limits, {}} {
		if err := s.CompareAndSwapAdmissionLimits(t.Context(), limits, limit); err != nil {
			t.Fatal(err)
		}
		usage, err := s.Usage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := s.RecordObservabilitySnapshot(t.Context()); err != nil {
			t.Fatal(err)
		}
		var collected metricdata.ResourceMetrics
		if err := reader.Collect(t.Context(), &collected); err != nil {
			t.Fatal(err)
		}
		for name, want := range map[string]float64{
			"badgerbox_retained_messages":       float64(usage.RetainedMessages),
			"badgerbox_retained_bytes":          float64(usage.RetainedBytes),
			"badgerbox_retained_messages_limit": float64(usage.Limits.MaxRetainedMessages),
			"badgerbox_retained_bytes_limit":    float64(usage.Limits.MaxRetainedBytes),
		} {
			if got := float64GaugeValueWithAttrs(collected, name, attribute.String("namespace", s.opts.Namespace)); got != want {
				t.Errorf("%s=%g want=%g", name, got, want)
			}
		}
	}
}
