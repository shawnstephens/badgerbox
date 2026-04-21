package demo

import (
	"context"
	"slices"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestSetupOTelDisabledReturnsNoop(t *testing.T) {
	t.Parallel()

	obs, shutdown, err := SetupOTel(context.Background(), OTelConfig{})
	if err != nil {
		t.Fatalf("setup otel: %v", err)
	}
	if obs.MeterProvider != nil || obs.TracerProvider != nil {
		t.Fatalf("expected empty observability, got %#v", obs)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown noop: %v", err)
	}
}

func TestSetupOTelEnabledBuildsObservability(t *testing.T) {
	t.Parallel()

	obs, shutdown, err := SetupOTel(context.Background(), OTelConfig{
		Endpoint:    "localhost:4318",
		ServiceName: "badgerbox-demo-test",
	})
	if err != nil {
		t.Fatalf("setup otel: %v", err)
	}
	if obs.MeterProvider == nil || obs.TracerProvider == nil {
		t.Fatalf("expected populated observability, got %#v", obs)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = shutdown(shutdownCtx)
}

func TestLatencyHistogramViewUsesMillisecondBuckets(t *testing.T) {
	t.Parallel()

	view := latencyHistogramView("badgerbox_process_duration_seconds")
	stream, ok := view(sdkmetric.Instrument{
		Name: "badgerbox_process_duration_seconds",
		Kind: sdkmetric.InstrumentKindHistogram,
	})
	if !ok {
		t.Fatal("expected process duration histogram view to match")
	}

	histogram, ok := stream.Aggregation.(sdkmetric.AggregationExplicitBucketHistogram)
	if !ok {
		t.Fatalf("aggregation type = %T, want explicit bucket histogram", stream.Aggregation)
	}
	if !slices.Equal(histogram.Boundaries, latencyHistogramBoundaries) {
		t.Fatalf("boundaries = %#v, want %#v", histogram.Boundaries, latencyHistogramBoundaries)
	}
	if histogram.NoMinMax {
		t.Fatal("NoMinMax = true, want false")
	}
}

func TestLatencyHistogramViewLeavesOtherMetricsUntouched(t *testing.T) {
	t.Parallel()

	view := latencyHistogramView("badgerbox_enqueue_duration_seconds")

	if _, ok := view(sdkmetric.Instrument{
		Name: "badgerbox_retry_delay_seconds",
		Kind: sdkmetric.InstrumentKindHistogram,
	}); ok {
		t.Fatal("unexpected match for unrelated histogram")
	}

	if _, ok := view(sdkmetric.Instrument{
		Name: "badgerbox_enqueue_duration_seconds",
		Kind: sdkmetric.InstrumentKindCounter,
	}); ok {
		t.Fatal("unexpected match for non-histogram instrument kind")
	}
}
