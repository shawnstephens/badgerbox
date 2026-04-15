package demo

import (
	"context"
	"testing"
	"time"
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
