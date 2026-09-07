// Package telemetry provides exporter-neutral OpenTelemetry configuration and database metrics.
package telemetry

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	"time"
)

type Options struct {
	MeterProvider  metric.MeterProvider
	TracerProvider oteltrace.TracerProvider
	Propagator     propagation.TextMapPropagator
	MeterName      string
	TracerName     string
	PollInterval   time.Duration
	// DurationMaxWindow retains the current and previous fixed windows of duration maxima.
	// Zero selects one minute. Use at least the longest reader collection interval.
	DurationMaxWindow time.Duration
}

type QueueSnapshot struct {
	ReadyDepth          int64
	ProcessingDepth     int64
	DeadLetterDepth     int64
	OldestReadyAge      time.Duration
	OldestProcessingAge time.Duration
}
