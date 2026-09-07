// Package metricconfig provides instrument defaults shared by queue and database telemetry.
package metricconfig

import "go.opentelemetry.io/otel/metric"

// DurationHistogram supplies second-scale boundaries even when the embedding
// application uses an unconfigured OpenTelemetry SDK. SDK views may override
// these advisory boundaries, including selecting exponential histograms.
func DurationHistogram(meter metric.Meter, name string) (metric.Float64Histogram, error) {
	return meter.Float64Histogram(name,
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(
			0.00001, 0.00005, 0.0001, 0.00025, 0.0005,
			0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
			1, 2.5, 5, 10, 30, 60, 300, 900, 3600, 21600, 86400,
		),
	)
}
