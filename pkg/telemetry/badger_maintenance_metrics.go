package telemetry

import (
	"context"
	"errors"
	"github.com/shawnstephens/badgerbox/internal/metricconfig"
	"github.com/shawnstephens/badgerbox/pkg/maintenance"
	"strings"

	"github.com/dgraph-io/badger/v4"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	badgerMaintenanceAttemptsMetric = "badgerbox_badger_maintenance_attempts"
	badgerMaintenanceOutcomesMetric = "badgerbox_badger_maintenance_outcomes"
	badgerMaintenanceRewritesMetric = "badgerbox_badger_maintenance_rewrites"
	badgerMaintenanceDurationMetric = "badgerbox_badger_maintenance_duration_seconds"
)

// BadgerMaintenanceMetricsOptions configures OpenTelemetry instruments for
// database-wide Badger maintenance.
type BadgerMaintenanceMetricsOptions struct {
	// MeterProvider registers the maintenance instruments and is required.
	MeterProvider metric.MeterProvider
	// MeterName overrides the module's default OpenTelemetry instrumentation scope.
	MeterName string
}

// BadgerMaintenanceMetrics observes completed startup Flatten and periodic
// value-log GC operations.
type BadgerMaintenanceMetrics struct {
	attempt  metric.Int64Counter
	outcome  metric.Int64Counter
	rewrite  metric.Int64Counter
	duration metric.Float64Histogram
}

// NewBadgerMaintenanceMetrics registers Badger maintenance instruments. A
// blank meter name uses the badgerbox module's default instrumentation scope.
func NewBadgerMaintenanceMetrics(
	options BadgerMaintenanceMetricsOptions,
) (*BadgerMaintenanceMetrics, error) {
	if options.MeterProvider == nil {
		return nil, telemetryErrorf("Badger maintenance metrics meter provider is nil")
	}
	meterName := strings.TrimSpace(options.MeterName)
	if meterName == "" {
		meterName = defaultInstrumentationName
	}
	meter := options.MeterProvider.Meter(meterName)
	telemetry := &BadgerMaintenanceMetrics{}
	var err error
	if telemetry.attempt, err = meter.Int64Counter(badgerMaintenanceAttemptsMetric); err != nil {
		return nil, telemetryErrorf("register Badger maintenance attempt counter: %w", err)
	}
	if telemetry.outcome, err = meter.Int64Counter(badgerMaintenanceOutcomesMetric); err != nil {
		return nil, telemetryErrorf("register Badger maintenance outcome counter: %w", err)
	}
	if telemetry.rewrite, err = meter.Int64Counter(badgerMaintenanceRewritesMetric); err != nil {
		return nil, telemetryErrorf("register Badger maintenance rewrite counter: %w", err)
	}
	if telemetry.duration, err = metricconfig.DurationHistogram(meter, badgerMaintenanceDurationMetric); err != nil {
		return nil, telemetryErrorf("register Badger maintenance duration histogram: %w", err)
	}
	return telemetry, nil
}

// ObserveMaintenance records one completed maintenance operation.
// ErrNoRewrite is recorded as a normal no_rewrite outcome, and only successful
// value-log GC operations increment the rewrite counter.
func (t *BadgerMaintenanceMetrics) ObserveMaintenance(
	ctx context.Context,
	result maintenance.Result,
) {
	operation := string(result.Operation)
	operationAttribute := attribute.String("operation", operation)
	t.attempt.Add(ctx, 1, metric.WithAttributes(operationAttribute))
	outcome := "success"
	if errors.Is(result.Err, badger.ErrNoRewrite) {
		outcome = "no_rewrite"
	} else if result.Err != nil {
		outcome = "error"
	}
	t.outcome.Add(
		ctx,
		1,
		metric.WithAttributes(operationAttribute, attribute.String("outcome", outcome)),
	)
	if result.Operation == maintenance.OperationValueLogGC && result.Err == nil {
		t.rewrite.Add(ctx, 1, metric.WithAttributes(operationAttribute))
	}
	t.duration.Record(ctx, result.Duration.Seconds(), metric.WithAttributes(operationAttribute))
}

var _ maintenance.Observer = (*BadgerMaintenanceMetrics)(nil)
