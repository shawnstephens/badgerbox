package telemetry

import (
	"context"
	"github.com/shawnstephens/badgerbox/internal/metricconfig"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"time"
)

// DeliveryObserver records flush operations for one named shared delivery client.
// Keep names fixed by application configuration to bound metric cardinality.
type DeliveryObserver struct {
	name               string
	attempts, failures metric.Int64Counter
	duration           metric.Float64Histogram
}

func NewDeliveryObserver(options Options, name string) (*DeliveryObserver, error) {
	observer := &DeliveryObserver{name: name}
	if options.MeterProvider == nil {
		return observer, nil
	}
	scope := options.MeterName
	if scope == "" {
		scope = defaultInstrumentationName
	}
	meter := options.MeterProvider.Meter(scope)
	var err error
	if observer.attempts, err = meter.Int64Counter("badgerbox_delivery_flush_total"); err != nil {
		return nil, err
	}
	if observer.failures, err = meter.Int64Counter("badgerbox_delivery_flush_error_total"); err != nil {
		return nil, err
	}
	if observer.duration, err = metricconfig.DurationHistogram(meter, "badgerbox_delivery_flush_duration_seconds"); err != nil {
		return nil, err
	}
	return observer, nil
}
func (o *DeliveryObserver) ObserveFlush(ctx context.Context, duration time.Duration, err error) {
	if o == nil || o.attempts == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("delivery", o.name))
	o.attempts.Add(ctx, 1, attrs)
	o.duration.Record(ctx, max(0, duration.Seconds()), attrs)
	if err != nil {
		o.failures.Add(ctx, 1, attrs)
	}
}
