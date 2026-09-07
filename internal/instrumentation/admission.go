package instrumentation

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// AdmissionSnapshot is the payload-free namespace accounting used by metrics.
// The store adapter supplies a transactionally consistent persisted snapshot.
type AdmissionSnapshot struct {
	RetainedMessages    uint64
	RetainedBytes       uint64
	MaxRetainedMessages uint64
	MaxRetainedBytes    uint64
}

type admissionGauges struct {
	messages, bytes, messageLimit, byteLimit metric.Float64Gauge
	errors                                   metric.Int64Counter
}

func newAdmissionGauges(meter metric.Meter) (admissionGauges, error) {
	var gauges admissionGauges
	for _, spec := range []struct {
		name, unit, description string
		target                  *metric.Float64Gauge
	}{
		{"badgerbox_retained_messages", "{message}", "Retained messages across ready, processing and dead-letter states", &gauges.messages},
		{"badgerbox_retained_bytes", "By", "Canonical retained record bytes; excludes indexes and physical storage overhead", &gauges.bytes},
		{"badgerbox_retained_messages_limit", "{message}", "Persisted retained message limit; zero means unlimited", &gauges.messageLimit},
		{"badgerbox_retained_bytes_limit", "By", "Persisted canonical retained byte limit; zero means unlimited", &gauges.byteLimit},
	} {
		instrument, err := meter.Float64Gauge(spec.name, metric.WithUnit(spec.unit), metric.WithDescription(spec.description))
		if err != nil {
			return admissionGauges{}, err
		}
		*spec.target = instrument
	}
	var err error
	gauges.errors, err = meter.Int64Counter("badgerbox_admission_snapshot_error_total", metric.WithDescription("Failed persisted admission usage reads"))
	return gauges, err
}

func (o *Queue) recordAdmissionSnapshot(ctx context.Context) error {
	if o.AdmissionSnapshot == nil || o.admission.messages == nil {
		return nil
	}
	value, err := o.AdmissionSnapshot(ctx)
	attrs := metric.WithAttributes(namespaceAttributes(o.namespace)...)
	if err != nil {
		o.admission.errors.Add(ctx, 1, attrs)
		return err
	}
	// Float64 keeps the entire uint64 range nonnegative instead of wrapping at
	// MaxInt64. Above 2^53, these monitoring values may round; the public Usage
	// snapshot and HTTP JSON preserve integer values for exact inspection.
	o.admission.messages.Record(ctx, float64(value.RetainedMessages), attrs)
	o.admission.bytes.Record(ctx, float64(value.RetainedBytes), attrs)
	o.admission.messageLimit.Record(ctx, float64(value.MaxRetainedMessages), attrs)
	o.admission.byteLimit.Record(ctx, float64(value.MaxRetainedBytes), attrs)
	return nil
}
