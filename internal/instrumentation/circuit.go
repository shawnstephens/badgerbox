package instrumentation

import (
	"context"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"time"
)

// Breaker instruments use only bounded state/outcome labels and never scan Badger.
func (o *Queue) RecordCircuitState(ctx context.Context, state string) {
	if o.circuitState == nil {
		return
	}
	var value int64
	switch state {
	case "open":
		value = 1
	case "half_open":
		value = 2
	}
	o.circuitState.Record(ctx, value, metric.WithAttributes(attribute.String("namespace", o.namespace)))
}
func (o *Queue) RecordCircuitTransition(ctx context.Context, from, to string) {
	o.RecordCircuitState(ctx, to)
	if o.circuitTransitions != nil {
		o.circuitTransitions.Add(ctx, 1, metric.WithAttributes(attribute.String("namespace", o.namespace), attribute.String("from", string(from)), attribute.String("to", string(to))))
	}
}
func (o *Queue) RecordCircuitTrial(ctx context.Context, outcome string) {
	if o.circuitTrials != nil {
		o.circuitTrials.Add(ctx, 1, metric.WithAttributes(attribute.String("namespace", o.namespace), attribute.String("outcome", outcome)))
	}
}
func (o *Queue) RecordCircuitDeferred(ctx context.Context, reason string) {
	if o.circuitDeferred != nil {
		o.circuitDeferred.Add(ctx, 1, metric.WithAttributes(attribute.String("namespace", o.namespace), attribute.String("reason", reason)))
	}
}
func (o *Queue) RecordCircuitOpenDuration(ctx context.Context, duration time.Duration) {
	if o.circuitOpenDuration != nil {
		o.circuitOpenDuration.Record(ctx, positiveDuration(duration).Seconds(), metric.WithAttributes(attribute.String("namespace", o.namespace)))
	}
}

func (o *Queue) RecordCircuitRecoveryDelay(ctx context.Context, reason string, duration time.Duration) {
	if o.circuitRecoveryDelay != nil {
		o.circuitRecoveryDelay.Record(ctx, positiveDuration(duration).Seconds(), metric.WithAttributes(attribute.String("namespace", o.namespace), attribute.String("reason", reason)))
	}
}

func (o *Queue) RecordProcessDeferred(ctx context.Context, d time.Duration) {
	o.RecordProcessOutcome(ctx, "deferred", "unavailable", d)
}
