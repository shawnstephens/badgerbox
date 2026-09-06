// Package instrumentation carries private telemetry between processing adapters.
package instrumentation

import (
	"context"
	"time"
)

type DeliveryObserver interface {
	RecordKafkaProduce(context.Context, int, int)
	RecordKafkaPromise(context.Context, time.Duration, error)
}
type key struct{}

func WithDeliveryObserver(ctx context.Context, o DeliveryObserver) context.Context {
	return context.WithValue(ctx, key{}, o)
}
func DeliveryFromContext(ctx context.Context) DeliveryObserver {
	o, _ := ctx.Value(key{}).(DeliveryObserver)
	return o
}
