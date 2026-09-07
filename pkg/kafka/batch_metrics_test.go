package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/internal/instrumentation"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

type producerFunc func(context.Context, *kgo.Record, func(*kgo.Record, error))

func (f producerFunc) Produce(ctx context.Context, record *kgo.Record, callback func(*kgo.Record, error)) {
	f(ctx, record, callback)
}

type deliveryStats struct {
	produceCalls, scheduled, invalid, promises int
	contextErr                                 error
}

func (s *deliveryStats) RecordKafkaProduce(ctx context.Context, scheduled, invalid int) {
	s.produceCalls++
	s.scheduled += scheduled
	s.invalid += invalid
	s.contextErr = ctx.Err()
}
func (s *deliveryStats) RecordKafkaPromise(context.Context, time.Duration, error) {
	s.promises++
}

func TestKafkaSchedulingMetricsSurvivePartialBatchCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stats := new(deliveryStats)
	ctx = instrumentation.WithDeliveryObserver(ctx, stats)
	fn := newBatchProducerFunc(producerFunc(func(_ context.Context, record *kgo.Record, callback func(*kgo.Record, error)) {
		cancel()
		callback(record, context.Canceled)
	}))
	results := make(chan badgerbox.BatchProcessResult, 3)
	err := fn(ctx, []badgerbox.Message[KafkaMessage, KafkaDestination]{
		{ID: 1}, // Invalid destination was rejected before the cancellation.
		{ID: 2, Destination: KafkaDestination{Topic: "orders"}},
		{ID: 3, Destination: KafkaDestination{Topic: "orders"}},
	}, results)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("batch error=%v, want cancellation", err)
	}
	if stats.produceCalls != 1 || stats.scheduled != 1 || stats.invalid != 1 || stats.promises != 1 || stats.contextErr != nil {
		t.Fatalf("partial batch metrics were lost on cancellation: %+v", stats)
	}
	if len(results) != 2 {
		t.Fatalf("reported results=%d, want 2", len(results))
	}
}
