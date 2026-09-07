package kafka

import (
	"context"
	"errors"
	"github.com/shawnstephens/badgerbox/internal/instrumentation"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kgo"
	"time"
)

var ErrInvalidPartition = errors.New("kafka: partition must be nonnegative")

// NewBatchProducerFunc validates the client before scheduling asynchronous delivery.
// Callbacks only report results; durable settlement remains owned by the processor.
func NewBatchProducerFunc(client *kgo.Client) (badgerbox.BatchProcessFunc[KafkaMessage, KafkaDestination], error) {
	if err := validateClient(client); err != nil {
		return nil, err
	}
	return newBatchProducerFunc(client), nil
}

func newBatchProducerFunc(producer asyncProducer) badgerbox.BatchProcessFunc[KafkaMessage, KafkaDestination] {
	return func(ctx context.Context, messages []badgerbox.Message[KafkaMessage, KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
		if ctx == nil {
			return badgerbox.ErrNilContext
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		obs := instrumentation.DeliveryFromContext(ctx)
		scheduled, failed := 0, 0
		for _, msg := range messages {
			if err := ctx.Err(); err != nil {
				return err
			}
			if producer == nil {
				results <- badgerbox.BatchProcessResult{ID: msg.ID, Err: ErrNilClient}
				failed++
				continue
			}
			record, err := newKafkaRecord(msg)
			if err != nil {
				results <- badgerbox.BatchProcessResult{ID: msg.ID, Err: badgerbox.Permanent(err)}
				failed++
				continue
			}
			id := msg.ID
			started := time.Now()
			scheduled++
			messageCtx := badgerbox.ContextForMessage(ctx, msg.ID)
			producer.Produce(messageCtx, record, func(_ *kgo.Record, err error) {
				if obs != nil {
					obs.RecordKafkaPromise(context.WithoutCancel(messageCtx), time.Since(started), err)
				}
				results <- badgerbox.BatchProcessResult{ID: id, Err: err}
			})
		}
		if obs != nil {
			obs.RecordKafkaProduce(context.WithoutCancel(ctx), scheduled, failed)
		}
		return nil
	}
}
