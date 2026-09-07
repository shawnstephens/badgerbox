package kafka

import (
	"context"
	"errors"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrNilClient     = errors.New("kafka: client is nil")
	ErrTopicRequired = errors.New("kafka: topic is required")
)

type syncProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

type Options struct{}

// NewProcessFunc validates the client before returning a single-message delivery function.
func NewProcessFunc(client *kgo.Client, opts Options) (badgerbox.ProcessFunc[KafkaMessage, KafkaDestination], error) {
	if err := validateClient(client); err != nil {
		return nil, err
	}
	return newProcessFunc(client, opts), nil
}

func newProcessFunc(producer syncProducer, _ Options) badgerbox.ProcessFunc[KafkaMessage, KafkaDestination] {
	return func(ctx context.Context, msg badgerbox.Message[KafkaMessage, KafkaDestination]) error {
		if ctx == nil {
			return badgerbox.ErrNilContext
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		ctx = badgerbox.ContextForMessage(ctx, msg.ID)
		if producer == nil {
			return ErrNilClient
		}
		record, err := newKafkaRecord(msg)
		if err != nil {
			return badgerbox.Permanent(err)
		}

		return producer.ProduceSync(ctx, record).FirstErr()
	}
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}
