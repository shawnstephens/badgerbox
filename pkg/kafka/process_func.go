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

type Producer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
}

type Options struct{}

func NewProcessFunc(client *kgo.Client, opts Options) badgerbox.ProcessFunc[KafkaMessage, KafkaDestination] {
	if client == nil {
		return func(context.Context, badgerbox.Message[KafkaMessage, KafkaDestination]) error {
			return ErrNilClient
		}
	}
	return NewProcessFuncWithProducer(client, opts)
}

func NewProcessFuncWithProducer(producer Producer, _ Options) badgerbox.ProcessFunc[KafkaMessage, KafkaDestination] {
	return func(ctx context.Context, msg badgerbox.Message[KafkaMessage, KafkaDestination]) error {
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
