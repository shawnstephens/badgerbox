package kafkaoutbox

import (
	"context"
	"errors"
	"sort"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrNilClient     = errors.New("kafkaoutbox: client is nil")
	ErrTopicRequired = errors.New("kafkaoutbox: topic is required")
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
		if msg.Destination.Topic == "" {
			return badgerbox.Permanent(ErrTopicRequired)
		}

		record := &kgo.Record{
			Topic: msg.Destination.Topic,
			Key:   cloneBytes(msg.Payload.Key),
			Value: cloneBytes(msg.Payload.Value),
		}
		if msg.Destination.Partition != nil {
			record.Partition = *msg.Destination.Partition
		}

		headerKeys := make([]string, 0, len(msg.Payload.Headers))
		for key := range msg.Payload.Headers {
			headerKeys = append(headerKeys, key)
		}
		sort.Strings(headerKeys)
		for _, key := range headerKeys {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   key,
				Value: cloneBytes(msg.Payload.Headers[key]),
			})
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
