package kafka

import (
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrPartitionerRequired identifies a client that cannot honor KafkaDestination.Partition.
var ErrPartitionerRequired = errors.New("kafka: client must use KafkaPartitioner")

// ErrAllISRAcksRequired identifies clients that do not wait for in-sync replicas
// before reporting success and settling an outbox record.
var ErrAllISRAcksRequired = errors.New("kafka: client must use RequiredAcks(AllISRAcks())")

// ErrTransactionalClient identifies clients whose successful produce callbacks
// do not prove that the surrounding Kafka transaction has committed.
var ErrTransactionalClient = errors.New("kafka: transactional clients cannot settle outbox records before transaction commit")

// ErrTopicOverride identifies clients that ignore KafkaDestination.Topic.
var ErrTopicOverride = errors.New("kafka: client must not use DefaultProduceTopicAlways")

// NewClient creates a franz-go client with explicit destination partition support.
// The required partitioner takes precedence over any RecordPartitioner in opts.
// Clients must require AllISRAcks and must not use TransactionalID or
// DefaultProduceTopicAlways: successful delivery must acknowledge the message
// at its destination before the processor removes its durable outbox record.
// The caller owns flushing and closing the client.
func NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	configured := append([]kgo.Opt(nil), opts...)
	configured = append(configured, kgo.RecordPartitioner(KafkaPartitioner()))
	client, err := kgo.NewClient(configured...)
	if err != nil {
		return nil, err
	}
	if err := validateClient(client); err != nil {
		client.Close()
		return nil, err
	}
	return client, nil
}

func validateClient(client *kgo.Client) error {
	if client == nil {
		return ErrNilClient
	}
	if _, ok := client.OptValue(kgo.RecordPartitioner).(kafkaDestinationPartitioner); !ok {
		return ErrPartitionerRequired
	}
	if client.OptValue(kgo.RequiredAcks) != kgo.AllISRAcks() {
		return ErrAllISRAcksRequired
	}
	if client.OptValue(kgo.TransactionalID) != "" {
		return ErrTransactionalClient
	}
	if client.OptValue(kgo.DefaultProduceTopicAlways) == true {
		return ErrTopicOverride
	}
	return nil
}
