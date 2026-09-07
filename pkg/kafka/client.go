package kafka

import (
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrPartitionerRequired identifies a client that cannot honor KafkaDestination.Partition.
var ErrPartitionerRequired = errors.New("kafka: client must use KafkaPartitioner")

// NewClient creates a franz-go client with explicit destination partition support.
// The required partitioner takes precedence over any RecordPartitioner in opts.
// The caller owns flushing and closing the client.
func NewClient(opts ...kgo.Opt) (*kgo.Client, error) {
	configured := append([]kgo.Opt(nil), opts...)
	configured = append(configured, kgo.RecordPartitioner(KafkaPartitioner()))
	return kgo.NewClient(configured...)
}

func validateClient(client *kgo.Client) error {
	if client == nil {
		return ErrNilClient
	}
	if _, ok := client.OptValue(kgo.RecordPartitioner).(kafkaDestinationPartitioner); !ok {
		return ErrPartitionerRequired
	}
	return nil
}
