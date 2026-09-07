package kafka

import (
	"context"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"sort"

	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaUnassignedPartition int32 = -1
const kafkaDefaultUniformBytes = 64 << 10

// asyncProducer is the internal seam for deterministic delivery tests.
type asyncProducer interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
}

// KafkaPartitioner returns a franz-go partitioner for KafkaDestination records.
// Nonnegative Record.Partition values are honored explicitly; the -1 value used
// when a destination omits Partition delegates to franz-go's automatic uniform
// bytes partitioner.
func KafkaPartitioner() kgo.Partitioner {
	return kafkaDestinationPartitioner{
		fallback: kgo.UniformBytesPartitioner(kafkaDefaultUniformBytes, true, true, nil),
	}
}

type kafkaDestinationPartitioner struct {
	fallback kgo.Partitioner
}

func (p kafkaDestinationPartitioner) ForTopic(topic string) kgo.TopicPartitioner {
	fallback := p.fallback.ForTopic(topic)
	if backup, ok := fallback.(kgo.TopicBackupPartitioner); ok {
		return kafkaDestinationBackupTopicPartitioner{fallback: backup}
	}
	return kafkaDestinationTopicPartitioner{fallback: fallback}
}

type kafkaDestinationTopicPartitioner struct {
	fallback kgo.TopicPartitioner
}

func (p kafkaDestinationTopicPartitioner) RequiresConsistency(record *kgo.Record) bool {
	if record.Partition != kafkaUnassignedPartition {
		return true
	}
	return p.fallback.RequiresConsistency(record)
}

func (p kafkaDestinationTopicPartitioner) Partition(record *kgo.Record, partitionCount int) int {
	if record.Partition != kafkaUnassignedPartition {
		return int(record.Partition)
	}
	return p.fallback.Partition(record, partitionCount)
}

type kafkaDestinationBackupTopicPartitioner struct {
	fallback kgo.TopicBackupPartitioner
}

func (p kafkaDestinationBackupTopicPartitioner) RequiresConsistency(record *kgo.Record) bool {
	if record.Partition != kafkaUnassignedPartition {
		return true
	}
	return p.fallback.RequiresConsistency(record)
}

func (p kafkaDestinationBackupTopicPartitioner) Partition(record *kgo.Record, partitionCount int) int {
	if record.Partition != kafkaUnassignedPartition {
		return int(record.Partition)
	}
	return p.fallback.Partition(record, partitionCount)
}

func (p kafkaDestinationBackupTopicPartitioner) PartitionByBackup(record *kgo.Record, partitionCount int, backup kgo.TopicBackupIter) int {
	if record.Partition != kafkaUnassignedPartition {
		return int(record.Partition)
	}
	return p.fallback.PartitionByBackup(record, partitionCount, backup)
}

func newKafkaRecord(msg badgerbox.Message[KafkaMessage, KafkaDestination]) (*kgo.Record, error) {
	if msg.Destination.Topic == "" {
		return nil, ErrTopicRequired
	}

	record := &kgo.Record{
		Topic:     msg.Destination.Topic,
		Partition: kafkaUnassignedPartition,
		Key:       cloneKafkaBytes(msg.Payload.Key),
		Value:     cloneKafkaBytes(msg.Payload.Value),
	}
	if msg.Destination.Partition != nil {
		if *msg.Destination.Partition < 0 {
			return nil, ErrInvalidPartition
		}
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
			Value: cloneKafkaBytes(msg.Payload.Headers[key]),
		})
	}
	return record, nil
}

func cloneKafkaBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}
