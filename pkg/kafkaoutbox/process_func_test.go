package kafkaoutbox

import (
	"context"
	"errors"
	"testing"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

type stubProducer struct {
	records []*kgo.Record
	err     error
}

func (s *stubProducer) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	s.records = append(s.records, records...)

	results := make(kgo.ProduceResults, len(records))
	for i, record := range records {
		results[i] = kgo.ProduceResult{Record: record, Err: s.err}
	}
	return results
}

func TestNewProcessFuncReturnsErrorForNilClient(t *testing.T) {
	t.Parallel()

	fn := NewProcessFunc(nil, Options{})
	err := fn(context.Background(), badgerbox.Message[KafkaMessage, KafkaDestination]{
		Payload: KafkaMessage{Value: []byte("value")},
		Destination: KafkaDestination{
			Topic: "topic",
		},
	})
	if !errors.Is(err, ErrNilClient) {
		t.Fatalf("expected ErrNilClient, got %v", err)
	}
}

func TestNewProcessFuncRejectsMissingTopic(t *testing.T) {
	t.Parallel()

	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatalf("new kafka client: %v", err)
	}
	defer client.Close()

	fn := NewProcessFunc(client, Options{})
	err = fn(context.Background(), badgerbox.Message[KafkaMessage, KafkaDestination]{
		Payload: KafkaMessage{Value: []byte("value")},
	})
	if !badgerbox.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if !errors.Is(err, ErrTopicRequired) {
		t.Fatalf("expected ErrTopicRequired, got %v", err)
	}
}

func TestNewProcessFuncWithProducerProducesRecord(t *testing.T) {
	t.Parallel()

	partition := int32(7)
	sourceKey := []byte("order-1")
	sourceValue := []byte("created")
	sourceHeaderA := []byte("a-value")
	sourceHeaderB := []byte("b-value")
	producer := &stubProducer{}

	fn := NewProcessFuncWithProducer(producer, Options{})
	err := fn(context.Background(), badgerbox.Message[KafkaMessage, KafkaDestination]{
		Payload: KafkaMessage{
			Key:   sourceKey,
			Value: sourceValue,
			Headers: map[string][]byte{
				"beta":  sourceHeaderB,
				"alpha": sourceHeaderA,
			},
		},
		Destination: KafkaDestination{
			Topic:     "orders",
			Partition: &partition,
		},
	})
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	if len(producer.records) != 1 {
		t.Fatalf("records produced = %d, want 1", len(producer.records))
	}

	record := producer.records[0]
	if record.Topic != "orders" {
		t.Fatalf("record topic = %q, want orders", record.Topic)
	}
	if record.Partition != partition {
		t.Fatalf("record partition = %d, want %d", record.Partition, partition)
	}
	if string(record.Key) != string(sourceKey) {
		t.Fatalf("record key = %q, want %q", record.Key, sourceKey)
	}
	if string(record.Value) != string(sourceValue) {
		t.Fatalf("record value = %q, want %q", record.Value, sourceValue)
	}
	if len(record.Headers) != 2 {
		t.Fatalf("headers len = %d, want 2", len(record.Headers))
	}
	if record.Headers[0].Key != "alpha" || record.Headers[1].Key != "beta" {
		t.Fatalf("header order = %#v, want alpha then beta", record.Headers)
	}
	if string(record.Headers[0].Value) != string(sourceHeaderA) || string(record.Headers[1].Value) != string(sourceHeaderB) {
		t.Fatalf("header values = %#v", record.Headers)
	}

	sourceKey[0] = 'X'
	sourceValue[0] = 'X'
	sourceHeaderA[0] = 'X'
	sourceHeaderB[0] = 'X'

	if string(record.Key) != "order-1" {
		t.Fatalf("record key aliased source slice: %q", record.Key)
	}
	if string(record.Value) != "created" {
		t.Fatalf("record value aliased source slice: %q", record.Value)
	}
	if string(record.Headers[0].Value) != "a-value" || string(record.Headers[1].Value) != "b-value" {
		t.Fatalf("record headers aliased source slices: %#v", record.Headers)
	}
}

func TestNewProcessFuncWithProducerPropagatesProduceError(t *testing.T) {
	t.Parallel()

	produceErr := errors.New("produce failed")
	producer := &stubProducer{err: produceErr}

	fn := NewProcessFuncWithProducer(producer, Options{})
	err := fn(context.Background(), badgerbox.Message[KafkaMessage, KafkaDestination]{
		Payload: KafkaMessage{Value: []byte("value")},
		Destination: KafkaDestination{
			Topic: "orders",
		},
	})
	if !errors.Is(err, produceErr) {
		t.Fatalf("expected produce error, got %v", err)
	}
}

func TestNewProcessFuncWithProducerRejectsMissingTopic(t *testing.T) {
	t.Parallel()

	producer := &stubProducer{}
	fn := NewProcessFuncWithProducer(producer, Options{})

	err := fn(context.Background(), badgerbox.Message[KafkaMessage, KafkaDestination]{
		Payload: KafkaMessage{Value: []byte("value")},
	})
	if !badgerbox.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if !errors.Is(err, ErrTopicRequired) {
		t.Fatalf("expected ErrTopicRequired, got %v", err)
	}
	if len(producer.records) != 0 {
		t.Fatalf("producer invoked for invalid message")
	}
}

func TestCloneBytes(t *testing.T) {
	t.Parallel()

	if got := cloneBytes(nil); got != nil {
		t.Fatalf("cloneBytes(nil) = %#v, want nil", got)
	}

	source := []byte("payload")
	cloned := cloneBytes(source)
	if string(cloned) != string(source) {
		t.Fatalf("clone = %q, want %q", cloned, source)
	}

	source[0] = 'X'
	if string(cloned) != "payload" {
		t.Fatalf("clone aliased source slice: %q", cloned)
	}
}
