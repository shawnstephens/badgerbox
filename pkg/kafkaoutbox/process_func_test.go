package kafkaoutbox_test

import (
	"context"
	"errors"
	"testing"

	"badgerbox/pkg/badgerbox"
	"badgerbox/pkg/kafkaoutbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewProcessFuncReturnsErrorForNilClient(t *testing.T) {
	t.Parallel()

	fn := kafkaoutbox.NewProcessFunc(nil, kafkaoutbox.Options{})
	err := fn(context.Background(), badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		Payload: kafkaoutbox.KafkaMessage{Value: []byte("value")},
		Destination: kafkaoutbox.KafkaDestination{
			Topic: "topic",
		},
	})
	if !errors.Is(err, kafkaoutbox.ErrNilClient) {
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

	fn := kafkaoutbox.NewProcessFunc(client, kafkaoutbox.Options{})
	err = fn(context.Background(), badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		Payload: kafkaoutbox.KafkaMessage{Value: []byte("value")},
	})
	if !badgerbox.IsPermanent(err) {
		t.Fatalf("expected permanent error, got %v", err)
	}
	if !errors.Is(err, kafkaoutbox.ErrTopicRequired) {
		t.Fatalf("expected ErrTopicRequired, got %v", err)
	}
}
