package demo

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeProducerClient struct {
	produceErr error
	closed     bool
}

func (c *fakeProducerClient) ProduceSync(context.Context, *kgo.Record) error {
	return c.produceErr
}

func (c *fakeProducerClient) Close() error {
	c.closed = true
	return nil
}

func TestReloadingPublisherReloadFromStateRecreatesClientOnBrokerChange(t *testing.T) {
	t.Parallel()

	publisher := NewReloadingPublisher("state.json", []string{"old:9092"}, "demo-topic", nil)
	oldClient := &fakeProducerClient{}
	publisher.client = oldClient

	newClient := &fakeProducerClient{}
	factoryCalls := 0
	publisher.newClient = func(brokers []string) (producerClient, error) {
		factoryCalls++
		if len(brokers) != 1 || brokers[0] != "new:9092" {
			t.Fatalf("unexpected brokers: %#v", brokers)
		}
		return newClient, nil
	}
	publisher.readState = func(string) (State, error) {
		return State{
			Version: 1,
			Brokers: []string{"new:9092"},
			Topic:   "demo-topic",
		}, nil
	}

	result, err := publisher.ReloadFromState()
	if err != nil {
		t.Fatalf("ReloadFromState() error = %v", err)
	}
	if !result.Reloaded || !result.BrokersChanged {
		t.Fatalf("expected broker reload, got %#v", result)
	}
	if factoryCalls != 1 {
		t.Fatalf("expected factory to be called once, got %d", factoryCalls)
	}
	if !oldClient.closed {
		t.Fatal("expected previous client to be closed")
	}
	if publisher.client != newClient {
		t.Fatal("expected publisher to hold new client")
	}
}

func TestReloadingPublisherReloadFromStateDoesNotRecreateClientWhenStateUnchanged(t *testing.T) {
	t.Parallel()

	publisher := NewReloadingPublisher("state.json", []string{"same:9092"}, "demo-topic", nil)
	currentClient := &fakeProducerClient{}
	publisher.client = currentClient

	factoryCalls := 0
	publisher.newClient = func([]string) (producerClient, error) {
		factoryCalls++
		return &fakeProducerClient{}, nil
	}
	publisher.readState = func(string) (State, error) {
		return State{
			Version: 1,
			Brokers: []string{"same:9092"},
			Topic:   "demo-topic",
		}, nil
	}

	result, err := publisher.ReloadFromState()
	if err != nil {
		t.Fatalf("ReloadFromState() error = %v", err)
	}
	if result.Reloaded {
		t.Fatalf("expected no reload, got %#v", result)
	}
	if factoryCalls != 0 {
		t.Fatalf("expected factory to not be called, got %d", factoryCalls)
	}
	if currentClient.closed {
		t.Fatal("expected current client to remain open")
	}
}

func TestReloadingPublisherPublishReloadsClientAfterFailure(t *testing.T) {
	t.Parallel()

	publisher := NewReloadingPublisher("state.json", []string{"old:9092"}, "demo-topic", nil)
	downClient := &fakeProducerClient{produceErr: errors.New("dial tcp old:9092: connect refused")}
	publisher.client = downClient

	newClient := &fakeProducerClient{}
	factoryCalls := 0
	publisher.newClient = func(brokers []string) (producerClient, error) {
		factoryCalls++
		if len(brokers) != 1 || brokers[0] != "new:9092" {
			t.Fatalf("unexpected brokers: %#v", brokers)
		}
		return newClient, nil
	}
	publisher.readState = func(string) (State, error) {
		return State{
			Version: 1,
			Brokers: []string{"new:9092"},
			Topic:   "demo-topic",
		}, nil
	}

	err := publisher.Publish(context.Background(), badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		ID: 42,
		Payload: kafkaoutbox.KafkaMessage{
			Key:   []byte("demo-key"),
			Value: []byte("demo-value"),
		},
		Destination: kafkaoutbox.KafkaDestination{Topic: "demo-topic"},
	})
	if err == nil {
		t.Fatal("expected publish error")
	}
	if factoryCalls != 1 {
		t.Fatalf("expected one client rebuild, got %d", factoryCalls)
	}
	if !downClient.closed {
		t.Fatal("expected failed client to be closed")
	}
	if publisher.client != newClient {
		t.Fatal("expected publisher to swap in rebuilt client")
	}
}

func TestReloadingPublisherPublishLogsAttemptMetadata(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := NewLogger(&out, string(ColorNever))

	publisher := NewReloadingPublisher("state.json", []string{"old:9092"}, "demo-topic", logger)
	downClient := &fakeProducerClient{produceErr: errors.New("dial tcp old:9092: connect refused")}
	publisher.client = downClient
	publisher.readState = func(string) (State, error) {
		return State{
			Version: 1,
			Brokers: []string{"old:9092"},
			Topic:   "demo-topic",
		}, nil
	}

	err := publisher.Publish(context.Background(), badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		ID:          42,
		Attempt:     3,
		MaxAttempts: 10,
		Payload: kafkaoutbox.KafkaMessage{
			Key:   []byte("demo-key"),
			Value: []byte("demo-value"),
		},
		Destination: kafkaoutbox.KafkaDestination{Topic: "demo-topic"},
	})
	if err == nil {
		t.Fatal("expected publish error")
	}

	logs := out.String()
	if !strings.Contains(logs, "event=publish_failed") {
		t.Fatalf("expected publish_failed log, got %q", logs)
	}
	if !strings.Contains(logs, "attempt=3") || !strings.Contains(logs, "max_attempts=10") {
		t.Fatalf("expected attempt metadata in log, got %q", logs)
	}
}

func TestLoggingPublisherPublishLogsSummary(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := NewLogger(&out, string(ColorNever))
	publisher := NewLoggingPublisher(logger)

	err := publisher.Publish(context.Background(), badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		ID:      77,
		Attempt: 4,
		Payload: kafkaoutbox.KafkaMessage{
			Key:   []byte("demo-key"),
			Value: []byte(`{"message":"hello"}`),
			Headers: map[string][]byte{
				"demo-source": []byte("badgerbox-demo"),
				"sequence":    []byte("77"),
			},
		},
		Destination: kafkaoutbox.KafkaDestination{Topic: "demo-topic"},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	logs := out.String()
	if !strings.Contains(logs, "event=logged") {
		t.Fatalf("expected logged event, got %q", logs)
	}
	if !strings.Contains(logs, "msg_id=77") || !strings.Contains(logs, "attempt=4") {
		t.Fatalf("expected message metadata, got %q", logs)
	}
	if !strings.Contains(logs, "key=demo-key") || !strings.Contains(logs, "topic=demo-topic") {
		t.Fatalf("expected key and topic, got %q", logs)
	}
	if !strings.Contains(logs, "payload_bytes=19") || !strings.Contains(logs, "header_count=2") {
		t.Fatalf("expected payload/header summary, got %q", logs)
	}
}

func TestLoggingPublisherCloseNoop(t *testing.T) {
	t.Parallel()

	publisher := NewLoggingPublisher(nil)
	if err := publisher.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
