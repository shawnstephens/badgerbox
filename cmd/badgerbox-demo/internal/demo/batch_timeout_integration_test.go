//go:build integration

package demo

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDemoTimeoutPreservesMissingTopicCause(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	container, brokers, err := StartKafka(ctx, DefaultKafkaImage, DefaultClusterID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		if err := container.Terminate(cleanup); err != nil {
			t.Error(err)
		}
	})
	publisher := NewReloadingPublisher("state", brokers, "topic", nil,
		kgo.RecordDeliveryTimeout(time.Second), kgo.UnknownTopicRetries(-1),
		kgo.MetadataMinAge(10*time.Millisecond), kgo.MetadataMaxAge(100*time.Millisecond))
	defer publisher.Close()
	publisher.readState = func(string) (State, error) { return State{Brokers: brokers, Topic: "topic"}, nil }
	client, _, err := publisher.ensureClient()
	if err != nil {
		t.Fatal(err)
	}
	topic := fmt.Sprintf("demo-deadline-%d", time.Now().UnixNano())
	if err := CreateTopic(ctx, client.(*franzProducerClient).client, topic, 1); err != nil {
		t.Fatal(err)
	}
	fn := NewBatchProcessFunc(publisher, 2*time.Second, nil)
	results := make(chan badgerbox.BatchProcessResult, 1)
	missing := badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]{ID: 1, Destination: kafka.KafkaDestination{Topic: "missing-" + topic}}
	if err := fn(ctx, []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]{missing}, results); err != nil {
		t.Fatal("outer timeout masked the delivery result", err)
	}
	result := <-results
	if !errors.Is(result.Err, kgo.ErrRecordTimeout) || !errors.Is(result.Err, kerr.UnknownTopicOrPartition) || badgerbox.IsUnavailable(result.Err) {
		t.Fatalf("missing-topic cause lost through demo wrapper: %v", result.Err)
	}
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := badgerbox.New[kafka.KafkaMessage, kafka.KafkaDestination](db, badgerbox.Serde[kafka.KafkaMessage, kafka.KafkaDestination]{}, badgerbox.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, name := range []string{missing.Destination.Topic, topic} {
		if _, err := store.Enqueue(ctx, badgerbox.EnqueueRequest[kafka.KafkaMessage, kafka.KafkaDestination]{Payload: kafka.KafkaMessage{Value: []byte("healthy")}, Destination: kafka.KafkaDestination{Topic: name}}); err != nil {
			t.Fatal(err)
		}
	}
	var transitions atomic.Int64
	processor, err := badgerbox.NewBatchProcessor(store, fn, badgerbox.BatchProcessorOptions{ClaimBatchSize: 1, ProcessorOptions: badgerbox.ProcessorOptions{Concurrency: 1, MaxAttempts: 1, CircuitBreaker: &badgerbox.CircuitBreakerOptions{FailureThreshold: 1, OnStateChange: func(_, _ string) { transitions.Add(1) }}}})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- processor.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	for {
		q, err := store.QueueSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if q.ReadyDepth == 0 && q.ProcessingDepth == 0 {
			if q.DeadLetterDepth != 1 || transitions.Load() != 0 {
				t.Fatalf("queue=%+v circuit transitions=%d", q, transitions.Load())
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	consumer, err := NewKafkaClient(brokers, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	for {
		records, err := PollForRecords(ctx, consumer)
		if err != nil || ctx.Err() != nil {
			t.Fatalf("consume: %v %v", err, ctx.Err())
		}
		if len(records) > 0 {
			return
		}
	}
}
