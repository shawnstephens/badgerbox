//go:build integration

package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMissingTopicTimeoutRetainsRetryBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	brokers := startKafka(t, ctx)
	producer := mustKafkaClient(t, brokers,
		kgo.UnknownTopicRetries(-1), kgo.RecordDeliveryTimeout(2*time.Second),
		kgo.MetadataMinAge(10*time.Millisecond), kgo.MetadataMaxAge(100*time.Millisecond))
	defer producer.Close()
	topic := fmt.Sprintf("healthy-%d", time.Now().UnixNano())
	createTopic(t, producer, topic)
	missingTopic := "missing-" + topic
	// Auto-topic creation is disabled by default in franz-go. Unlimited
	// unknown-topic retries make the record timeout win over the retry count.
	err := producer.ProduceSync(ctx, &kgo.Record{Topic: missingTopic, Value: []byte("probe")}).FirstErr()
	if !errors.Is(err, kgo.ErrRecordTimeout) || !errors.Is(err, kerr.UnknownTopicOrPartition) {
		t.Fatalf("expected a real timeout wrapping missing-topic metadata: %v", err)
	}
	if badgerbox.IsUnavailable(kafka.ClassifyProducerError(err)) {
		t.Fatal("missing topic classified as outage")
	}
	_, store, cleanup := openKafkaStore(t, "missing-topic")
	defer cleanup()
	for _, destination := range []string{missingTopic, topic} {
		if _, err := store.Enqueue(ctx, badgerbox.EnqueueRequest[kafka.KafkaMessage, kafka.KafkaDestination]{
			Payload: kafka.KafkaMessage{Value: []byte("retained")}, Destination: kafka.KafkaDestination{Topic: destination},
		}); err != nil {
			t.Fatal(err)
		}
	}
	fn, err := kafka.NewProcessFunc(producer, kafka.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var transitions atomic.Int64
	processor, err := badgerbox.NewProcessor(store, fn, badgerbox.ProcessorOptions{
		Concurrency: 1, MaxAttempts: 1, LeaseDuration: 15 * time.Second, PollInterval: 10 * time.Millisecond,
		CircuitBreaker: &badgerbox.CircuitBreakerOptions{FailureThreshold: 1, OnStateChange: func(_, _ string) { transitions.Add(1) }},
	})
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
		snapshot, err := store.QueueSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.ReadyDepth == 0 && snapshot.ProcessingDepth == 0 {
			if snapshot.DeadLetterDepth != 1 || transitions.Load() != 0 {
				t.Fatalf("missing topic bypassed retry budget or paused queue: %+v transitions=%d", snapshot, transitions.Load())
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("queue did not settle", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	consumer := mustKafkaClient(t, brokers, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer consumer.Close()
	for {
		fetches := consumer.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatal("healthy message was not delivered", err)
		}
		if err := firstFatalFetchError(fetches.Errors()); err != nil {
			t.Fatal(err)
		}
		if len(fetches.Records()) > 0 {
			return
		}
	}
}
