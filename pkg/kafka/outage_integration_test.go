//go:build integration

package kafka_test

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	dockerclient "github.com/moby/moby/client"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
	"github.com/testcontainers/testcontainers-go"
	testcontainerskafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type outageHook struct{ failed atomic.Int64 }

func (h *outageHook) OnProduceRecordUnbuffered(_ *kgo.Record, err error) {
	if err != nil {
		h.failed.Add(1)
	}
}

// A stalled broker must never turn a failed produce into an outbox
// acknowledgement. Once resumed, every persisted payload must be consumable.
func TestBatchDeliverySurvivesBrokerOutage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	broker, err := testcontainerskafka.Run(ctx, "confluentinc/confluent-local:7.5.0", testcontainerskafka.WithClusterID("outage-cluster"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(broker); err != nil {
			t.Error(err)
		}
	})
	brokers, err := broker.Brokers(ctx)
	if err != nil {
		t.Fatal(err)
	}
	hook := &outageHook{}
	producer := mustKafkaClient(t, brokers, kgo.WithHooks(hook),
		kgo.RecordDeliveryTimeout(time.Second), kgo.ProduceRequestTimeout(time.Second),
		kgo.RequestTimeoutOverhead(time.Second), kgo.RetryBackoffFn(func(int) time.Duration { return 20 * time.Millisecond }))
	defer producer.Close()
	topic := fmt.Sprintf("outage-%d", time.Now().UnixNano())
	createTopic(t, producer, topic)
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if _, err := provider.Client().ContainerPause(ctx, broker.GetContainerID(), dockerclient.ContainerPauseOptions{}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = provider.Client().ContainerUnpause(cleanupCtx, broker.GetContainerID(), dockerclient.ContainerUnpauseOptions{})
	}()
	_, store, cleanup := openKafkaStore(t, "outage")
	defer cleanup()
	const count = 64
	payload := bytes.Repeat([]byte{0, 255, 127, 1}, 4096)
	ids := make([]badgerbox.MessageID, count)
	for i := range count {
		ids[i], err = store.Enqueue(ctx, badgerbox.EnqueueRequest[kafka.KafkaMessage, kafka.KafkaDestination]{
			Payload: kafka.KafkaMessage{Key: []byte(strconv.Itoa(i)), Value: payload}, Destination: kafka.KafkaDestination{Topic: topic},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	fn, err := kafka.NewBatchProducerFunc(producer)
	if err != nil {
		t.Fatal(err)
	}
	processor, err := badgerbox.NewBatchProcessor(store, fn, badgerbox.BatchProcessorOptions{ClaimBatchSize: 16, ProcessorOptions: badgerbox.ProcessorOptions{
		Concurrency: 2, PollInterval: 10 * time.Millisecond, LeaseDuration: 10 * time.Second,
		RetryBaseDelay: 50 * time.Millisecond, RetryMaxDelay: 100 * time.Millisecond, MaxAttempts: 100,
	}})
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
	deadline := time.Now().Add(20 * time.Second)
	for hook.failed.Load() == 0 {
		if ctx.Err() != nil || time.Now().After(deadline) {
			t.Fatal("broker outage produced no delivery failures")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range ids {
		msg, err := store.Get(ctx, id)
		if err != nil || !bytes.Equal(msg.Payload.Value, payload) {
			t.Fatalf("unconfirmed message %s was removed or corrupted: %v", id, err)
		}
	}
	if _, err := provider.Client().ContainerUnpause(ctx, broker.GetContainerID(), dockerclient.ContainerUnpauseOptions{}); err != nil {
		t.Fatal(err)
	}
	consumer := mustKafkaClient(t, brokers, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer consumer.Close()
	seen := make(map[int]bool, count)
	for len(seen) < count {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consumed %d/%d messages after broker resumed: %v", len(seen), count, ctx.Err())
		}
		if err := firstFatalFetchError(fetches.Errors()); err != nil {
			t.Fatal(err)
		}
		for _, record := range fetches.Records() {
			i, err := strconv.Atoi(string(record.Key))
			if err != nil || i < 0 || i >= count || !bytes.Equal(record.Value, payload) {
				t.Fatalf("unexpected or corrupt Kafka record: key=%q", record.Key)
			}
			seen[i] = true // At-least-once duplicates are allowed during reconnect.
		}
	}
	for {
		snapshot, err := store.QueueSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.ReadyDepth == 0 && snapshot.ProcessingDepth == 0 {
			if snapshot.DeadLetterDepth != 0 {
				t.Fatalf("outage exhausted retry allowance: %+v", snapshot)
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatal(ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	report, err := store.Audit(ctx, badgerbox.AuditOptions{})
	if err != nil || !report.Complete || report.LiveRows != 0 || report.DeadLetters.Rows != 0 || len(report.Samples.Anomalies) != 0 {
		t.Fatalf("post-outage audit=%+v err=%v", report, err)
	}
}
