//go:build integration

package kafkaoutbox_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"

	"github.com/dgraph-io/badger/v4"
	"github.com/testcontainers/testcontainers-go"
	testcontainerskafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestKafkaProcessFuncProducesRecord(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	topic := fmt.Sprintf("topic-%d", time.Now().UnixNano())

	producer := mustKafkaClient(t, brokers)
	defer producer.Close()

	createTopic(t, producer, topic)

	consumer := mustKafkaClient(t, brokers,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	_, store, cleanup := openKafkaStore(t, "produce")
	defer cleanup()

	processFn := kafkaoutbox.NewProcessFunc(producer, kafkaoutbox.Options{})
	processor, err := badgerbox.NewProcessor(store, processFn, badgerbox.ProcessorOptions{
		PollInterval:   10 * time.Millisecond,
		LeaseDuration:  2 * time.Second,
		RetryBaseDelay: 20 * time.Millisecond,
		RetryMaxDelay:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	if _, err := store.Enqueue(context.Background(), badgerbox.EnqueueRequest[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		Payload: kafkaoutbox.KafkaMessage{
			Key:   []byte("order-1"),
			Value: []byte("created"),
			Headers: map[string][]byte{
				"type": []byte("order.created"),
			},
		},
		Destination: kafkaoutbox.KafkaDestination{Topic: topic},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	record := waitForKafkaRecord(t, consumer)
	if string(record.Key) != "order-1" {
		t.Fatalf("unexpected record key: %q", record.Key)
	}
	if string(record.Value) != "created" {
		t.Fatalf("unexpected record value: %q", record.Value)
	}
	if got := headerMap(record.Headers)["type"]; string(got) != "order.created" {
		t.Fatalf("unexpected record headers: %#v", record.Headers)
	}
}

func TestKafkaProcessFuncRetriesThenProducesRecord(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	topic := fmt.Sprintf("retry-topic-%d", time.Now().UnixNano())

	producer := mustKafkaClient(t, brokers)
	defer producer.Close()

	createTopic(t, producer, topic)

	consumer := mustKafkaClient(t, brokers,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	_, store, cleanup := openKafkaStore(t, "retry")
	defer cleanup()

	baseFn := kafkaoutbox.NewProcessFunc(producer, kafkaoutbox.Options{})
	var attempts atomic.Int32
	processFn := func(ctx context.Context, msg badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]) error {
		if attempts.Add(1) == 1 {
			return errors.New("synthetic first failure")
		}
		return baseFn(ctx, msg)
	}

	processor, err := badgerbox.NewProcessor(store, processFn, badgerbox.ProcessorOptions{
		PollInterval:   10 * time.Millisecond,
		LeaseDuration:  2 * time.Second,
		RetryBaseDelay: 20 * time.Millisecond,
		RetryMaxDelay:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	if _, err := store.Enqueue(context.Background(), badgerbox.EnqueueRequest[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		Payload: kafkaoutbox.KafkaMessage{
			Key:   []byte("order-2"),
			Value: []byte("retry-success"),
		},
		Destination: kafkaoutbox.KafkaDestination{Topic: topic},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	record := waitForKafkaRecord(t, consumer)
	if string(record.Value) != "retry-success" {
		t.Fatalf("unexpected record value: %q", record.Value)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
}

func TestKafkaOutboxDemoConcurrentFlow(t *testing.T) {
	ctx := context.Background()
	brokers := startKafka(t, ctx)
	topic := fmt.Sprintf("demo-topic-%d", time.Now().UnixNano())
	const (
		messageCount         = 10
		processorConcurrency = 4
	)

	producer := mustKafkaClient(t, brokers)
	defer producer.Close()
	createTopic(t, producer, topic)

	consumer := mustKafkaClient(t, brokers,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	defer consumer.Close()

	_, store, cleanup := openKafkaStore(t, "demo")
	defer cleanup()

	baseFn := kafkaoutbox.NewProcessFunc(producer, kafkaoutbox.Options{})
	var (
		activeWorkers atomic.Int32
		maxActive     atomic.Int32
		workerCounter atomic.Uint64
	)
	processFn := func(ctx context.Context, msg badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]) error {
		workerID := workerCounter.Add(1)
		key := string(msg.Payload.Key)
		startedAt := time.Now().UTC()

		current := activeWorkers.Add(1)
		updateMaxActive(&maxActive, current)
		t.Logf("phase=process-start worker=%d msg_id=%d key=%s topic=%s attempt=%d active=%d at=%s", workerID, msg.ID, key, msg.Destination.Topic, msg.Attempt, current, startedAt.Format(time.RFC3339Nano))

		time.Sleep(20 * time.Millisecond)

		err := baseFn(ctx, msg)
		finishedAt := time.Now().UTC()
		remaining := activeWorkers.Add(-1)
		if err != nil {
			t.Logf("phase=process-error worker=%d msg_id=%d key=%s topic=%s err=%q active=%d at=%s", workerID, msg.ID, key, msg.Destination.Topic, err, remaining, finishedAt.Format(time.RFC3339Nano))
			return err
		}

		t.Logf("phase=process-done worker=%d msg_id=%d key=%s topic=%s active=%d at=%s", workerID, msg.ID, key, msg.Destination.Topic, remaining, finishedAt.Format(time.RFC3339Nano))
		return nil
	}

	processor, err := badgerbox.NewProcessor(store, processFn, badgerbox.ProcessorOptions{
		Concurrency:    processorConcurrency,
		ClaimBatchSize: messageCount,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  5 * time.Second,
		RetryBaseDelay: 20 * time.Millisecond,
		RetryMaxDelay:  20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	expected := make(map[string]string, messageCount)
	for i := 0; i < messageCount; i++ {
		key := fmt.Sprintf("demo-%02d", i)
		value := fmt.Sprintf("payload-%02d", i)
		expected[key] = value

		t.Logf("phase=enqueue key=%s topic=%s index=%d", key, topic, i)
		if _, err := store.Enqueue(ctx, badgerbox.EnqueueRequest[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
			Payload: kafkaoutbox.KafkaMessage{
				Key:   []byte(key),
				Value: []byte(value),
				Headers: map[string][]byte{
					"demo-index": []byte(fmt.Sprintf("%d", i)),
				},
			},
			Destination: kafkaoutbox.KafkaDestination{Topic: topic},
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	consumed := consumeKafkaRecordsWithLogs(t, consumer, messageCount)
	if len(consumed) != len(expected) {
		t.Fatalf("unexpected consumed count: got=%d want=%d", len(consumed), len(expected))
	}

	var observedKeys []string
	for key, value := range expected {
		got, ok := consumed[key]
		if !ok {
			t.Fatalf("missing consumed key %q", key)
		}
		if got != value {
			t.Fatalf("unexpected value for key %q: got=%q want=%q", key, got, value)
		}
		observedKeys = append(observedKeys, key)
	}

	slices.Sort(observedKeys)
	t.Logf("phase=summary topic=%s produced=%d consumed=%d keys=%v max_active=%d", topic, len(expected), len(consumed), observedKeys, maxActive.Load())

	if maxActive.Load() < 2 {
		t.Fatalf("expected concurrent processing, max active workers=%d", maxActive.Load())
	}
}

func startKafka(tb testing.TB, ctx context.Context) []string {
	tb.Helper()

	defer func() {
		if recovered := recover(); recovered != nil {
			tb.Skipf("skipping Kafka integration test: %v", recovered)
		}
	}()

	container, err := testcontainerskafka.Run(ctx,
		"confluentinc/confluent-local:7.5.0",
		testcontainerskafka.WithClusterID("test-cluster"),
	)
	if err != nil {
		tb.Skipf("skipping Kafka integration test: %v", err)
	}
	tb.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			tb.Fatalf("terminate kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	if err != nil {
		tb.Fatalf("fetch kafka brokers: %v", err)
	}
	return brokers
}

func mustKafkaClient(tb testing.TB, brokers []string, opts ...kgo.Opt) *kgo.Client {
	tb.Helper()

	allOpts := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)
	client, err := kgo.NewClient(allOpts...)
	if err != nil {
		tb.Fatalf("new kafka client: %v", err)
	}
	return client
}

func openKafkaStore(tb testing.TB, namespace string) (*badger.DB, *badgerbox.Store[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination], func()) {
	tb.Helper()

	db, err := badger.Open(badger.DefaultOptions(tb.TempDir()).WithLogger(nil))
	if err != nil {
		tb.Fatalf("open badger: %v", err)
	}

	store, err := badgerbox.New[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination](
		db,
		badgerbox.Serde[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{},
		badgerbox.Options{Namespace: namespace},
	)
	if err != nil {
		db.Close()
		tb.Fatalf("new store: %v", err)
	}

	cleanup := func() {
		if err := store.Close(); err != nil {
			tb.Fatalf("close store: %v", err)
		}
		if err := db.Close(); err != nil {
			tb.Fatalf("close db: %v", err)
		}
	}
	return db, store, cleanup
}

func runProcessor(processor *badgerbox.Processor[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processor.Run(ctx)
	}()
	return cancel, done
}

func stopProcessor(tb testing.TB, cancel context.CancelFunc, done <-chan error) {
	tb.Helper()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			tb.Fatalf("processor stopped with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		tb.Fatal("timed out waiting for processor shutdown")
	}
}

func waitForKafkaRecord(tb testing.TB, client *kgo.Client) *kgo.Record {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		fetches := client.PollFetches(ctx)
		if err := firstFatalFetchError(fetches.Errors()); err != nil {
			tb.Fatalf("poll kafka fetches: %v", err)
		}

		iter := fetches.RecordIter()
		if !iter.Done() {
			return iter.Next()
		}

		if ctx.Err() != nil {
			tb.Fatalf("timed out waiting for kafka record: %v", ctx.Err())
		}
	}
}

func consumeKafkaRecordsWithLogs(tb testing.TB, client *kgo.Client, want int) map[string]string {
	tb.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	consumed := make(map[string]string, want)
	for len(consumed) < want {
		fetches := client.PollFetches(ctx)
		if err := firstFatalFetchError(fetches.Errors()); err != nil {
			tb.Fatalf("poll kafka fetches: %v", err)
		}

		iter := fetches.RecordIter()
		for !iter.Done() && len(consumed) < want {
			record := iter.Next()
			key := string(record.Key)
			value := string(record.Value)
			tb.Logf("phase=consume key=%s topic=%s partition=%d offset=%d value=%s", key, record.Topic, record.Partition, record.Offset, value)
			consumed[key] = value
		}

		if ctx.Err() != nil {
			tb.Fatalf("timed out waiting for %d kafka records; got %d", want, len(consumed))
		}
	}

	return consumed
}

func createTopic(tb testing.TB, client *kgo.Client, topic string) {
	tb.Helper()

	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topic
	reqTopic.NumPartitions = 1
	reqTopic.ReplicationFactor = 1
	req.Topics = append(req.Topics, reqTopic)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		tb.Fatalf("create topic %q: %v", topic, err)
	}
	if len(resp.Topics) != 1 {
		tb.Fatalf("unexpected create topic response size: %d", len(resp.Topics))
	}

	if err := kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		tb.Fatalf("create topic %q response error: %v", topic, err)
	}
}

func firstFatalFetchError(errs []kgo.FetchError) error {
	for _, fetchErr := range errs {
		if errors.Is(fetchErr.Err, kerr.UnknownTopicOrPartition) {
			continue
		}
		if errors.Is(fetchErr.Err, kerr.LeaderNotAvailable) || errors.Is(fetchErr.Err, kerr.NotLeaderForPartition) {
			continue
		}
		return fetchErr.Err
	}
	return nil
}

func headerMap(headers []kgo.RecordHeader) map[string][]byte {
	result := make(map[string][]byte, len(headers))
	for _, header := range headers {
		result[header.Key] = header.Value
	}
	return result
}

func updateMaxActive(maxActive *atomic.Int32, current int32) {
	for {
		existing := maxActive.Load()
		if current <= existing {
			return
		}
		if maxActive.CompareAndSwap(existing, current) {
			return
		}
	}
}
