//go:build integration

package kafkaoutbox_test

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"
	"github.com/twmb/franz-go/pkg/kgo"
)

const benchmarkKafkaPayloadSize = 10 * 1024

func BenchmarkProcessorToKafka10KB(b *testing.B) {
	payload := bytes.Repeat([]byte("k"), benchmarkKafkaPayloadSize)
	ctx := context.Background()
	brokers := startKafka(b, ctx)

	for _, concurrency := range benchmarkConcurrencyLevels() {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			topic := fmt.Sprintf("benchmark-topic-%d-%d", concurrency, time.Now().UnixNano())

			producer := mustKafkaClient(b, brokers)
			defer producer.Close()
			createTopic(b, producer, topic)

			consumer := mustKafkaClient(b, brokers,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			defer consumer.Close()

			_, store, cleanup := openKafkaStore(b, fmt.Sprintf("benchmark-processor-%d", concurrency))
			defer cleanup()

			for i := 0; i < b.N; i++ {
				if _, err := store.Enqueue(context.Background(), badgerbox.EnqueueRequest[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
					Payload: kafkaoutbox.KafkaMessage{
						Key:   []byte(fmt.Sprintf("key-%d", i)),
						Value: payload,
					},
					Destination: kafkaoutbox.KafkaDestination{Topic: topic},
				}); err != nil {
					b.Fatalf("enqueue %d: %v", i, err)
				}
			}

			baseFn := kafkaoutbox.NewProcessFunc(producer, kafkaoutbox.Options{})
			var processed atomic.Int64
			processFn := func(ctx context.Context, msg badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]) error {
				err := baseFn(ctx, msg)
				if err == nil {
					processed.Add(1)
				}
				return err
			}

			claimBatchSize := b.N
			if claimBatchSize < 1 {
				claimBatchSize = 1
			}
			maxBatch := concurrency * 4
			if maxBatch < 1 {
				maxBatch = 1
			}
			if claimBatchSize > maxBatch {
				claimBatchSize = maxBatch
			}
			if claimBatchSize > 256 {
				claimBatchSize = 256
			}

			processor, err := badgerbox.NewProcessor(store, processFn, badgerbox.ProcessorOptions{
				Concurrency:    concurrency,
				ClaimBatchSize: claimBatchSize,
				PollInterval:   time.Millisecond,
				LeaseDuration:  30 * time.Second,
			})
			if err != nil {
				b.Fatalf("new processor: %v", err)
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()

			cancel, done := runProcessor(processor)
			waitForProcessedCount(b, &processed, int64(b.N), 60*time.Second)
			b.StopTimer()

			consumeKafkaRecords(b, consumer, b.N, len(payload))
			stopProcessor(b, cancel, done)
		})
	}
}

func waitForProcessedCount(b *testing.B, counter *atomic.Int64, want int64, timeout time.Duration) {
	b.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if counter.Load() >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}

	b.Fatalf("timed out waiting for %d processed messages; got %d", want, counter.Load())
}

func consumeKafkaRecords(b *testing.B, client *kgo.Client, want int, payloadSize int) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var consumed int
	for consumed < want {
		fetches := client.PollFetches(ctx)
		if err := firstFatalFetchError(fetches.Errors()); err != nil {
			b.Fatalf("poll kafka fetches: %v", err)
		}

		iter := fetches.RecordIter()
		for !iter.Done() && consumed < want {
			record := iter.Next()
			if len(record.Value) != payloadSize {
				b.Fatalf("unexpected kafka payload size: got=%d want=%d", len(record.Value), payloadSize)
			}
			consumed++
		}

		if ctx.Err() != nil {
			b.Fatalf("timed out waiting for %d kafka records; got %d", want, consumed)
		}
	}
}

func benchmarkConcurrencyLevels() []int {
	cores := runtime.NumCPU()
	if cores < 1 {
		return []int{1}
	}

	levels := []int{1}
	for concurrency := 2; concurrency <= cores; concurrency *= 2 {
		levels = append(levels, concurrency)
	}
	if levels[len(levels)-1] != cores {
		levels = append(levels, cores)
	}
	return levels
}
