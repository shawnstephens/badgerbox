package badgerbox

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// Measures draining a fixed, pre-enqueued backlog through the real dispatcher,
// workers, and settlement path. Latencies are from Run start to callback entry,
// including queue residence, not network latency or enqueue latency.
func BenchmarkCircuitHealthyDrain(b *testing.B) {
	for _, concurrency := range []int{1, 4} {
		for _, enabled := range []bool{false, true} {
			b.Run(fmt.Sprintf("workers=%d/enabled=%t", concurrency, enabled), func(b *testing.B) {
				db, err := badger.Open(badger.DefaultOptions(b.TempDir()).WithLogger(nil))
				if err != nil {
					b.Fatal(err)
				}
				defer db.Close()
				s, err := New[string, string](db, Serde[string, string]{}, Options{})
				if err != nil {
					b.Fatal(err)
				}
				defer s.Close()
				for range b.N {
					if _, err := s.Enqueue(b.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", 1024)}); err != nil {
						b.Fatal(err)
					}
				}
				var opts *CircuitBreakerOptions
				if enabled {
					opts = &CircuitBreakerOptions{}
				}
				latencies := make([]time.Duration, 0, b.N)
				var mu sync.Mutex
				var started time.Time
				p, err := NewBatchProcessor(s, func(_ context.Context, msgs []Message[string, string], results chan<- BatchProcessResult) error {
					elapsed := time.Since(started)
					mu.Lock()
					for range msgs {
						latencies = append(latencies, elapsed)
					}
					mu.Unlock()
					for _, msg := range msgs {
						results <- BatchProcessResult{ID: msg.ID}
					}
					return nil
				}, BatchProcessorOptions{ClaimBatchSize: 16, ProcessorOptions: ProcessorOptions{Concurrency: concurrency, CircuitBreaker: opts, PollInterval: time.Millisecond, LeaseDuration: time.Minute}})
				if err != nil {
					b.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(b.Context(), time.Minute)
				done := make(chan error, 1)
				defer func() {
					cancel()
					if err := <-done; err != nil {
						b.Error(err)
					}
				}()
				b.ReportAllocs()
				b.ResetTimer()
				started = time.Now()
				go func() { done <- p.Run(ctx) }()
				for {
					q, err := s.QueueSnapshot(ctx)
					if err != nil {
						b.Fatal(err)
					}
					if q.ReadyDepth+q.ProcessingDepth == 0 {
						if q.DeadLetterDepth != 0 {
							b.Fatal("healthy message entered DLQ")
						}
						break
					}
					select {
					case <-ctx.Done():
						b.Fatal(ctx.Err())
					case <-time.After(time.Millisecond):
					}
				}
				elapsed := time.Since(started)
				b.StopTimer()
				mu.Lock()
				defer mu.Unlock()
				if len(latencies) != b.N {
					b.Fatalf("delivered %d/%d", len(latencies), b.N)
				}
				slices.Sort(latencies)
				b.ReportMetric(float64(b.N)/elapsed.Seconds(), "messages/s")
				b.ReportMetric(float64(latencies[(len(latencies)-1)/2])/float64(time.Millisecond), "p50-drain-ms")
				b.ReportMetric(float64(latencies[(len(latencies)-1)*99/100])/float64(time.Millisecond), "p99-drain-ms")
			})
		}
	}
}
