package badgerbox

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

const benchmarkPayloadSize = 10 * 1024

type rawBytesCodec struct{}

func (rawBytesCodec) Marshal(value []byte) ([]byte, error) {
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned, nil
}

func (rawBytesCodec) Unmarshal(data []byte) ([]byte, error) {
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned, nil
}

type benchmarkDestination struct {
	Route string `json:"route"`
}

func BenchmarkStoreEnqueue10KB(b *testing.B) {
	payload := bytes.Repeat([]byte("p"), benchmarkPayloadSize)
	for _, concurrency := range benchmarkConcurrencyLevels() {
		b.Run(fmt.Sprintf("concurrency-%d", concurrency), func(b *testing.B) {
			db, err := badger.Open(badger.DefaultOptions(b.TempDir()).WithLogger(nil))
			if err != nil {
				b.Fatalf("open badger: %v", err)
			}
			defer db.Close()

			store, err := New[[]byte, benchmarkDestination](db, Serde[[]byte, benchmarkDestination]{
				Message: rawBytesCodec{},
			}, Options{Namespace: fmt.Sprintf("benchmark-producer-%d", concurrency)})
			if err != nil {
				b.Fatalf("new store: %v", err)
			}
			defer store.Close()

			req := EnqueueRequest[[]byte, benchmarkDestination]{
				Payload:     payload,
				Destination: benchmarkDestination{Route: "benchmark"},
			}

			workers := concurrency
			if workers > b.N && b.N > 0 {
				workers = b.N
			}
			if workers < 1 {
				workers = 1
			}

			var next atomic.Int64
			start := make(chan struct{})
			var wg sync.WaitGroup
			errCh := make(chan error, workers)
			ctx := context.Background()

			for worker := 0; worker < workers; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for {
						index := int(next.Add(1) - 1)
						if index >= b.N {
							return
						}
						if _, err := store.Enqueue(ctx, req); err != nil {
							select {
							case errCh <- fmt.Errorf("enqueue %d: %w", index, err):
							default:
							}
							return
						}
					}
				}()
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			close(start)
			wg.Wait()
			b.StopTimer()
			select {
			case err := <-errCh:
				b.Fatal(err)
			default:
			}
		})
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
