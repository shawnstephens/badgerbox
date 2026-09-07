package demo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
)

func TestTimeoutPreservesBufferedOutcomesBeforeDeliveryReturns(t *testing.T) {
	client := &reloadBatchClient{deliver: func(ctx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], out chan<- badgerbox.BatchProcessResult) error {
		out <- badgerbox.BatchProcessResult{ID: messages[0].ID}
		// Model asynchronous production whose scheduling call returns after the
		// last record exhausts the publish deadline.
		<-ctx.Done()
		out <- badgerbox.BatchProcessResult{ID: messages[1].ID, Err: ctx.Err()}
		return nil
	}}
	fn := NewBatchProcessFunc(client, 10*time.Millisecond, nil)
	results := make(chan badgerbox.BatchProcessResult, 2)
	err := fn(t.Context(), []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]{{ID: 1}, {ID: 2}}, results)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error=%v", err)
	}
	if len(results) != 2 {
		t.Fatalf("buffered outcomes discarded at deadline: got %d results, want 2", len(results))
	}
	if first := <-results; first.ID != 1 || first.Err != nil {
		t.Fatal(first)
	}
	if second := <-results; second.ID != 2 || !errors.Is(second.Err, context.DeadlineExceeded) {
		t.Fatal(second)
	}
}

type batchLogWriter func([]byte) (int, error)

func (w batchLogWriter) Write(p []byte) (int, error) { return w(p) }

func TestCancellationDrainsOutcomesBufferedDuringForwarding(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		name := "publish deadline"
		if shutdown {
			name = "parent cancellation"
		}
		t.Run(name, func(t *testing.T) {
			parent, stop := context.WithCancel(t.Context())
			defer stop()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			logger := NewLogger(batchLogWriter(func(p []byte) (int, error) {
				once.Do(func() { close(entered); <-release })
				return len(p), nil
			}), "never")
			var incoming chan<- badgerbox.BatchProcessResult
			var publishCtx context.Context
			client := &reloadBatchClient{deliver: func(ctx context.Context, m []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], out chan<- badgerbox.BatchProcessResult) error {
				publishCtx, incoming = ctx, out
				out <- badgerbox.BatchProcessResult{ID: m[0].ID}
				return nil
			}}
			publisher := NewReloadingPublisher("state", []string{"old:9092"}, "topic", nil)
			publisher.client = client
			reads := 0
			publisher.readState = func(string) (State, error) { reads++; return State{Brokers: []string{"old:9092"}, Topic: "topic"}, nil }
			results := make(chan badgerbox.BatchProcessResult, 2)
			done := make(chan error, 1)
			fn := NewBatchProcessFunc(publisher, 200*time.Millisecond, logger)
			go func() {
				done <- fn(parent, []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]{{ID: 1}, {ID: 2}}, results)
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("forwarding did not start")
			}
			incoming <- badgerbox.BatchProcessResult{ID: 2}
			wantErr, wantReads := error(context.DeadlineExceeded), 1
			if shutdown {
				stop()
				wantErr, wantReads = context.Canceled, 0
			}
			<-publishCtx.Done()
			close(release)
			select {
			case err := <-done:
				if !errors.Is(err, wantErr) {
					t.Fatalf("err=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("canceled delivery did not finish")
			}
			if reads != wantReads || len(results) != 2 {
				t.Fatalf("reloads=%d results=%d", reads, len(results))
			}
			for id := badgerbox.MessageID(1); id <= 2; id++ {
				if result := <-results; result.ID != id || result.Err != nil {
					t.Fatal(result)
				}
			}
		})
	}
}
