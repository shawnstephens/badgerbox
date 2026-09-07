package demo

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type reloadBatchClient struct {
	deliver func(context.Context, []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], chan<- badgerbox.BatchProcessResult) error
	close   func() error
}

func (c *reloadBatchClient) Deliver(ctx context.Context, m []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], r chan<- badgerbox.BatchProcessResult) error {
	return c.deliver(ctx, m, r)
}
func (*reloadBatchClient) ProduceSync(context.Context, *kgo.Record) error { return nil }
func (*reloadBatchClient) Flush(context.Context) error                    { return nil }
func (c *reloadBatchClient) Close() error {
	if c.close != nil {
		return c.close()
	}
	return nil
}

func TestBatchReloadOnEveryFailureExit(t *testing.T) {
	failure := errors.New("publish failed")
	reloadFailure := errors.New("state unreadable")
	for _, mode := range []string{"timeout", "immediate", "callback", "partial timeout", "shutdown", "unchanged", "reload failure", "success"} {
		t.Run(mode, func(t *testing.T) {
			parent, stop := context.WithCancel(t.Context())
			defer stop()
			var logs bytes.Buffer
			publisher := NewReloadingPublisher("state.json", []string{"old:9092"}, "topic", nil)
			var calls, reads, closes atomic.Int32
			var callbacks sync.WaitGroup
			var publishCtx context.Context
			old := &reloadBatchClient{}
			old.deliver = func(ctx context.Context, m []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], out chan<- badgerbox.BatchProcessResult) error {
				publishCtx = ctx
				if mode == "immediate" {
					return failure
				}
				if mode == "callback" || mode == "unchanged" || mode == "reload failure" {
					out <- badgerbox.BatchProcessResult{ID: m[0].ID, Err: failure}
					out <- badgerbox.BatchProcessResult{ID: m[1].ID}
					return nil
				}
				if mode == "success" {
					for _, msg := range m {
						out <- badgerbox.BatchProcessResult{ID: msg.ID}
					}
					return nil
				}
				first := 0
				if mode == "partial timeout" {
					out <- badgerbox.BatchProcessResult{ID: m[0].ID}
					first = 1
				}
				callbacks.Go(func() {
					<-ctx.Done()
					for _, msg := range m[first:] {
						out <- badgerbox.BatchProcessResult{ID: msg.ID, Err: ctx.Err()}
					}
				})
				if mode == "shutdown" {
					stop()
				}
				return nil
			}
			old.close = func() error {
				closes.Add(1)
				if publishCtx.Err() == nil {
					t.Error("client replaced before cancellation")
				}
				callbacks.Wait()
				return nil
			}
			publisher.client = old
			publisher.readState = func(string) (State, error) {
				reads.Add(1)
				if mode == "reload failure" {
					return State{}, reloadFailure
				}
				broker := "new:9092"
				if mode == "unchanged" {
					broker = "old:9092"
				}
				return State{Brokers: []string{broker}, Topic: "topic"}, nil
			}
			next := &reloadBatchClient{deliver: func(_ context.Context, m []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], out chan<- badgerbox.BatchProcessResult) error {
				for _, msg := range m {
					out <- badgerbox.BatchProcessResult{ID: msg.ID}
				}
				return nil
			}}
			publisher.newClient = func(b []string) (producerClient, error) {
				calls.Add(1)
				if len(b) != 1 || b[0] != "new:9092" {
					t.Errorf("brokers=%v", b)
				}
				return next, nil
			}
			fn := NewBatchProcessFunc(publisher, 50*time.Millisecond, NewLogger(&logs, "never"))
			messages := []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]{{ID: 1}, {ID: 2}}
			results := make(chan badgerbox.BatchProcessResult, 2)
			done := make(chan error, 1)
			go func() { done <- fn(parent, messages, results) }()
			if mode == "partial timeout" {
				select {
				case result := <-results:
					if result.ID != 1 || result.Err != nil {
						t.Fatal(result)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("partial success not forwarded")
				}
			}
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("batch did not return")
			}
			callbacks.Wait()
			wantErr := error(nil)
			if mode == "timeout" || mode == "partial timeout" {
				wantErr = context.DeadlineExceeded
			}
			if mode == "shutdown" {
				wantErr = context.Canceled
			}
			if mode == "immediate" {
				wantErr = failure
			}
			if !errors.Is(err, wantErr) {
				t.Fatalf("err=%v want=%v", err, wantErr)
			}
			wantReads := int32(1)
			if mode == "shutdown" || mode == "success" {
				wantReads = 0
			}
			wantNew := wantReads
			if mode == "unchanged" || mode == "reload failure" {
				wantNew = 0
			}
			if reads.Load() != wantReads || calls.Load() != wantNew || closes.Load() != wantNew {
				t.Fatalf("reads=%d new=%d close=%d", reads.Load(), calls.Load(), closes.Load())
			}
			if mode == "reload failure" && !strings.Contains(logs.String(), reloadFailure.Error()) {
				t.Fatal("reload failure not logged")
			}
			if mode == "callback" {
				if len(results) != 2 {
					t.Fatal("callback results lost")
				}
				first := <-results
				if !errors.Is(first.Err, failure) {
					t.Fatal(first)
				}
			}
			if wantNew == 1 {
				second := make(chan badgerbox.BatchProcessResult, 2)
				if err := fn(parent, messages, second); err != nil || len(second) != 2 {
					t.Fatalf("next batch failed: %v", err)
				}
				if reads.Load() != 1 {
					t.Fatal("successful batch reloaded")
				}
			}
		})
	}
}

func TestBatchForwardingHonorsCancellation(t *testing.T) {
	client := &reloadBatchClient{deliver: func(_ context.Context, m []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], out chan<- badgerbox.BatchProcessResult) error {
		out <- badgerbox.BatchProcessResult{ID: m[0].ID}
		return nil
	}}
	fn := NewBatchProcessFunc(client, 20*time.Millisecond, nil)
	err := fn(t.Context(), []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]{{ID: 1}}, make(chan badgerbox.BatchProcessResult))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}
