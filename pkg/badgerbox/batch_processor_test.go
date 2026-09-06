package badgerbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBatchSettlementMixedAndIncompleteResults(t *testing.T) {
	for _, mode := range []string{"mixed", "error", "panic", "cancel", "missing", "duplicates"} {
		t.Run(mode, func(t *testing.T) {
			_, s, cleanup := openTestStore[string, string](t, mode, Serde[string, string]{})
			defer cleanup()
			for range 2 {
				if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "payload", Destination: "destination"}); err != nil {
					t.Fatal(err)
				}
			}
			lease := time.Second
			if mode == "missing" {
				lease = 40 * time.Millisecond
			}
			work, err := s.claimReadyBatch(t.Context(), time.Now(), 2, lease, 10)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			fn := func(ctx context.Context, m []Message[string, string], results chan<- BatchProcessResult) error {
				results <- BatchProcessResult{ID: m[0].ID}
				switch mode {
				case "mixed":
					results <- BatchProcessResult{ID: m[1].ID, Err: Permanent(errors.New("invalid"))}
				case "error":
					return Permanent(errors.New("whole batch failed"))
				case "panic":
					panic("callback failed")
				case "cancel":
					cancel()
				case "duplicates":
					results <- BatchProcessResult{ID: m[0].ID}
					results <- BatchProcessResult{ID: 999}
					return errors.New("missing second result")
				}
				return nil
			}
			p, err := NewBatchProcessor(s, fn, ProcessorOptions{BatchSettlementTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.processBatch(ctx, work); err != nil {
				t.Fatal(err)
			}
			q, err := s.QueueSnapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if q.ProcessingDepth != 0 {
				t.Fatalf("stranded claims: %+v", q)
			}
			if mode == "mixed" {
				if q.DeadLetterDepth != 1 || q.ReadyDepth != 0 {
					t.Fatalf("mixed: %+v", q)
				}
			} else if q.ReadyDepth != 1 || q.DeadLetterDepth != 0 {
				t.Fatalf("incomplete: %+v", q)
			}
		})
	}
}
func TestExpiredBatchDoesNotInvokeCallback(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "expired-batch", Serde[string, string]{})
	defer cleanup()
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
		t.Fatal(err)
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now().Add(time.Second), 1, time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	work[0].LeaseUntil = time.Now().Add(-time.Second)
	p, _ := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error {
		t.Error("expired callback invoked")
		return nil
	}, ProcessorOptions{})
	if err := p.processBatch(t.Context(), work); err != nil {
		t.Fatal(err)
	}
}
