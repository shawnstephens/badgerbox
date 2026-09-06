package badgerbox

import (
	"context"
	"testing"
	"time"
)

func TestBatchClaimsBoundedByWorkers(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "admission", Serde[string, string]{})
	defer cleanup()
	for range 8 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{}, 2)
	p, _ := NewBatchProcessor(s, func(ctx context.Context, _ []Message[string, string], _ chan<- BatchProcessResult) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}, ProcessorOptions{Concurrency: 2, ClaimBatchSize: 2, LeaseDuration: time.Minute})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	q, err := s.QueueSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if q.ProcessingDepth != 4 || q.ReadyDepth != 4 {
		t.Fatalf("overclaimed: %+v", q)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("workers did not stop")
	}
	q, err = s.QueueSnapshot(t.Context())
	if err != nil || q.ReadyDepth != 8 || q.ProcessingDepth != 0 {
		t.Fatalf("shutdown=%+v err=%v", q, err)
	}
}
func TestReleaseUndispatchedClaimPreservesAttemptBudget(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "release", Serde[string, string]{})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p, _ := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, ProcessorOptions{})
	if err := p.releaseClaimedBatch(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	msg, err := s.Get(t.Context(), id)
	if err != nil || msg.Attempt != 0 || msg.State != MessageStateReady {
		t.Fatalf("released=%+v err=%v", msg, err)
	}
}
