package badgerbox

import (
	"context"
	"sync"
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
	}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{Concurrency: 2, LeaseDuration: time.Minute}, ClaimBatchSize: 2})
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
	p, _ := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{}})
	if err := p.releaseClaimedBatch(ctx, claimed); err != nil {
		t.Fatal(err)
	}
	msg, err := s.Get(t.Context(), id)
	if err != nil || msg.Attempt != 0 || msg.State != MessageStateReady {
		t.Fatalf("released=%+v err=%v", msg, err)
	}
}

func TestRunJoinsCallbackBeforeReturning(t *testing.T) {
	_, s, closeStore := openTestStore[string, string](t, "join-callback", Serde[string, string]{})
	defer closeStore()
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	p, _ := NewBatchProcessor(s, func(ctx context.Context, _ []Message[string, string], _ chan<- BatchProcessResult) error {
		close(started)
		<-release
		return ctx.Err()
	}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{}, ClaimBatchSize: 1})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	<-started
	cancel()
	select {
	case <-done:
		t.Fatal("Run returned before callback completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLeaseExpiryRetainsCallbackConcurrencySlot(t *testing.T) {
	_, s, closeStore := openTestStore[string, string](t, "expiry-concurrency", Serde[string, string]{})
	defer closeStore()
	for range 2 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{}, 2)
	canceled := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	p, err := NewBatchProcessor(s, func(ctx context.Context, _ []Message[string, string], _ chan<- BatchProcessResult) error {
		started <- struct{}{}
		<-ctx.Done()
		canceled <- struct{}{}
		// Model cancellation cleanup that outlives the original lease.
		<-release
		return ctx.Err()
	}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{
		Concurrency: 1, PollInterval: time.Millisecond, LeaseDuration: 50 * time.Millisecond,
		RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour,
	}, ClaimBatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(release) })
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("processor did not join callbacks")
		}
	}()
	waitForChannel(t, started)
	waitForChannel(t, canceled)
	// Settlement still completes promptly even though the callback is cleaning
	// up. The second message must remain unclaimed while the slot is occupied.
	waitFor(t, func() bool {
		q, err := s.QueueSnapshot(t.Context())
		return err == nil && q.ReadyDepth == 2 && q.ProcessingDepth == 0
	})
	select {
	case <-started:
		t.Fatal("lease expiry started another callback before the first returned")
	case <-time.After(150 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	waitForChannel(t, started)
}
