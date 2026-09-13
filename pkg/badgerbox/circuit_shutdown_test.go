package badgerbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCircuitShutdownSettlesKnownOutcomes(t *testing.T) {
	for _, halfOpen := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			err       error
			custom    bool
			wantDefer bool
		}{
			{name: "outage", err: Unavailable(errors.New("offline")), wantDefer: true},
			{name: "publish_deadline", err: Unavailable(context.DeadlineExceeded), wantDefer: true},
			{name: "custom_outage", err: errors.New("offline"), custom: true, wantDefer: true},
			{name: "cancellation", err: context.Canceled, custom: true},
			{name: "wrapped_cancellation", err: Unavailable(context.Canceled), custom: true},
			{name: "run_deadline", err: context.DeadlineExceeded, custom: true},
			{name: "permanent", err: Permanent(Unavailable(errors.New("bad message")))},
		} {
			stateName := "closed"
			if halfOpen {
				stateName = "half_open"
			}
			t.Run(stateName+"/"+tc.name, func(t *testing.T) {
				b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
				defer b.close()
				if tc.custom {
					b.opts.IsUnavailable = func(error) bool { return true }
				}
				if halfOpen {
					permit, _ := b.admit()
					b.report(permit, false, true)
					if err := b.wait(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
				_, s, cleanup := openTestStoreWithOptions[string, string](t, "shutdown-outage", Serde[string, string]{}, Options{Runtime: r})
				defer cleanup()
				id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "retained"})
				if err != nil {
					t.Fatal(err)
				}
				usage, err := s.Usage(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				work, err := s.claimReadyBatch(t.Context(), r.Now(), 1, time.Minute, 1)
				if err != nil || len(work) != 1 {
					t.Fatalf("claim=%v err=%v", work, err)
				}
				work[0].permit, _ = b.admit()
				p, err := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{CircuitBreaker: &b.opts}})
				if err != nil {
					t.Fatal(err)
				}
				p.breaker = b
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				ctx, endTraces := p.startMessageTraces(ctx, work, r.Now())
				defer endTraces()
				pending := map[MessageID]claimedRecord[string, string]{id: work[0]}
				results := make(chan BatchProcessResult, 1)
				results <- BatchProcessResult{ID: id, Err: tc.err}
				state, generation := b.state, b.generation
				cancel() // Deterministically cancel after delivery but before settlement.
				if err := p.cancelPendingBatchResults(ctx, results, pending, r.Now()); err != nil {
					t.Fatal(err)
				}
				if len(pending) != 0 || b.state != state || b.generation != generation {
					t.Fatal("shutdown left pending results or changed circuit generation")
				}
				snapshot, err := s.QueueSnapshot(t.Context())
				if err != nil || snapshot.ProcessingDepth != 0 {
					t.Fatalf("snapshot=%+v err=%v", snapshot, err)
				}
				if tc.wantDefer {
					msg, err := s.Get(t.Context(), id)
					if err != nil || msg.Attempt != 0 || !msg.AvailableAt.Equal(r.Now().Add(b.opts.InitialCooldown)) || snapshot.ReadyDepth != 1 || snapshot.DeadLetterDepth != 0 {
						t.Fatalf("known outage was not deferred: message=%+v snapshot=%+v err=%v", msg, snapshot, err)
					}
					assertUsage(t, s, usage.RetainedMessages, usage.RetainedBytes)
				} else if snapshot.ReadyDepth != 0 || snapshot.DeadLetterDepth != 1 {
					t.Fatalf("shutdown/permanent error bypassed retry budget: %+v", snapshot)
				}
			})
		}
	}
}
