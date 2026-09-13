package badgerbox

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCircuitTrialRetainsOccupancyThroughCleanup(t *testing.T) {
	for _, outcome := range []string{"lease_expiry", "unavailable", "message_error"} {
		t.Run(outcome, func(t *testing.T) {
			b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
			defer b.close()
			_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, "trial-cleanup", Serde[testPayload, testDestination]{}, Options{Runtime: r})
			defer cleanup()
			for range 2 {
				if _, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{}); err != nil {
					t.Fatal(err)
				}
			}
			canceled, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			releaseCleanup := func() { releaseOnce.Do(func() { close(release) }) }
			var calls atomic.Int32
			const lease = 30 * time.Millisecond
			p := circuitBatchProcessor(t, s, func(ctx context.Context, msgs []Message[testPayload, testDestination], results chan<- BatchProcessResult) error {
				if calls.Add(1) == 1 {
					if outcome != "lease_expiry" {
						err := errors.New("message failure")
						if outcome == "unavailable" {
							err = Unavailable(err)
						}
						results <- BatchProcessResult{ID: msgs[0].ID, Err: err}
					}
					<-ctx.Done()
					r.Advance(lease)
					close(canceled)
					<-release
					return ctx.Err()
				}
				results <- BatchProcessResult{ID: msgs[0].ID}
				return nil
			}, BatchProcessorOptions{ClaimBatchSize: 1, ProcessorOptions: ProcessorOptions{Concurrency: 2, LeaseDuration: lease, CircuitBreaker: &b.opts}})
			p.breaker = b
			normal, _ := b.admit()
			b.report(normal, false, true)
			if err := b.wait(t.Context()); err != nil {
				t.Fatal(err)
			}
			queued := make(chan []claimedRecord[testPayload, testDestination], 2)
			slots := make(chan struct{}, 2)
			slots <- struct{}{}
			slots <- struct{}{}
			if err := p.dispatchAvailable(t.Context(), queued, slots); err != nil {
				t.Fatal(err)
			}
			first := <-queued
			done := make(chan struct{})
			var processErr error
			go func() { processErr = p.processBatch(t.Context(), first); close(done) }()
			defer func() {
				releaseCleanup()
				waitForChannel(t, done)
				if processErr != nil {
					t.Error(processErr)
				}
			}()
			waitForChannel(t, canceled)
			waitFor(t, func() bool { state, _ := b.signals(); return state == circuitOpen })
			// Durable settlement must finish before callback cleanup is released.
			waitFor(t, func() bool { q, err := s.QueueSnapshot(t.Context()); return err == nil && q.ProcessingDepth == 0 })
			msg, err := s.Get(t.Context(), first[0].Message.ID)
			wantAttempt := 1
			if outcome == "unavailable" {
				wantAttempt = 0
			}
			if err != nil || msg.Attempt != wantAttempt {
				t.Fatalf("attempt=%d want=%d err=%v", msg.Attempt, wantAttempt, err)
			}
			if err := b.wait(t.Context()); err != nil {
				t.Fatal(err)
			}
			// The reservation has matured and a spare worker exists, but the original
			// invocation is still alive. No claim or callback may cross that boundary.
			if err := p.dispatchAvailable(t.Context(), queued, slots); err != nil {
				t.Fatal(err)
			}
			if len(queued) != 0 || calls.Load() != 1 {
				t.Fatal("second trial admitted during producer cleanup")
			}
			_, changed := b.signals()
			releaseCleanup()
			waitForChannel(t, done)
			select {
			case <-changed:
			default:
				t.Fatal("completed cleanup did not wake dispatch")
			}
			if err := p.dispatchAvailable(t.Context(), queued, slots); err != nil || len(queued) != 1 {
				t.Fatalf("trial did not resume: batches=%d err=%v", len(queued), err)
			}
			if err := p.processBatch(t.Context(), <-queued); err != nil {
				t.Fatal(err)
			}
			if state, _ := b.signals(); state != circuitClosed || calls.Load() != 2 {
				t.Fatal("successful recovery did not close circuit")
			}
		})
	}
}

func TestCircuitTrialOwnershipIgnoresObsoleteReleases(t *testing.T) {
	b, _ := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
	defer b.close()
	normal, _ := b.admit()
	b.report(normal, false, true)
	if err := b.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	empty, _ := b.admit()
	b.empty(empty)
	active, _ := b.admit()
	b.releaseUnstarted(empty)
	if b.start(empty) {
		t.Fatal("empty claim retained execution permission")
	}
	if _, ok := b.admit(); ok {
		t.Fatal("obsolete empty release cleared a newer trial")
	}
	b.report(active, false, true)
	if err := b.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.admit(); ok {
		t.Fatal("generation change cleared trial occupancy")
	}
	b.finishTrial(active)
	newer, ok := b.admit()
	if !ok {
		t.Fatal("old generation could not release its own trial")
	}
	b.finishTrial(active)
	if _, ok := b.admit(); ok {
		t.Fatal("duplicate completion released newer trial")
	}
	b.report(newer, true, false)
	// Success restores normal admission without waiting for invocation cleanup.
	if permit, ok := b.admit(); !ok || permit.trial {
		t.Fatal("success did not restore normal admission")
	}
	b.finishTrial(newer)
}
