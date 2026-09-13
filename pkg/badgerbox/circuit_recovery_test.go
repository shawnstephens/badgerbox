package badgerbox

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestCircuitClockJumpDoesNotChangeElapsedWait(t *testing.T) {
	for _, shift := range []time.Duration{-10 * time.Minute, 10 * time.Minute} {
		t.Run(shift.String(), func(t *testing.T) {
			b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
			defer b.close()
			permit, _ := b.admit()
			b.report(permit, false, true)
			// A clock correction during opening cleanup must neither skip nor
			// extend the elapsed recovery wait.
			r.SetNow(r.Now().Add(shift))
			calls := 0
			r.sleepFunc = func(_ context.Context, delay time.Duration) error {
				calls++
				if calls > 1 {
					return errors.New("wall correction caused an extra sleep")
				}
				if delay != 5*time.Second {
					t.Errorf("elapsed delay=%v, want 5s", delay)
				}
				r.Advance(delay)
				// A second correction while asleep must not restart the wait.
				r.SetNow(r.Now().Add(shift))
				return nil
			}
			if err := b.wait(t.Context()); err != nil {
				t.Fatal(err)
			}
			trial, allowed := b.admit()
			if !allowed || !trial.trial || calls != 1 {
				t.Fatal("recovery permission was not issued after one elapsed wait")
			}
			b.empty(trial)
			_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, "clock-jump", Serde[testPayload, testDestination]{}, Options{Runtime: r})
			defer cleanup()
			id, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{})
			if err != nil {
				t.Fatal(err)
			}
			p := circuitBatchProcessor(t, s, func(context.Context, []Message[testPayload, testDestination], chan<- BatchProcessResult) error {
				return nil
			}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{CircuitBreaker: &b.opts}})
			p.breaker = b
			queued := make(chan []claimedRecord[testPayload, testDestination], 1)
			slots := make(chan struct{}, 1)
			slots <- struct{}{}
			if err := p.dispatchAvailable(t.Context(), queued, slots); err != nil || len(queued) != 1 {
				t.Fatalf("fresh eligible message could not probe after wall jump: %v", err)
			}
			work := <-queued
			if len(work) != 1 || work[0].Message.ID != id || !b.start(work[0].permit) {
				t.Fatal("wrong recovery claim")
			}
			if err := p.releaseQueuedBatch(t.Context(), work); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestSystemRuntimePreservesMonotonicReading(t *testing.T) {
	now := (SystemRuntime{}).MonotonicNow()
	if now == now.Round(0) { //nolint:staticcheck // Structural equality detects the monotonic reading, which Equal ignores.
		t.Fatal("elapsed clock has no monotonic reading")
	}
	wall := (SystemRuntime{}).Now()
	if wall.Location() != time.UTC {
		t.Fatal("durable wall clock changed timezone")
	}
}

func TestCircuitJitterAndFixedDefaults(t *testing.T) {
	b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
	b.opts.DisableJitter = false
	defer b.close()
	for _, nominal := range []time.Duration{1, 2, time.Second, 5 * time.Second, time.Duration(math.MaxInt64)} {
		for _, sample := range []float64{0, 0.5, 1} {
			b.random = func() float64 { return sample }
			got := b.sampleDelay(nominal)
			if got < minimumCircuitDelay(nominal) || got > nominal || got <= 0 {
				t.Fatalf("nominal=%v sample=%v delay=%v", nominal, sample, got)
			}
		}
	}
	for i, want := range []time.Duration{4 * time.Second, 5 * time.Second, 4500 * time.Millisecond} {
		sample := []float64{0, 1, 0.5}[i]
		b.random = func() float64 { return sample }
		permit, ok := b.admit()
		if !ok {
			t.Fatal("trial denied")
		}
		b.report(permit, false, true)
		if delay := b.reservation.DelayFrom(b.now()); delay != want || b.cooldown != 5*time.Second || b.deferralDelay() != 4*time.Second {
			t.Fatalf("delay=%v nominal=%v deferral=%v", delay, b.cooldown, b.deferralDelay())
		}
		r.Advance(want - time.Nanosecond)
		if b.reservation.DelayFrom(b.now()) <= 0 {
			t.Fatal("trial matured early")
		}
		if err := b.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	b.opts.DisableJitter = true
	if b.sampleDelay(5*time.Second) != 5*time.Second || b.deferralDelay() != 5*time.Second {
		t.Fatal("jitter disablement changed exact delays")
	}
}

func TestCircuitTrialReleaseWakesWithoutEmptyQueueSpin(t *testing.T) {
	b, _ := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
	defer b.close()
	p, _ := b.admit()
	b.report(p, false, true)
	if err := b.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	p, _ = b.admit()
	_, changed := b.signals()
	b.empty(p)
	select {
	case <-changed:
		t.Fatal("empty claim wakes itself and would spin")
	default:
	}
	p, _ = b.admit()
	b.releaseUnstarted(p)
	select {
	case <-changed:
	default:
		t.Fatal("unstarted release did not wake dispatcher")
	}
	if _, ok := b.admit(); !ok {
		t.Fatal("wake arrived before trial occupancy cleared")
	}
}

func TestCircuitClockJumpCancellation(t *testing.T) {
	for _, shift := range []time.Duration{-time.Hour, time.Hour} {
		b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
		p, _ := b.admit()
		b.report(p, false, true)
		ctx, cancel := context.WithCancel(t.Context())
		r.sleepFunc = func(context.Context, time.Duration) error {
			r.SetNow(r.Now().Add(shift))
			cancel()
			return ctx.Err()
		}
		if err := b.wait(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost after clock jump: %v", err)
		}
		// CancelAt must restore the future reserved token using elapsed time.
		if tokens := b.limiter.TokensAt(b.now()); tokens != 0 {
			t.Fatalf("reservation not canceled against elapsed clock: tokens=%v", tokens)
		}
		b.close()
	}
}

type legacyCircuitRuntime struct{ Runtime }

func TestCircuitLegacyRuntimeFallback(t *testing.T) {
	_, r := testCircuit(t, CircuitBreakerOptions{})
	opts, err := normalizeCircuitBreakerOptions(&CircuitBreakerOptions{FailureThreshold: 1, DisableJitter: true})
	if err != nil {
		t.Fatal(err)
	}
	b := newCircuitBreaker(opts, legacyCircuitRuntime{r}, nil)
	defer b.close()
	p, _ := b.admit()
	b.report(p, false, true)
	if err := b.wait(t.Context()); err != nil || b.state != circuitHalfOpen {
		t.Fatalf("legacy runtime: state=%v err=%v", b.state, err)
	}
}

func TestCircuitPoisonMessagesUseShortTrials(t *testing.T) {
	b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1, MaxCooldown: 40 * time.Second})
	defer b.close()
	_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, "short-trials", Serde[testPayload, testDestination]{}, Options{Runtime: r})
	defer cleanup()
	var retryID MessageID
	for _, name := range []string{"permanent", "retry", "healthy"} {
		id, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{Payload: testPayload{Name: name}})
		if err != nil {
			t.Fatal(err)
		}
		if name == "retry" {
			retryID = id
		}
		r.Advance(time.Nanosecond)
	}
	p := circuitBatchProcessor(t, s, func(_ context.Context, msgs []Message[testPayload, testDestination], results chan<- BatchProcessResult) error {
		if len(msgs) != 1 {
			t.Error("trial batch exceeds one message")
			return errors.New("unexpected trial batch size")
		}
		var err error
		switch msgs[0].Payload.Name {
		case "permanent":
			err = Permanent(errors.New("invalid"))
		case "retry":
			err = errors.New("retry later")
		}
		results <- BatchProcessResult{ID: msgs[0].ID, Err: err}
		return nil
	}, BatchProcessorOptions{ClaimBatchSize: 16, ProcessorOptions: ProcessorOptions{CircuitBreaker: &b.opts, RetryBaseDelay: time.Hour, RetryMaxDelay: time.Hour}})
	p.breaker = b
	permit, _ := b.admit()
	b.report(permit, false, true)
	if err := b.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	permit, _ = b.admit()
	b.report(permit, false, true) // Accumulate outage backoff before bad messages.
	queued := make(chan []claimedRecord[testPayload, testDestination], 1)
	slots := make(chan struct{}, 1)
	slots <- struct{}{}
	for i := range 3 {
		if err := b.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := p.dispatchAvailable(t.Context(), queued, slots); err != nil || len(queued) != 1 {
			t.Fatalf("dispatch: batches=%d err=%v", len(queued), err)
		}
		if err := p.processWorkerBatch(t.Context(), <-queued); err != nil {
			t.Fatal(err)
		}
		slots <- struct{}{}
		if i < 2 {
			if b.state != circuitOpen || b.cooldown != 10*time.Second || b.reservation.DelayFrom(b.now()) != time.Second {
				t.Fatalf("message error reset outage backoff or retained long delay: %+v", b)
			}
		} else if b.state != circuitClosed || b.cooldown != 5*time.Second {
			t.Fatal("healthy trial did not restore normal admission")
		}
	}
	msg, err := s.Get(t.Context(), retryID)
	if err != nil || msg.Attempt != 1 || !msg.AvailableAt.After(r.Now()) {
		t.Fatalf("ordinary retry was refunded or admitted early: %+v %v", msg, err)
	}
	q, err := s.QueueSnapshot(t.Context())
	if err != nil || q.DeadLetterDepth != 1 || q.ReadyDepth != 1 || q.ProcessingDepth != 0 {
		t.Fatalf("queue=%+v err=%v", q, err)
	}
}
