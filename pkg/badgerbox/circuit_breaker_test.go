package badgerbox

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func testCircuit(t *testing.T, opts CircuitBreakerOptions) (*circuitBreaker, *fakeRuntime) {
	t.Helper()
	opts.DisableJitter = true // Exact schedules for existing state-machine tests.
	normalized, err := normalizeCircuitBreakerOptions(&opts)
	if err != nil {
		t.Fatal(err)
	}
	r := newFakeRuntime(time.Now().UTC())
	r.sleepFunc = func(ctx context.Context, d time.Duration) error {
		if err := ctxErr(ctx); err != nil {
			return err
		}
		r.Advance(d)
		return nil
	}
	return newCircuitBreaker(normalized, r, nil), r
}

func TestCircuitOptionsAndErrors(t *testing.T) {
	base := errors.New("offline")
	wrapped := fmt.Errorf("publish: %w", Unavailable(base))
	if !IsUnavailable(wrapped) || !errors.Is(wrapped, base) || Unavailable(nil) != nil {
		t.Fatal("wrapper lost identity")
	}
	for _, o := range []CircuitBreakerOptions{{FailureThreshold: -1}, {InitialCooldown: -1}, {MaxCooldown: -1}, {MessageErrorCooldown: -1}, {InitialCooldown: time.Minute, MaxCooldown: time.Second}} {
		if _, err := normalizeCircuitBreakerOptions(&o); err == nil {
			t.Fatalf("accepted %+v", o)
		}
	}
	opts, err := normalizeCircuitBreakerOptions(&CircuitBreakerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if opts.FailureThreshold != 3 || opts.InitialCooldown != 5*time.Second || opts.MaxCooldown != 5*time.Second || opts.MessageErrorCooldown != time.Second || opts.DisableJitter {
		t.Fatalf("defaults: %+v", opts)
	}
	b, _ := testCircuit(t, CircuitBreakerOptions{IsUnavailable: func(error) bool { return true }})
	if b.unavailable(Permanent(base)) || b.unavailable(nil) {
		t.Fatal("classification precedence")
	}
}

func TestCircuitRecoveryReservationsAndGenerations(t *testing.T) {
	b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 2, InitialCooldown: time.Second, MaxCooldown: 4 * time.Second})
	old, _ := b.admit()
	b.report(old, false, true)
	b.report(old, false, false) // Message errors are neutral.
	b.report(old, true, false)  // Success resets availability streak.
	b.report(old, false, true)
	if state, _ := b.signals(); state != circuitClosed {
		t.Fatal(state)
	}
	b.report(old, false, true)
	if b.start(old) {
		t.Fatal("obsolete start permitted")
	}
	if _, ok := b.admit(); ok {
		t.Fatal("open admission")
	}
	if delay := b.reservation.DelayFrom(r.Now()); delay != time.Second {
		t.Fatalf("initial token was not drained: %v", delay)
	}
	r.Advance(time.Second - time.Nanosecond)
	if delay := b.reservation.DelayFrom(r.Now()); delay != time.Nanosecond {
		t.Fatal(delay)
	}
	b.report(old, true, false)
	if state, _ := b.signals(); state != circuitOpen {
		t.Fatal("old success closed circuit")
	}
	for _, want := range []time.Duration{2 * time.Second, 4 * time.Second, 4 * time.Second} {
		if err := b.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
		trial, ok := b.admit()
		if !ok || !trial.trial {
			t.Fatal("trial denied")
		}
		r.Advance(time.Hour) // A slow trial cannot accumulate another admission.
		if _, ok := b.admit(); ok {
			t.Fatal("overlapping trial")
		}
		b.report(trial, false, true)
		if b.cooldown != want || b.reservation.DelayFrom(r.Now()) != want {
			t.Fatalf("cooldown=%v want=%v", b.cooldown, want)
		}
	}
	if err := b.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	trial, _ := b.admit()
	b.empty(trial)
	trial, ok := b.admit()
	if !ok {
		t.Fatal("empty claim leaked trial")
	}
	b.report(trial, false, false)
	if b.cooldown != 4*time.Second {
		t.Fatal("message error changed cooldown")
	}
	if err := b.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	trial, _ = b.admit()
	b.report(trial, true, false)
	b.report(old, false, true)
	if state, _ := b.signals(); state != circuitClosed || b.cooldown != time.Second {
		t.Fatal("recovery did not reset")
	}
}

func TestCircuitConcurrentFailuresAndCancellation(t *testing.T) {
	b, r := testCircuit(t, CircuitBreakerOptions{})
	permit, _ := b.admit()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); b.report(permit, false, true) }()
	}
	wg.Wait()
	if b.generation != 1 {
		t.Fatalf("late failures reopened circuit: %d", b.generation)
	}
	r.sleepFunc = func(ctx context.Context, _ time.Duration) error { <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.wait(ctx) }()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, ok := b.admit(); ok {
		t.Fatal("canceled wait admitted work")
	}
	b.close()
}

func TestDeferClaimRefundsOnceAndPreservesIndexes(t *testing.T) {
	_, s, cleanup := openTestStore[testPayload, testDestination](t, "defer", Serde[testPayload, testDestination]{})
	defer cleanup()
	id, err := s.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{Payload: testPayload{Name: "one"}})
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	now := s.runtime.Now()
	claimed, err := s.claimReadyBatch(context.Background(), now, 1, time.Second, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %d", err, len(claimed))
	}
	at := now.Add(time.Minute)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			applied, err := s.releaseClaimedAt(context.Background(), []claimedRecord[testPayload, testDestination]{claimed[0]}, at)
			if err != nil {
				t.Error(err)
			}
			if applied > 0 {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 {
		t.Fatal("release applied more than once")
	}
	msg, err := s.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Attempt != 0 || !msg.AvailableAt.Equal(at) {
		t.Fatalf("message=%+v", msg)
	}
	snapshot, err := s.queueSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReadyDepth != 1 || snapshot.ProcessingDepth != 0 || snapshot.DeadLetterDepth != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	assertUsage(t, s, usage.RetainedMessages, usage.RetainedBytes)
	next, err := s.claimReadyBatch(context.Background(), at, 1, time.Second, 1)
	if err != nil || len(next) != 1 {
		t.Fatal(err)
	}
	if applied, err := s.releaseClaimedAt(context.Background(), []claimedRecord[testPayload, testDestination]{claimed[0]}, now); err != nil || applied != 0 {
		t.Fatal("stale release applied", err)
	}
	if _, err = s.requeueExpired(context.Background(), at.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if applied, err := s.releaseClaimedAt(context.Background(), []claimedRecord[testPayload, testDestination]{next[0]}, now); err != nil || applied != 0 {
		t.Fatal("expired ownership refunded", err)
	}
	msg, err = s.Get(context.Background(), id)
	if err != nil || msg.Attempt != 1 {
		t.Fatalf("expired attempt: %+v %v", msg, err)
	}
}

func TestCircuitMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer provider.Shutdown(context.Background())
	obs, err := newOTelInstrumentation(ObservabilityOptions{MeterProvider: provider}, "breaker", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer obs.Close()
	b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
	b.obs = obs
	permit, _ := b.admit()
	b.report(permit, false, true)
	r.Advance(time.Second)
	if err := b.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	permit, _ = b.admit()
	b.report(permit, true, false)
	obs.RecordCircuitDeferred(context.Background(), "unavailable")
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	ns := attribute.String("namespace", "breaker")
	if got := int64SumValueWithAttrs(metrics, "badgerbox_circuit_transitions_total", ns, attribute.String("from", "closed"), attribute.String("to", "open")); got != 1 {
		t.Fatal(got)
	}
	if got := int64SumValueWithAttrs(metrics, "badgerbox_circuit_trials_total", ns, attribute.String("outcome", "success")); got != 1 {
		t.Fatal(got)
	}
	if got := int64SumValueWithAttrs(metrics, "badgerbox_circuit_deferred_total", ns); got != 1 {
		t.Fatal(got)
	}
	if got := float64HistogramCountWithAttrs(metrics, "badgerbox_circuit_open_duration_seconds", ns); got != 1 {
		t.Fatal(got)
	}
	if got := float64HistogramCountWithAttrs(metrics, "badgerbox_circuit_recovery_delay_seconds", ns, attribute.String("reason", "unavailable")); got != 1 {
		t.Fatal(got)
	}
	if got := int64GaugeValueWithAttrs(metrics, "badgerbox_circuit_state", ns); got != 0 {
		t.Fatal(got)
	}
}

// Keep the imported Badger error checked as a storage error, not an outage.

func TestCircuitDoesNotClassifyStorageErrors(t *testing.T) {
	b, _ := testCircuit(t, CircuitBreakerOptions{})
	if b.unavailable(badger.ErrDBClosed) {
		t.Fatal("storage error classified as outage")
	}
}

func TestHalfOpenAdmissionIsExclusiveAndEmptyClaimRetainsPermission(t *testing.T) {
	b, _ := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
	p, _ := b.admit()
	b.report(p, false, true)
	if err := b.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var admitted atomic.Int32
	permits := make(chan circuitPermit, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if p, ok := b.admit(); ok {
				admitted.Add(1)
				permits <- p
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("trials admitted: %d", admitted.Load())
	}
	b.empty(<-permits)
	if _, ok := b.admit(); !ok {
		t.Fatal("empty trial did not retain permission")
	}
}

func circuitBatchProcessor(t *testing.T, s *Store[testPayload, testDestination], fn BatchProcessFunc[testPayload, testDestination], opts BatchProcessorOptions) *BatchProcessor[testPayload, testDestination] {
	t.Helper()
	p, err := NewBatchProcessor(s, fn, opts)
	if err != nil {
		t.Fatal(err)
	}
	p.breaker = newCircuitBreaker(p.opts.CircuitBreaker, s.runtime, s.obs)
	return p
}

func TestCircuitOpenSkipsStoreAndReleasesQueuedBatches(t *testing.T) {
	_, s, cleanup := openTestStore[testPayload, testDestination](t, "queued-circuit", Serde[testPayload, testDestination]{})
	defer cleanup()
	for i := 0; i < 12; i++ {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{}); err != nil {
			t.Fatal(err)
		}
	}
	p := circuitBatchProcessor(t, s, func(context.Context, []Message[testPayload, testDestination], chan<- BatchProcessResult) error {
		t.Error("obsolete callback invoked")
		return nil
	}, BatchProcessorOptions{ClaimBatchSize: 4, ProcessorOptions: ProcessorOptions{Concurrency: 3, CircuitBreaker: &CircuitBreakerOptions{FailureThreshold: 1}}})
	queued := make(chan []claimedRecord[testPayload, testDestination], 3)
	slots := make(chan struct{}, 3)
	permit, _ := p.breaker.admit()
	for i := 0; i < 3; i++ {
		work, err := s.claimReadyBatch(t.Context(), s.runtime.Now(), 4, time.Minute, 1)
		if err != nil {
			t.Fatal(err)
		}
		for j := range work {
			work[j].permit = permit
		}
		s.obs.WorkQueuedBatch(len(work))
		queued <- work
	}
	p.breaker.report(permit, false, true)
	if err := p.drainQueuedWork(t.Context(), queued, slots); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.QueueSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.ReadyDepth != 12 || snapshot.ProcessingDepth != 0 || len(slots) != 3 {
		t.Fatalf("bad release: %+v slots=%d", snapshot, len(slots))
	}
	work, err := s.claimReadyBatch(t.Context(), s.runtime.Now(), 1, time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	work[0].permit = permit
	if err := p.processBatch(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := p.dispatchAvailable(t.Context(), queued, slots); err != nil {
		t.Fatalf("open gate accessed Badger: %v", err)
	}
}

func TestProcessorCircuitSuspendsClaimsAndRecovers(t *testing.T) {
	r := newFakeRuntime(time.Now().UTC())
	sleeping := make(chan struct{}, 1)
	wake := make(chan struct{}, 1)
	r.sleepFunc = func(ctx context.Context, d time.Duration) error {
		if d <= conflictRetryDelay {
			r.Advance(d)
			return nil
		}
		select {
		case sleeping <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-wake:
			r.Advance(d)
			return nil
		}
	}
	_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, "generic-circuit", Serde[testPayload, testDestination]{}, Options{Runtime: r})
	defer cleanup()
	var ids []MessageID
	for i := 0; i < 20; i++ {
		id, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	var calls atomic.Int32
	p, err := NewProcessor(s, func(context.Context, Message[testPayload, testDestination]) error {
		if calls.Add(1) == 1 {
			return Unavailable(errors.New("offline"))
		}
		return nil
	}, ProcessorOptions{Concurrency: 1, MaxAttempts: 1, CircuitBreaker: &CircuitBreakerOptions{FailureThreshold: 1, InitialCooldown: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runProcessor(p)
	defer stopProcessor(t, cancel, done)
	waitForChannel(t, sleeping)
	waitFor(t, func() bool {
		snapshot, err := s.QueueSnapshot(t.Context())
		return err == nil && snapshot.ProcessingDepth == 0
	})
	for _, id := range ids {
		msg, err := s.Get(t.Context(), id)
		if err != nil || msg.Attempt != 0 {
			t.Fatalf("attempt=%+v %v", msg, err)
		}
	}
	r.mu.Lock()
	before := r.tokenCounter
	r.mu.Unlock()
	for i := 0; i < 30; i++ {
		r.TickAll()
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{}); err != nil {
			t.Fatal(err)
		}
	}
	r.mu.Lock()
	after := r.tokenCounter
	r.mu.Unlock()
	if after != before || calls.Load() != 1 {
		t.Fatal("claims continued while open")
	}
	wake <- struct{}{}
	assertMessageDeleted(t, s)
	if calls.Load() != 51 {
		t.Fatal(calls.Load())
	}
}

func TestHalfOpenBatchOutcomes(t *testing.T) {
	for _, outcome := range []string{"unavailable", "ordinary", "permanent", "panic", "success", "shutdown", "terminal_outage"} {
		t.Run(outcome, func(t *testing.T) {
			b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
			_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, outcome, Serde[testPayload, testDestination]{}, Options{Runtime: r})
			defer cleanup()
			if _, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p := circuitBatchProcessor(t, s, func(_ context.Context, msgs []Message[testPayload, testDestination], results chan<- BatchProcessResult) error {
				var err error
				switch outcome {
				case "unavailable":
					err = Unavailable(errors.New("offline"))
				case "ordinary":
					err = errors.New("message")
				case "permanent":
					err = Permanent(Unavailable(errors.New("bad")))
				case "panic":
					panic("bad")
				case "shutdown":
					cancel()
					return context.Canceled
				case "terminal_outage":
					return Unavailable(errors.New("offline"))
				}
				results <- BatchProcessResult{ID: msgs[0].ID, Err: err}
				return nil
			}, BatchProcessorOptions{ClaimBatchSize: 16, ProcessorOptions: ProcessorOptions{MaxAttempts: 1, CircuitBreaker: &b.opts}})
			p.breaker = b
			if outcome == "panic" {
				b.opts.IsUnavailable = func(error) bool { return true }
			}
			permit, _ := b.admit()
			b.report(permit, false, true)
			if err := b.wait(ctx); err != nil {
				t.Fatal(err)
			}
			queued := make(chan []claimedRecord[testPayload, testDestination], 8)
			slots := make(chan struct{}, 8)
			for i := 0; i < 8; i++ {
				slots <- struct{}{}
			}
			if err := p.dispatchAvailable(ctx, queued, slots); err != nil {
				t.Fatal(err)
			}
			if len(queued) != 1 {
				t.Fatal("expected one trial")
			}
			work := <-queued
			if len(work) != 1 {
				t.Fatal("trial claimed a batch")
			}
			if err := p.processBatch(ctx, work); err != nil {
				t.Fatal(err)
			}
			state, _ := b.signals()
			if outcome == "shutdown" {
				if state != circuitHalfOpen {
					t.Fatal("shutdown changed circuit", state)
				}
				return
			}
			if outcome == "success" {
				if state != circuitClosed {
					t.Fatal(state)
				}
			} else if state != circuitOpen {
				t.Fatal(state)
			}
			snapshot, err := s.QueueSnapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			switch outcome {
			case "unavailable", "terminal_outage":
				if snapshot.ReadyDepth != 1 || snapshot.DeadLetterDepth != 0 {
					t.Fatal(snapshot)
				}
			case "success":
				if snapshot.ReadyDepth+snapshot.ProcessingDepth+snapshot.DeadLetterDepth != 0 {
					t.Fatal(snapshot)
				}
			default:
				if snapshot.DeadLetterDepth != 1 {
					t.Fatal(snapshot)
				}
			}
		})
	}
}

func TestRepeatedOutageTrialsPreserveAttempts(t *testing.T) {
	b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1, InitialCooldown: time.Second, MaxCooldown: 2 * time.Second})
	_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, "repeated-outage", Serde[testPayload, testDestination]{}, Options{Runtime: r})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{})
	if err != nil {
		t.Fatal(err)
	}
	p := circuitBatchProcessor(t, s, func(_ context.Context, msgs []Message[testPayload, testDestination], results chan<- BatchProcessResult) error {
		for _, m := range msgs {
			results <- BatchProcessResult{ID: m.ID, Err: Unavailable(errors.New("offline"))}
		}
		return nil
	}, BatchProcessorOptions{ClaimBatchSize: 16, ProcessorOptions: ProcessorOptions{MaxAttempts: 1, CircuitBreaker: &b.opts}})
	p.breaker = b
	for i := 0; i < 6; i++ {
		if err := b.wait(t.Context()); err != nil {
			t.Fatal(err)
		}
		permit, ok := b.admit()
		if !ok {
			t.Fatal("trial unavailable")
		}
		work, err := s.claimReadyBatch(t.Context(), r.Now(), 1, time.Minute, 1)
		if err != nil || len(work) != 1 {
			t.Fatalf("claim %v %d", err, len(work))
		}
		work[0].permit = permit
		if err = p.processBatch(t.Context(), work); err != nil {
			t.Fatal(err)
		}
		msg, err := s.Get(t.Context(), id)
		if err != nil || msg.Attempt != 0 {
			t.Fatalf("attempt exhausted: %+v %v", msg, err)
		}
	}
}

func TestCircuitReducesOutageWrites(t *testing.T) {
	type result struct{ claims, calls, bytes int64 }
	run := func(mode string) result {
		r := newFakeRuntime(time.Now().UTC())
		_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, "write-cost", Serde[testPayload, testDestination]{}, Options{Runtime: r})
		defer cleanup()
		for i := 0; i < 16; i++ {
			if _, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{Payload: testPayload{Name: "fixed backlog"}}); err != nil {
				t.Fatal(err)
			}
		}
		var got result
		opts := BatchProcessorOptions{ClaimBatchSize: 16, ProcessorOptions: ProcessorOptions{RetryBaseDelay: time.Second, RetryMaxDelay: time.Second, MaxAttempts: 10000}}
		if mode != "disabled" {
			opts.CircuitBreaker = &CircuitBreakerOptions{}
			if mode == "exponential" {
				opts.CircuitBreaker.MaxCooldown = time.Minute
				opts.CircuitBreaker.DisableJitter = true
			}
		}
		p := circuitBatchProcessor(t, s, func(_ context.Context, msgs []Message[testPayload, testDestination], results chan<- BatchProcessResult) error {
			for _, m := range msgs {
				got.calls++
				results <- BatchProcessResult{ID: m.ID, Err: Unavailable(errors.New("offline"))}
			}
			return nil
		}, opts)
		defer p.breaker.close()
		writes := expvar.Get("badger_write_bytes_user").(*expvar.Int)
		before := writes.Value()
		if p.breaker != nil {
			p.breaker.random = func() float64 { return 0.5 }
		}
		for tick := 0; tick < 600; tick++ {
			if p.breaker != nil {
				state, _ := p.breaker.signals()
				if state == circuitOpen && p.breaker.reservation.DelayFrom(p.breaker.now()) == 0 {
					if err := p.breaker.wait(t.Context()); err != nil {
						t.Fatal(err)
					}
				}
			}
			permit, ok := p.breaker.admit()
			if ok {
				n := 16
				if permit.trial {
					n = 1
				}
				work, err := s.claimReadyBatch(t.Context(), r.Now(), n, time.Minute, 10000)
				if err != nil {
					t.Fatal(err)
				}
				if len(work) == 0 {
					p.breaker.empty(permit)
				} else {
					for i := range work {
						work[i].permit = permit
					}
					if err = p.processBatch(t.Context(), work); err != nil {
						t.Fatal(err)
					}
				}
			}
			r.Advance(100 * time.Millisecond)
		}
		got.bytes = writes.Value() - before
		r.mu.Lock()
		got.claims = int64(r.tokenCounter)
		r.mu.Unlock()
		return got
	}
	disabled, old, enabled := run("disabled"), run("exponential"), run("fixed")
	t.Logf("previous exponential policy: claims=%d calls=%d Badger user-write bytes=%d", old.claims, old.calls, old.bytes)
	t.Logf("60s simulated outage, 16 messages: disabled claims=%d calls=%d Badger user-write bytes=%d; enabled claims=%d calls=%d Badger user-write bytes=%d", disabled.claims, disabled.calls, disabled.bytes, enabled.claims, enabled.calls, enabled.bytes)
	if enabled.claims >= disabled.claims/2 || enabled.calls >= disabled.calls/2 || enabled.bytes >= disabled.bytes/2 {
		t.Fatal("outage IO not reduced")
	}
}

func TestExpiredUnstartedTrialReleasesPermission(t *testing.T) {
	b, r := testCircuit(t, CircuitBreakerOptions{FailureThreshold: 1})
	_, s, cleanup := openTestStoreWithOptions[testPayload, testDestination](t, "expired-trial", Serde[testPayload, testDestination]{}, Options{Runtime: r})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[testPayload, testDestination]{})
	if err != nil {
		t.Fatal(err)
	}
	p := circuitBatchProcessor(t, s, func(context.Context, []Message[testPayload, testDestination], chan<- BatchProcessResult) error {
		t.Error("expired callback invoked")
		return nil
	}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{CircuitBreaker: &b.opts}})
	p.breaker = b
	permit, _ := b.admit()
	b.report(permit, false, true)
	if err = b.wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	permit, _ = b.admit()
	work, err := s.claimReadyBatch(t.Context(), r.Now(), 1, time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	work[0].permit = permit
	r.Advance(2 * time.Second)
	if err = p.processBatch(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.admit(); !ok {
		t.Fatal("expired unstarted trial stranded the circuit")
	}
	msg, err := s.Get(t.Context(), id)
	if err != nil || msg.Attempt != 0 {
		t.Fatalf("expired trial release: %+v %v", msg, err)
	}
}
