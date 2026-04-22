package badgerbox

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestNewProcessorValidatesInputs(t *testing.T) {
	t.Parallel()

	process := ProcessFunc[testPayload, testDestination](func(context.Context, Message[testPayload, testDestination]) error {
		return nil
	})
	if _, err := NewProcessor[testPayload, testDestination](nil, process, ProcessorOptions{}); !errors.Is(err, ErrNilStore) {
		t.Fatalf("NewProcessor(nil, fn) err = %v, want %v", err, ErrNilStore)
	}

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "processor-guards", Serde[testPayload, testDestination]{})
	defer cleanup()

	if _, err := NewProcessor[testPayload, testDestination](store, nil, ProcessorOptions{}); !errors.Is(err, ErrProcessorFuncNil) {
		t.Fatalf("NewProcessor(store, nil) err = %v, want %v", err, ErrProcessorFuncNil)
	}
}

func TestProcessorClaimsMessagesInOrder(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "ordered", Serde[testPayload, testDestination]{})
	defer cleanup()

	base := time.Now().UTC().Add(30 * time.Millisecond)
	requests := []EnqueueRequest[testPayload, testDestination]{
		{Payload: testPayload{Name: "third"}, Destination: testDestination{Route: "/3"}, AvailableAt: base.Add(40 * time.Millisecond)},
		{Payload: testPayload{Name: "first"}, Destination: testDestination{Route: "/1"}, AvailableAt: base},
		{Payload: testPayload{Name: "second"}, Destination: testDestination{Route: "/2"}, AvailableAt: base.Add(20 * time.Millisecond)},
	}
	for _, req := range requests {
		if _, err := store.Enqueue(context.Background(), req); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var (
		mu    sync.Mutex
		order []string
	)

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		mu.Lock()
		order = append(order, msg.Payload.Name)
		mu.Unlock()
		return nil
	}, ProcessorOptions{
		Concurrency:    1,
		ClaimBatchSize: 1,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	want := []string{"first", "second", "third"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected processing order: got %v want %v", got, want)
	}
}

func TestProcessorConcurrentWorkersProcessDistinctMessages(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "concurrent", Serde[testPayload, testDestination]{})
	defer cleanup()

	const total = 8
	for i := 0; i < total; i++ {
		if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
			Payload:     testPayload{Name: fmt.Sprintf("msg-%d", i)},
			Destination: testDestination{Route: "/bulk"},
		}); err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
	}

	var (
		mu        sync.Mutex
		counts    = make(map[MessageID]int)
		completed int
		release   = make(chan struct{})
	)

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		mu.Lock()
		counts[msg.ID]++
		mu.Unlock()

		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}

		mu.Lock()
		completed++
		mu.Unlock()
		return nil
	}, ProcessorOptions{
		Concurrency:    4,
		ClaimBatchSize: total,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(counts) == 4
	})
	close(release)

	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return completed == total
	})

	mu.Lock()
	defer mu.Unlock()
	for id, count := range counts {
		if count != 1 {
			t.Fatalf("message %d processed %d times", id, count)
		}
	}
}

func TestProcessorRetryableFailureReschedules(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "retry", Serde[testPayload, testDestination]{})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "retry"},
		Destination: testDestination{Route: "/retry"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var attempts atomic.Int32
	start := time.Now()

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		if attempts.Add(1) == 1 {
			return errors.New("try again")
		}
		return nil
	}, ProcessorOptions{
		Concurrency:    1,
		ClaimBatchSize: 1,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  50 * time.Millisecond,
		RetryBaseDelay: 25 * time.Millisecond,
		RetryMaxDelay:  25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		return attempts.Load() == 2
	})

	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("expected retry delay, got %v", elapsed)
	}
	assertMessageDeleted(t, store)
}

func TestProcessorPermanentFailureMovesToDLQ(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "dlq-perm", Serde[testPayload, testDestination]{})
	defer cleanup()

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "dead"},
		Destination: testDestination{Route: "/dead"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		return Permanent(errors.New("do not retry"))
	}, ProcessorOptions{
		PollInterval:  5 * time.Millisecond,
		LeaseDuration: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	var deadLetters []DeadLetter[testPayload, testDestination]
	waitFor(t, func() bool {
		var cursor []byte
		deadLetters, cursor, err = store.ListDeadLetters(context.Background(), 10, nil)
		if err != nil {
			t.Fatalf("list dead letters: %v", err)
		}
		_ = cursor
		return len(deadLetters) == 1
	})

	if deadLetters[0].Message.ID != id || !deadLetters[0].Permanent {
		t.Fatalf("unexpected dead letter: %#v", deadLetters[0])
	}
}

func TestProcessorMaxAttemptsMovesToDLQ(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "dlq-max", Serde[testPayload, testDestination]{})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "max"},
		Destination: testDestination{Route: "/max"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		return errors.New("keep failing")
	}, ProcessorOptions{
		Concurrency:    1,
		ClaimBatchSize: 1,
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  50 * time.Millisecond,
		RetryBaseDelay: 5 * time.Millisecond,
		RetryMaxDelay:  5 * time.Millisecond,
		MaxAttempts:    2,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	var deadLetters []DeadLetter[testPayload, testDestination]
	waitFor(t, func() bool {
		var err error
		deadLetters, _, err = store.ListDeadLetters(context.Background(), 10, nil)
		if err != nil {
			t.Fatalf("list dead letters: %v", err)
		}
		return len(deadLetters) == 1
	})

	if deadLetters[0].Message.Attempt != 2 || deadLetters[0].Permanent {
		t.Fatalf("unexpected dead letter attempt data: %#v", deadLetters[0])
	}
}

func TestProcessorReclaimsExpiredLeaseAndIgnoresStaleAck(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "reclaim", Serde[testPayload, testDestination]{})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "lease"},
		Destination: testDestination{Route: "/lease"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var attempts atomic.Int32
	firstRunning := make(chan struct{})
	releaseFirst := make(chan struct{})

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		switch attempts.Add(1) {
		case 1:
			close(firstRunning)
			<-releaseFirst
			return nil
		default:
			return nil
		}
	}, ProcessorOptions{
		Concurrency:    2,
		ClaimBatchSize: 1,
		PollInterval:   10 * time.Millisecond,
		LeaseDuration:  30 * time.Millisecond,
		RetryBaseDelay: 10 * time.Millisecond,
		RetryMaxDelay:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitForChannel(t, firstRunning)
	waitFor(t, func() bool {
		return attempts.Load() >= 2
	})
	close(releaseFirst)

	assertMessageDeleted(t, store)
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts.Load())
	}
}

func TestProcessorRecoversPanic(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "panic", Serde[testPayload, testDestination]{})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "panic"},
		Destination: testDestination{Route: "/panic"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	var attempts atomic.Int32
	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		if attempts.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}, ProcessorOptions{
		PollInterval:   5 * time.Millisecond,
		LeaseDuration:  50 * time.Millisecond,
		RetryBaseDelay: 5 * time.Millisecond,
		RetryMaxDelay:  5 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		return attempts.Load() == 2
	})
	assertMessageDeleted(t, store)
}

func TestProcessorRunIgnoresSnapshotFailuresAtStartup(t *testing.T) {
	t.Parallel()

	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	expected := errors.New("snapshot failed")

	_, store, cleanup := openTestStoreWithOptions(t, "processor-snapshot-failure", Serde[testPayload, testDestination]{}, Options{
		Runtime: runtime,
		Observability: ObservabilityOptions{
			MeterProvider: provider,
		},
	})
	defer cleanup()

	store.obs.queueSnapshot = func(context.Context) (queueSnapshot, error) {
		return queueSnapshot{}, expected
	}

	processor, err := NewProcessor(store, func(context.Context, Message[testPayload, testDestination]) error {
		return nil
	}, ProcessorOptions{})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		return runtime.TickerCount() == 3
	})
}

func TestProcessorRunAdvancesFromRuntimeTickers(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_700_000_000, 0).UTC()
	runtime := newFakeRuntime(base)

	_, store, cleanup := openTestStoreWithOptions(t, "processor-runtime", Serde[testPayload, testDestination]{}, Options{Runtime: runtime})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "later"},
		Destination: testDestination{Route: "/later"},
		AvailableAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	processed := make(chan string, 1)
	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		processed <- msg.Payload.Name
		return nil
	}, ProcessorOptions{
		Concurrency:    1,
		ClaimBatchSize: 1,
		PollInterval:   time.Second,
		LeaseDuration:  time.Minute,
	})
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		return runtime.TickerCount() == 2
	})

	select {
	case got := <-processed:
		t.Fatalf("message processed before runtime tick: %q", got)
	default:
	}

	runtime.SetNow(base.Add(time.Minute))
	runtime.TickAll()

	select {
	case got := <-processed:
		if got != "later" {
			t.Fatalf("processed payload = %q, want later", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runtime-driven processing")
	}
}
