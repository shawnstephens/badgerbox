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

	"github.com/dgraph-io/badger/v4"
)

type testPayload struct {
	Name string `json:"name"`
}

type testDestination struct {
	Route string `json:"route"`
}

type stringCodec struct {
	prefix string
}

func (c stringCodec) Marshal(value string) ([]byte, error) {
	return []byte(c.prefix + value), nil
}

func (c stringCodec) Unmarshal(data []byte) (string, error) {
	return string(data[len(c.prefix):]), nil
}

func TestJSONCodecRoundTrip(t *testing.T) {
	t.Parallel()

	codec := JSONCodec[testPayload]{}
	encoded, err := codec.Marshal(testPayload{Name: "hello"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded, err := codec.Unmarshal(encoded)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Name != "hello" {
		t.Fatalf("decoded payload mismatch: %#v", decoded)
	}
}

func TestStoreUsesCustomCodec(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[string, string](t, "custom", Serde[string, string]{
		Message:     stringCodec{prefix: "msg:"},
		Destination: stringCodec{prefix: "dst:"},
	})
	defer cleanup()

	id, err := store.Enqueue(context.Background(), EnqueueRequest[string, string]{
		Payload:     "payload",
		Destination: "destination",
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	message, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if message.Payload != "payload" || message.Destination != "destination" {
		t.Fatalf("decoded values mismatch: %#v", message)
	}

	err = store.db.View(func(txn *badger.Txn) error {
		record, err := store.loadRecord(txn, id)
		if err != nil {
			return err
		}
		if string(record.PayloadBytes) != "msg:payload" {
			t.Fatalf("unexpected payload bytes: %q", record.PayloadBytes)
		}
		if string(record.DestinationBytes) != "dst:destination" {
			t.Fatalf("unexpected destination bytes: %q", record.DestinationBytes)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view raw record: %v", err)
	}
}

func TestStoreEnqueueAndGetCreatesReadyIndex(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "enqueue", Serde[testPayload, testDestination]{})
	defer cleanup()

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "alpha"},
		Destination: testDestination{Route: "/alpha"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	message, err := store.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if message.Payload.Name != "alpha" || message.Destination.Route != "/alpha" {
		t.Fatalf("unexpected message: %#v", message)
	}

	readyCount := countKeysWithPrefix(t, store.db, store.keys.readyPrefix)
	if readyCount != 1 {
		t.Fatalf("expected 1 ready key, got %d", readyCount)
	}
}

func TestStoreEnqueueTxIsAtomic(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "atomic", Serde[testPayload, testDestination]{})
	defer cleanup()

	var id MessageID
	err := store.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("app/state"), []byte("ok")); err != nil {
			return err
		}

		var err error
		id, err = store.EnqueueTx(context.Background(), txn, EnqueueRequest[testPayload, testDestination]{
			Payload:     testPayload{Name: "alpha"},
			Destination: testDestination{Route: "/atomic"},
		})
		return err
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	err = store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte("app/state"))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if string(value) != "ok" {
			t.Fatalf("unexpected app value: %q", value)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify app state: %v", err)
	}

	if _, err := store.Get(context.Background(), id); err != nil {
		t.Fatalf("get outbox message: %v", err)
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

func TestStoreReopenPreservesPendingMessage(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	store, err := New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{Namespace: "reopen"})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "persisted"},
		Destination: testDestination{Route: "/persist"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	db, err = badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("reopen badger: %v", err)
	}
	defer db.Close()

	store, err = New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{Namespace: "reopen"})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer store.Close()

	processed := make(chan string, 1)
	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		processed <- msg.Payload.Name
		return nil
	}, ProcessorOptions{PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	select {
	case got := <-processed:
		if got != "persisted" {
			t.Fatalf("unexpected processed payload: %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for persisted message")
	}
}

func TestNamespacesRemainIsolated(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	defer db.Close()

	storeA, err := New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{Namespace: "a"})
	if err != nil {
		t.Fatalf("new storeA: %v", err)
	}
	defer storeA.Close()

	storeB, err := New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{Namespace: "b"})
	if err != nil {
		t.Fatalf("new storeB: %v", err)
	}
	defer storeB.Close()

	if _, err := storeA.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "from-a"},
		Destination: testDestination{Route: "/a"},
	}); err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	if _, err := storeB.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "from-b"},
		Destination: testDestination{Route: "/b"},
	}); err != nil {
		t.Fatalf("enqueue b: %v", err)
	}

	var (
		aCount atomic.Int32
		bCount atomic.Int32
	)

	processorA, err := NewProcessor(storeA, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		aCount.Add(1)
		return nil
	}, ProcessorOptions{PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("new processorA: %v", err)
	}
	processorB, err := NewProcessor(storeB, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		bCount.Add(1)
		return nil
	}, ProcessorOptions{PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("new processorB: %v", err)
	}

	cancelA, doneA := runProcessor(processorA)
	defer stopProcessor(t, cancelA, doneA)
	cancelB, doneB := runProcessor(processorB)
	defer stopProcessor(t, cancelB, doneB)

	waitFor(t, func() bool {
		return aCount.Load() == 1 && bCount.Load() == 1
	})
}

func TestRequeueDeadLetter(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "requeue-dlq", Serde[testPayload, testDestination]{})
	defer cleanup()

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "requeue"},
		Destination: testDestination{Route: "/requeue"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	processor, err := NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		return Permanent(errors.New("move to dlq"))
	}, ProcessorOptions{PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done := runProcessor(processor)
	waitFor(t, func() bool {
		deadLetters, _, err := store.ListDeadLetters(context.Background(), 10, nil)
		if err != nil {
			t.Fatalf("list dead letters: %v", err)
		}
		return len(deadLetters) == 1
	})
	stopProcessor(t, cancel, done)

	if err := store.RequeueDeadLetter(context.Background(), id, time.Now().UTC()); err != nil {
		t.Fatalf("requeue dead letter: %v", err)
	}

	var processed atomic.Int32
	processor, err = NewProcessor(store, func(ctx context.Context, msg Message[testPayload, testDestination]) error {
		processed.Add(1)
		return nil
	}, ProcessorOptions{PollInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	cancel, done = runProcessor(processor)
	defer stopProcessor(t, cancel, done)

	waitFor(t, func() bool {
		return processed.Load() == 1
	})
	assertMessageDeleted(t, store)
}

func openTestStore[M any, D any](t *testing.T, namespace string, serde Serde[M, D]) (*badger.DB, *Store[M, D], func()) {
	t.Helper()

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	store, err := New[M, D](db, serde, Options{Namespace: namespace})
	if err != nil {
		db.Close()
		t.Fatalf("new store: %v", err)
	}

	cleanup := func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	}

	return db, store, cleanup
}

func runProcessor[M any, D any](processor *Processor[M, D]) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processor.Run(ctx)
	}()
	return cancel, done
}

func stopProcessor(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processor stopped with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for processor shutdown")
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition not met before timeout")
}

func waitForChannel(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting on channel")
	}
}

func countKeysWithPrefix(t *testing.T, db *badger.DB, prefix []byte) int {
	t.Helper()

	count := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("count keys: %v", err)
	}
	return count
}

func assertMessageDeleted(t *testing.T, store *Store[testPayload, testDestination]) {
	t.Helper()

	waitFor(t, func() bool {
		messageCount := countKeysWithPrefix(t, store.db, store.keys.messagePrefix)
		readyCount := countKeysWithPrefix(t, store.db, store.keys.readyPrefix)
		processingCount := countKeysWithPrefix(t, store.db, store.keys.processingPrefix)
		return messageCount == 0 && readyCount == 0 && processingCount == 0
	})
}
