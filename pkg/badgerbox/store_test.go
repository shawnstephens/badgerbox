package badgerbox

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type stringCodec struct {
	prefix string
}

func (c stringCodec) Marshal(value string) ([]byte, error) {
	return []byte(c.prefix + value), nil
}

func (c stringCodec) Unmarshal(data []byte) (string, error) {
	return string(data[len(c.prefix):]), nil
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

	var aCount atomic.Int32
	var bCount atomic.Int32

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

func TestStoreGuardsAndHelpers(t *testing.T) {
	t.Parallel()

	t.Run("EnqueueTxNilTxn", func(t *testing.T) {
		_, store, cleanup := openTestStore[testPayload, testDestination](t, "enqueue-nil-txn", Serde[testPayload, testDestination]{})
		defer cleanup()

		_, err := store.EnqueueTx(context.Background(), nil, EnqueueRequest[testPayload, testDestination]{
			Payload:     testPayload{Name: "alpha"},
			Destination: testDestination{Route: "/alpha"},
		})
		if !errors.Is(err, ErrNilTxn) {
			t.Fatalf("EnqueueTx err = %v, want %v", err, ErrNilTxn)
		}
	})

	t.Run("ClosedStore", func(t *testing.T) {
		_, store, cleanup := openTestStore[testPayload, testDestination](t, "closed-store", Serde[testPayload, testDestination]{})
		defer cleanup()

		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}

		if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{}); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("Enqueue err = %v, want %v", err, ErrStoreClosed)
		}
		if _, err := store.Get(context.Background(), 1); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("Get err = %v, want %v", err, ErrStoreClosed)
		}
		if _, _, err := store.ListDeadLetters(context.Background(), 1, nil); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("ListDeadLetters err = %v, want %v", err, ErrStoreClosed)
		}
		if err := store.RequeueDeadLetter(context.Background(), 1, time.Time{}); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("RequeueDeadLetter err = %v, want %v", err, ErrStoreClosed)
		}
		if err := store.StartObservability(context.Background()); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("StartObservability err = %v, want %v", err, ErrStoreClosed)
		}
		if err := store.RecordObservabilitySnapshot(context.Background()); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("RecordObservabilitySnapshot err = %v, want %v", err, ErrStoreClosed)
		}
		if err := store.RepairQueueState(context.Background()); !errors.Is(err, ErrStoreClosed) {
			t.Fatalf("RepairQueueState err = %v, want %v", err, ErrStoreClosed)
		}
	})

	t.Run("ListDeadLettersZeroLimit", func(t *testing.T) {
		_, store, cleanup := openTestStore[testPayload, testDestination](t, "dead-letter-limit", Serde[testPayload, testDestination]{})
		defer cleanup()

		deadLetters, cursor, err := store.ListDeadLetters(context.Background(), 0, nil)
		if err != nil {
			t.Fatalf("ListDeadLetters: %v", err)
		}
		if deadLetters != nil || cursor != nil {
			t.Fatalf("ListDeadLetters(limit=0) = (%v, %v), want (nil, nil)", deadLetters, cursor)
		}
	})

	t.Run("RequeueDeadLetterNotFound", func(t *testing.T) {
		_, store, cleanup := openTestStore[testPayload, testDestination](t, "dead-letter-missing", Serde[testPayload, testDestination]{})
		defer cleanup()

		if err := store.RequeueDeadLetter(context.Background(), 999, time.Time{}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("RequeueDeadLetter err = %v, want %v", err, ErrNotFound)
		}
	})
}

func TestStoreRuntimeAffectsClaimLifecycle(t *testing.T) {
	t.Parallel()

	fixedNow := time.Unix(1_700_000_000, 0).UTC()
	runtime := newFakeRuntime(fixedNow)
	runtime.tokenFunc = func() (string, error) { return "lease-token", nil }

	_, store, cleanup := openTestStoreWithOptions(t, "runtime-claim", Serde[testPayload, testDestination]{}, Options{Runtime: runtime})
	defer cleanup()

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "timed"},
		Destination: testDestination{Route: "/timed"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := store.claimReadyBatch(context.Background(), runtime.Now(), 1, 10*time.Second, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claimReadyBatch: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed len = %d, want 1", len(claimed))
	}
	if claimed[0].LeaseToken != "lease-token" {
		t.Fatalf("lease token = %q, want lease-token", claimed[0].LeaseToken)
	}

	err = store.db.View(func(txn *badger.Txn) error {
		record, err := store.loadRecord(txn, id)
		if err != nil {
			return err
		}
		if record.Attempt != 1 {
			t.Fatalf("record attempt = %d, want 1", record.Attempt)
		}
		if record.LeaseToken != "lease-token" {
			t.Fatalf("record lease token = %q, want lease-token", record.LeaseToken)
		}
		if record.AvailableAtUnix != fixedNow.UnixNano() {
			t.Fatalf("record available at = %d, want %d", record.AvailableAtUnix, fixedNow.UnixNano())
		}
		if record.LeaseUntilUnix != fixedNow.Add(10*time.Second).UnixNano() {
			t.Fatalf("record lease until = %d, want %d", record.LeaseUntilUnix, fixedNow.Add(10*time.Second).UnixNano())
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view record: %v", err)
	}
}

func TestStoreLeaseTokenFailureIsTestable(t *testing.T) {
	t.Parallel()

	expected := errors.New("token failed")
	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	runtime.tokenFunc = func() (string, error) { return "", expected }

	_, store, cleanup := openTestStoreWithOptions(t, "token-failure", Serde[testPayload, testDestination]{}, Options{Runtime: runtime})
	defer cleanup()

	if _, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "lease"},
		Destination: testDestination{Route: "/lease"},
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	if _, err := store.claimReadyBatch(context.Background(), runtime.Now(), 1, time.Minute, defaultMaxAttempts); !errors.Is(err, expected) {
		t.Fatalf("claimReadyBatch err = %v, want %v", err, expected)
	}
}

func TestStoreCloneBytesAndCtxErr(t *testing.T) {
	t.Parallel()

	if got := cloneBytes(nil); got != nil {
		t.Fatalf("cloneBytes(nil) = %#v, want nil", got)
	}

	source := []byte("payload")
	cloned := cloneBytes(source)
	source[0] = 'X'
	if string(cloned) != "payload" {
		t.Fatalf("clone aliased source slice: %q", cloned)
	}

	if err := ctxErr(nil); err != nil {
		t.Fatalf("ctxErr(nil) = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ctxErr(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ctxErr(canceled) = %v, want %v", err, context.Canceled)
	}
}
