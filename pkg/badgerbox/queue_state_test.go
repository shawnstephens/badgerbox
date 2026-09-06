package badgerbox

import (
	"context"

	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestNewInitializesQueueStateForFreshNamespace(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "queue-state-fresh", Serde[testPayload, testDestination]{})
	defer cleanup()

	err := store.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(store.keys.queueStateVersionKey)
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if len(value) != 1 || value[0] != queueStateVersion {
			t.Fatalf("queue-state version = %v, want [%d]", value, queueStateVersion)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view queue-state version: %v", err)
	}
}

func TestQueueStateMetadataTracksLifecycle(t *testing.T) {
	t.Parallel()

	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0).UTC())
	runtime.tokenFunc = func() (string, error) { return "lease-token", nil }

	_, store, cleanup := openTestStoreWithOptions(t, "queue-state-lifecycle", Serde[testPayload, testDestination]{}, Options{
		Runtime: runtime,
	})
	defer cleanup()

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "alpha"},
		Destination: testDestination{Route: "/alpha"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	assertQueueStateCounts(t, store, 1, 0, 0)
	if got := countKeysWithPrefix(t, store.db, store.keys.readyCreatedPrefix); got != 1 {
		t.Fatalf("ready created key count = %d, want 1", got)
	}

	claimed, err := store.claimReadyBatch(context.Background(), runtime.Now(), 1, time.Minute, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claimReadyBatch: %v", err)
	}
	if len(claimed) != 1 || claimed[0].Message.ID != id {
		t.Fatalf("claimed = %#v, want message %d", claimed, id)
	}

	assertQueueStateCounts(t, store, 0, 1, 0)
	if got := countKeysWithPrefix(t, store.db, store.keys.readyCreatedPrefix); got != 0 {
		t.Fatalf("ready created key count after claim = %d, want 0", got)
	}
	if got := countKeysWithPrefix(t, store.db, store.keys.processingCreatedPrefix); got != 1 {
		t.Fatalf("processing created key count after claim = %d, want 1", got)
	}

	result, err := store.failProcessing(context.Background(), id, claimed[0].LeaseToken, errors.New("retry once"), time.Second, time.Second)
	if err != nil {
		t.Fatalf("failProcessing retry: %v", err)
	}
	if result.outcome != metricOutcomeRetried {
		t.Fatalf("retry outcome = %q, want %q", result.outcome, metricOutcomeRetried)
	}

	assertQueueStateCounts(t, store, 1, 0, 0)
	if got := countKeysWithPrefix(t, store.db, store.keys.readyCreatedPrefix); got != 1 {
		t.Fatalf("ready created key count after retry = %d, want 1", got)
	}
	if got := countKeysWithPrefix(t, store.db, store.keys.processingCreatedPrefix); got != 0 {
		t.Fatalf("processing created key count after retry = %d, want 0", got)
	}

	runtime.SetNow(runtime.Now().Add(2 * time.Second))
	claimed, err = store.claimReadyBatch(context.Background(), runtime.Now(), 1, time.Minute, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claimReadyBatch second attempt: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed second attempt len = %d, want 1", len(claimed))
	}

	if err := store.acknowledge(context.Background(), id, claimed[0].LeaseToken); err != nil {
		t.Fatalf("acknowledge: %v", err)
	}

	assertQueueStateCounts(t, store, 0, 0, 0)
	if got := countKeysWithPrefix(t, store.db, store.keys.readyCreatedPrefix); got != 0 {
		t.Fatalf("ready created key count after ack = %d, want 0", got)
	}
	if got := countKeysWithPrefix(t, store.db, store.keys.processingCreatedPrefix); got != 0 {
		t.Fatalf("processing created key count after ack = %d, want 0", got)
	}
}

func TestQueueStateMetadataTracksDeadLetterRequeue(t *testing.T) {
	t.Parallel()

	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0).UTC())
	runtime.tokenFunc = func() (string, error) { return "lease-token", nil }

	_, store, cleanup := openTestStoreWithOptions(t, "queue-state-dlq", Serde[testPayload, testDestination]{}, Options{
		Runtime: runtime,
	})
	defer cleanup()

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "dead"},
		Destination: testDestination{Route: "/dead"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := store.claimReadyBatch(context.Background(), runtime.Now(), 1, time.Minute, defaultMaxAttempts)
	if err != nil {
		t.Fatalf("claimReadyBatch: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claimed len = %d, want 1", len(claimed))
	}

	result, err := store.failProcessing(context.Background(), id, claimed[0].LeaseToken, Permanent(errors.New("dead")), time.Second, time.Second)
	if err != nil {
		t.Fatalf("failProcessing dead-letter: %v", err)
	}
	if result.outcome != metricOutcomeDeadLetter {
		t.Fatalf("dead-letter outcome = %q, want %q", result.outcome, metricOutcomeDeadLetter)
	}

	assertQueueStateCounts(t, store, 0, 0, 1)

	runtime.SetNow(runtime.Now().Add(time.Second))
	if err := store.RequeueDeadLetter(context.Background(), id, runtime.Now().Add(-time.Second), runtime.Now()); err != nil {
		t.Fatalf("RequeueDeadLetter: %v", err)
	}

	assertQueueStateCounts(t, store, 1, 0, 0)
	if got := countKeysWithPrefix(t, store.db, store.keys.readyCreatedPrefix); got != 1 {
		t.Fatalf("ready created key count after dlq requeue = %d, want 1", got)
	}
}

func assertQueueStateCounts(t *testing.T, s *Store[testPayload, testDestination], ready, processing, dead int64) {
	t.Helper()
	q, err := s.QueueSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if q.ReadyDepth != ready || q.ProcessingDepth != processing || q.DeadLetterDepth != dead {
		t.Fatalf("snapshot=%+v, want %d/%d/%d", q, ready, processing, dead)
	}
}
