package badgerbox

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestNewInitializesQueueStateForFreshNamespace(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "queue-state-fresh", Serde[testPayload, testDestination]{})
	defer cleanup()

	if !store.queueStateEnabled() {
		t.Fatal("expected fresh namespace queue-state metadata to be enabled")
	}

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
	if err := store.RequeueDeadLetter(context.Background(), id, runtime.Now()); err != nil {
		t.Fatalf("RequeueDeadLetter: %v", err)
	}

	assertQueueStateCounts(t, store, 1, 0, 0)
	if got := countKeysWithPrefix(t, store.db, store.keys.readyCreatedPrefix); got != 1 {
		t.Fatalf("ready created key count after dlq requeue = %d, want 1", got)
	}
}

func TestRepairQueueStateBuildsMetadataForLegacyNamespace(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	defer db.Close()

	keys := newKeyspace("legacy-repair")
	now := time.Unix(1_700_000_000, 0).UTC()
	readyID := MessageID(1)
	processingID := MessageID(2)
	deadID := MessageID(3)

	readyRecord := storedRecord{
		ID:               readyID,
		PayloadBytes:     []byte(`{"name":"ready"}`),
		DestinationBytes: []byte(`{"route":"/ready"}`),
		CreatedAtUnix:    now.Add(-10 * time.Minute).UnixNano(),
		AvailableAtUnix:  now.Add(-time.Minute).UnixNano(),
		MaxAttempts:      defaultMaxAttempts,
		Status:           recordStatusPending,
	}
	processingRecord := storedRecord{
		ID:               processingID,
		PayloadBytes:     []byte(`{"name":"processing"}`),
		DestinationBytes: []byte(`{"route":"/processing"}`),
		CreatedAtUnix:    now.Add(-20 * time.Minute).UnixNano(),
		AvailableAtUnix:  now.Add(-19 * time.Minute).UnixNano(),
		Attempt:          1,
		MaxAttempts:      defaultMaxAttempts,
		Status:           recordStatusProcessing,
		LeaseToken:       "lease-token",
		LeaseUntilUnix:   now.Add(5 * time.Minute).UnixNano(),
	}
	deadRecord := storedRecord{
		ID:               deadID,
		PayloadBytes:     []byte(`{"name":"dead"}`),
		DestinationBytes: []byte(`{"route":"/dead"}`),
		CreatedAtUnix:    now.Add(-30 * time.Minute).UnixNano(),
		AvailableAtUnix:  now.Add(-29 * time.Minute).UnixNano(),
		Attempt:          defaultMaxAttempts,
		MaxAttempts:      defaultMaxAttempts,
		Status:           recordStatusProcessing,
		LeaseToken:       "dead-token",
		LeaseUntilUnix:   now.Add(-28 * time.Minute).UnixNano(),
	}

	err = db.Update(func(txn *badger.Txn) error {
		if err := storeLegacyRecord(txn, keys, readyRecord); err != nil {
			return err
		}
		if err := txn.Set(keys.readyKey(time.Unix(0, readyRecord.AvailableAtUnix).UTC(), readyID), emptyValue); err != nil {
			return err
		}
		if err := txn.Set(keys.readyKey(now, 999), emptyValue); err != nil {
			return err
		}

		if err := storeLegacyRecord(txn, keys, processingRecord); err != nil {
			return err
		}
		if err := txn.Set(keys.processingKey(time.Unix(0, processingRecord.LeaseUntilUnix).UTC(), processingID), []byte(processingRecord.LeaseToken)); err != nil {
			return err
		}

		deadLetter := storedDeadLetter{
			Record:    deadRecord,
			FailedAt:  now.Add(-27 * time.Minute).UnixNano(),
			Error:     "dead",
			Permanent: true,
		}
		encodedDeadLetter, err := json.Marshal(deadLetter)
		if err != nil {
			return err
		}
		if err := txn.Set(keys.deadLetterKey(time.Unix(0, deadLetter.FailedAt).UTC(), deadID), encodedDeadLetter); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatalf("seed legacy data: %v", err)
	}

	store, err := New[testPayload, testDestination](db, Serde[testPayload, testDestination]{}, Options{
		Namespace: "legacy-repair",
		Runtime:   newFakeRuntime(now),
	})
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if store.queueStateEnabled() {
		t.Fatal("expected unrepaired legacy namespace to use legacy queue snapshots")
	}

	snapshot, err := store.queueSnapshot(context.Background())
	if err != nil {
		t.Fatalf("legacy queueSnapshot: %v", err)
	}
	if snapshot.ReadyDepth != 1 || snapshot.ProcessingDepth != 1 || snapshot.DeadLetterDepth != 1 {
		t.Fatalf("legacy snapshot = %#v, want one record in each state", snapshot)
	}

	if err := store.RepairQueueState(context.Background()); err != nil {
		t.Fatalf("RepairQueueState: %v", err)
	}
	if !store.queueStateEnabled() {
		t.Fatal("expected repaired namespace to enable queue-state metadata")
	}

	assertQueueStateCounts(t, store, 1, 1, 1)
	if got := countKeysWithPrefix(t, store.db, store.keys.readyCreatedPrefix); got != 1 {
		t.Fatalf("ready created key count after repair = %d, want 1", got)
	}
	if got := countKeysWithPrefix(t, store.db, store.keys.processingCreatedPrefix); got != 1 {
		t.Fatalf("processing created key count after repair = %d, want 1", got)
	}

	snapshot, err = store.queueSnapshot(context.Background())
	if err != nil {
		t.Fatalf("fast queueSnapshot: %v", err)
	}
	if snapshot.ReadyDepth != 1 || snapshot.ProcessingDepth != 1 || snapshot.DeadLetterDepth != 1 {
		t.Fatalf("fast snapshot = %#v, want one record in each state", snapshot)
	}
	if snapshot.OldestReadyAge != 10*time.Minute {
		t.Fatalf("oldest ready age = %v, want 10m", snapshot.OldestReadyAge)
	}
	if snapshot.OldestProcessingAge != 20*time.Minute {
		t.Fatalf("oldest processing age = %v, want 20m", snapshot.OldestProcessingAge)
	}
}

func assertQueueStateCounts(t *testing.T, store *Store[testPayload, testDestination], ready, processing, deadLetter int64) {
	t.Helper()

	if got := queueStateCounterTotal(t, store.db, store.keys.readyCountPrefix); got != ready {
		t.Fatalf("ready counter total = %d, want %d", got, ready)
	}
	if got := queueStateCounterTotal(t, store.db, store.keys.processingCountPrefix); got != processing {
		t.Fatalf("processing counter total = %d, want %d", got, processing)
	}
	if got := queueStateCounterTotal(t, store.db, store.keys.deadLetterCountPrefix); got != deadLetter {
		t.Fatalf("dead-letter counter total = %d, want %d", got, deadLetter)
	}
}

func queueStateCounterTotal(t *testing.T, db *badger.DB, prefix []byte) int64 {
	t.Helper()

	var total int64
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(value) != 8 {
				t.Fatalf("counter value length = %d, want 8", len(value))
			}
			total += int64(binary.BigEndian.Uint64(value))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("queueStateCounterTotal: %v", err)
	}
	return total
}

func storeLegacyRecord(txn *badger.Txn, keys keyspace, record storedRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return txn.Set(keys.messageKey(record.ID), encoded)
}
