package badgerbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type guardCodec struct{ calls *int }

func (c guardCodec) Marshal(s string) ([]byte, error)   { *c.calls++; return []byte(s), nil }
func (c guardCodec) Unmarshal(b []byte) (string, error) { return string(b), nil }

func TestEnqueueGuardRejectsBeforeAllocationSerializationAndWrites(t *testing.T) {
	rejected := errors.New("disk pressure")
	calls := 0
	db, store, cleanup := openTestStoreWithOptions(t, "guard", Serde[string, string]{Message: guardCodec{&calls}}, Options{EnqueueGuard: func(context.Context) error { return rejected }})
	defer cleanup()
	next := store.seq.next
	if _, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "message"}); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("app/state"), []byte("caller-owned")); err != nil {
			return err
		}
		if _, err := store.EnqueueTx(t.Context(), txn, EnqueueRequest[string, string]{Payload: "message"}); !errors.Is(err, rejected) {
			t.Fatalf("EnqueueTx: %v", err)
		}
		// A pre-write guard rejection leaves the caller's transaction usable.
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 0 || store.seq.next != next || countKeysWithPrefix(t, db, store.keys.messagePrefix) != 0 {
		t.Fatal("rejection allocated, serialized, or wrote a message")
	}
}

func TestEnqueueGuardAllowsSettlementDuringPressure(t *testing.T) {
	pressure := false
	rejected := errors.New("disk pressure")
	_, store, cleanup := openTestStoreWithOptions(t, "drain-guard", Serde[string, string]{}, Options{EnqueueGuard: func(context.Context) error {
		if pressure {
			return rejected
		}
		return nil
	}})
	defer cleanup()
	id, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "message"})
	if err != nil {
		t.Fatal(err)
	}
	pressure = true
	claimed, err := store.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 3)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %d", err, len(claimed))
	}
	if err := store.acknowledge(t.Context(), id, claimed[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	if countKeysWithPrefix(t, store.db, store.keys.messagePrefix) != 0 {
		t.Fatal("message did not drain")
	}
	if _, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "blocked"}); !errors.Is(err, rejected) {
		t.Fatal(err)
	}
	pressure = false
	if _, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "recovered"}); err != nil {
		t.Fatal(err)
	}
}

func TestEnqueueGuardCancellationBeforeWrites(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	_, store, cleanup := openTestStoreWithOptions(t, "guard-cancel", Serde[string, string]{}, Options{EnqueueGuard: func(context.Context) error { cancel(); return nil }})
	defer cleanup()
	if _, err := store.Enqueue(ctx, EnqueueRequest[string, string]{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if countKeysWithPrefix(t, store.db, store.keys.messagePrefix) != 0 {
		t.Fatal("canceled request persisted")
	}
}

func TestEnqueueGuardRunsBeforeOpeningOwnedTransaction(t *testing.T) {
	_, store, cleanup := openTestStore[string, string](t, "guard-before-snapshot", Serde[string, string]{})
	defer cleanup()
	if _, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "retained"}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	store.opts.EnqueueGuard = func(ctx context.Context) error {
		calls++
		if calls == 1 {
			return store.CompareAndSwapAdmissionLimits(ctx, AdmissionLimits{}, AdmissionLimits{MaxRetainedMessages: 1})
		}
		return nil
	}
	if _, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "blocked"}); !errors.Is(err, ErrAdmissionLimit) {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("enqueue opened a stale snapshot before its guard: %d guard calls", calls)
	}
}
