package badgerbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestPermanentErrorHelpers(t *testing.T) {
	t.Parallel()

	root := errors.New("boom")
	if got := Permanent(nil); got != nil {
		t.Fatalf("Permanent(nil) = %v, want nil", got)
	}

	err := Permanent(root)
	if !IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = false, want true", err)
	}
	if IsPermanent(root) {
		t.Fatalf("IsPermanent(%v) = true, want false", root)
	}
	if !errors.Is(err, root) {
		t.Fatalf("errors.Is(%v, %v) = false, want true", err, root)
	}
	if errors.Unwrap(err) != root {
		t.Fatalf("errors.Unwrap(%v) = %v, want %v", err, errors.Unwrap(err), root)
	}

	var target permanentError
	if !errors.As(err, &target) {
		t.Fatalf("errors.As(%v, permanentError) = false, want true", err)
	}
	if target.Unwrap() != root {
		t.Fatalf("target.Unwrap() = %v, want %v", target.Unwrap(), root)
	}
}

func TestParseMessageKey(t *testing.T) {
	t.Parallel()

	keys := newKeyspace("parse")
	id := MessageID(42)

	got, err := parseMessageKey(keys.messagePrefix, keys.messageKey(id))
	if err != nil {
		t.Fatalf("parseMessageKey: %v", err)
	}
	if got != id {
		t.Fatalf("message id = %d, want %d", got, id)
	}

	if _, err := parseMessageKey(keys.messagePrefix, keys.messagePrefix); err == nil {
		t.Fatal("expected invalid message key length error")
	}

	otherKeys := newKeyspace("other")
	if _, err := parseMessageKey(keys.messagePrefix, otherKeys.messageKey(id)); err == nil {
		t.Fatal("expected invalid message key prefix error")
	}
}

func TestParseTimeAndIDKey(t *testing.T) {
	t.Parallel()

	keys := newKeyspace("parse")
	id := MessageID(7)
	at := time.Unix(100, 99).UTC()

	gotAt, gotID, err := parseTimeAndIDKey(keys.readyPrefix, keys.readyKey(at, id))
	if err != nil {
		t.Fatalf("parseTimeAndIDKey: %v", err)
	}
	if !gotAt.Equal(at) || gotID != id {
		t.Fatalf("parsed = (%v, %d), want (%v, %d)", gotAt, gotID, at, id)
	}

	invalidSeparator := append([]byte(nil), keys.readyKey(at, id)...)
	invalidSeparator[len(keys.readyPrefix)+8] = '.'
	if _, _, err := parseTimeAndIDKey(keys.readyPrefix, invalidSeparator); err == nil {
		t.Fatal("expected invalid indexed key separator error")
	}
}

func TestWithConflictRetryRetries(t *testing.T) {
	t.Parallel()

	var attempts, sleeps int
	err := withConflictRetry(context.Background(), runtimeDeps{
		sleep: func(_ context.Context, delay time.Duration) error {
			sleeps++
			if delay != conflictRetryDelay {
				t.Fatalf("sleep delay = %v, want %v", delay, conflictRetryDelay)
			}
			return nil
		},
	}, func() error {
		attempts++
		if attempts < 3 {
			return badger.ErrConflict
		}
		return nil
	})
	if err != nil {
		t.Fatalf("withConflictRetry: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if sleeps != 2 {
		t.Fatalf("sleeps = %d, want 2", sleeps)
	}
}

func TestWithConflictRetryObservedHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attempts, retries int
	err := withConflictRetryObserved(ctx, runtimeDeps{
		sleep: func(ctx context.Context, _ time.Duration) error {
			cancel()
			<-ctx.Done()
			return ctx.Err()
		},
	}, func() {
		retries++
	}, func() error {
		attempts++
		return badger.ErrConflict
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	if retries != 1 {
		t.Fatalf("retries = %d, want 1", retries)
	}
}

func TestRetryDelayClamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 1, want: 10 * time.Millisecond},
		{attempt: 2, want: 20 * time.Millisecond},
		{attempt: 3, want: 25 * time.Millisecond},
		{attempt: 4, want: 25 * time.Millisecond},
	}

	for _, tt := range tests {
		if got := retryDelay(10*time.Millisecond, 25*time.Millisecond, tt.attempt); got != tt.want {
			t.Fatalf("retryDelay(attempt=%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestSleepContext(t *testing.T) {
	t.Parallel()

	if err := sleepContext(nil, 0); err != nil {
		t.Fatalf("sleepContext(nil, 0) = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepContext(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleepContext canceled err = %v, want %v", err, context.Canceled)
	}
}

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

func TestStoreInjectedDepsAffectClaimLifecycle(t *testing.T) {
	t.Parallel()

	_, store, cleanup := openTestStore[testPayload, testDestination](t, "deps-claim", Serde[testPayload, testDestination]{})
	defer cleanup()

	fixedNow := time.Unix(1_700_000_000, 0).UTC()
	store.deps.now = func() time.Time { return fixedNow }
	store.deps.newLeaseToken = func() (string, error) { return "lease-token", nil }

	id, err := store.Enqueue(context.Background(), EnqueueRequest[testPayload, testDestination]{
		Payload:     testPayload{Name: "timed"},
		Destination: testDestination{Route: "/timed"},
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	claimed, err := store.claimReadyBatch(context.Background(), store.deps.now(), 1, 10*time.Second, defaultMaxAttempts)
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

func TestFailureKindAndPositiveDuration(t *testing.T) {
	t.Parallel()

	if got := positiveDuration(-time.Second); got != 0 {
		t.Fatalf("positiveDuration(-1s) = %v, want 0", got)
	}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "nil", err: nil, want: ""},
		{name: "permanent", err: Permanent(errors.New("stop")), want: "permanent"},
		{name: "canceled", err: context.Canceled, want: "context"},
		{name: "deadline", err: context.DeadlineExceeded, want: "context"},
		{name: "generic", err: errors.New("boom"), want: "error"},
	}

	for _, tt := range tests {
		if got := failureKind(tt.err); got != tt.want {
			t.Fatalf("%s failureKind = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestRecordSnapshotReturnsQueueSnapshotError(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	expected := errors.New("snapshot failed")
	var calls int

	obs, err := newOTelInstrumentation(ObservabilityOptions{MeterProvider: provider}, "orders", func(context.Context) (queueSnapshot, error) {
		calls++
		if calls == 1 {
			return queueSnapshot{}, nil
		}
		return queueSnapshot{}, expected
	})
	if err != nil {
		t.Fatalf("newOTelInstrumentation: %v", err)
	}
	defer obs.close()

	if err := obs.recordSnapshot(context.Background()); !errors.Is(err, expected) {
		t.Fatalf("recordSnapshot err = %v, want %v", err, expected)
	}
}
