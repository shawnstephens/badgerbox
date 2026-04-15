package badgerbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestWithConflictRetryRetries(t *testing.T) {
	t.Parallel()

	runtime := newFakeRuntime(time.Unix(100, 0))
	var attempts int

	err := withConflictRetry(context.Background(), runtime, func() error {
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

	sleepCalls := runtime.SleepCalls()
	if len(sleepCalls) != 2 {
		t.Fatalf("sleep calls = %d, want 2", len(sleepCalls))
	}
	for _, delay := range sleepCalls {
		if delay != conflictRetryDelay {
			t.Fatalf("sleep delay = %v, want %v", delay, conflictRetryDelay)
		}
	}
}

func TestWithConflictRetryObservedHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtime := newFakeRuntime(time.Unix(100, 0))
	runtime.sleepFunc = func(ctx context.Context, _ time.Duration) error {
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}

	var attempts, retries int
	err := withConflictRetryObserved(ctx, runtime, func() {
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
