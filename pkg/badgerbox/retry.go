package badgerbox

import (
	"context"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	defaultNamespace      = "default"
	defaultIDLeaseSize    = 128
	defaultConcurrency    = 4
	defaultClaimBatchSize = 32
	defaultPollInterval   = 250 * time.Millisecond
	defaultLeaseDuration  = 30 * time.Second
	defaultRetryBaseDelay = 1 * time.Second
	defaultRetryMaxDelay  = 1 * time.Minute
	defaultMaxAttempts    = 10
	conflictRetryDelay    = 5 * time.Millisecond
)

func withConflictRetry(ctx context.Context, runtime Runtime, fn func() error) error {
	return withConflictRetryObserved(ctx, runtime, nil, fn)
}

func withConflictRetryObserved(ctx context.Context, runtime Runtime, onRetry func(), fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime == nil {
		runtime = SystemRuntime{}
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn()
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}
		if onRetry != nil {
			onRetry()
		}
		if err := runtime.Sleep(ctx, conflictRetryDelay); err != nil {
			return err
		}
	}
}

func retryDelay(base, max time.Duration, attempt int) time.Duration {
	if attempt <= 1 {
		return base
	}

	delay := base
	for i := 1; i < attempt && delay < max; i++ {
		if delay > max/2 {
			return max
		}
		delay *= 2
	}

	if delay > max {
		return max
	}
	return delay
}
