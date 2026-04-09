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

func normalizeOptions(opts Options) Options {
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	if opts.IDLeaseSize == 0 {
		opts.IDLeaseSize = defaultIDLeaseSize
	}
	return opts
}

func normalizeProcessorOptions(opts ProcessorOptions) ProcessorOptions {
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultConcurrency
	}
	if opts.ClaimBatchSize <= 0 {
		opts.ClaimBatchSize = defaultClaimBatchSize
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultPollInterval
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = defaultLeaseDuration
	}
	if opts.RetryBaseDelay <= 0 {
		opts.RetryBaseDelay = defaultRetryBaseDelay
	}
	if opts.RetryMaxDelay <= 0 {
		opts.RetryMaxDelay = defaultRetryMaxDelay
	}
	if opts.RetryMaxDelay < opts.RetryBaseDelay {
		opts.RetryMaxDelay = opts.RetryBaseDelay
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = defaultMaxAttempts
	}
	return opts
}

func withConflictRetry(ctx context.Context, fn func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := fn()
		if !errors.Is(err, badger.ErrConflict) {
			return err
		}

		timer := time.NewTimer(conflictRetryDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
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
