package badgerbox

import (
	"context"
	"time"
)

type ProcessFunc[M any, D any] func(context.Context, Message[M, D]) error
type ProcessorOptions struct {
	// ClaimMaxBytes limits the sum of stored source values read by one claim.
	// Zero disables this limit; negative values are invalid. Badger metadata is
	// checked before copying or decoding. Values larger than the entire limit
	// enter referenced quarantine without being loaded; healthy records continue.
	// This is a source-byte budget, not a Go heap or RSS limit: account for JSON,
	// codec expansion, callback copies, and Concurrency when sizing memory.
	ClaimMaxBytes     int64
	Concurrency       int
	PollInterval      time.Duration
	LeaseDuration     time.Duration
	RetryBaseDelay    time.Duration
	RetryMaxDelay     time.Duration
	MaxAttempts       int
	RequeuePageSize   int
	SettlementTimeout time.Duration
}

type claimedRecord[M any, D any] struct {
	Message      Message[M, D]
	LeaseToken   string
	LeaseUntil   time.Time
	TraceCarrier map[string]string
}

func validateProcessorOptions(opts ProcessorOptions) error {
	if opts.ClaimMaxBytes < 0 {
		return boxErrorf("ClaimMaxBytes must be nonnegative; zero disables the limit")
	}

	for _, option := range []struct {
		name     string
		negative bool
	}{
		{"Concurrency", opts.Concurrency < 0},
		{"PollInterval", opts.PollInterval < 0},
		{"LeaseDuration", opts.LeaseDuration < 0},
		{"RetryBaseDelay", opts.RetryBaseDelay < 0},
		{"RetryMaxDelay", opts.RetryMaxDelay < 0},
		{"MaxAttempts", opts.MaxAttempts < 0},
		{"RequeuePageSize", opts.RequeuePageSize < 0},
		{"SettlementTimeout", opts.SettlementTimeout < 0},
	} {
		if option.negative {
			return boxErrorf("%s must be nonnegative; zero selects the default", option.name)
		}
	}
	base := opts.RetryBaseDelay
	if base == 0 {
		base = defaultRetryBaseDelay
	}
	if opts.RetryMaxDelay > 0 && base > opts.RetryMaxDelay {
		return boxErrorf("RetryMaxDelay must be at least RetryBaseDelay")
	}
	return nil
}

func normalizeProcessorOptions(opts ProcessorOptions) ProcessorOptions {
	if opts.Concurrency <= 0 {
		opts.Concurrency = defaultConcurrency
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
	if opts.RequeuePageSize <= 0 {
		opts.RequeuePageSize = defaultRequeuePageSize
	}
	if opts.SettlementTimeout <= 0 {
		opts.SettlementTimeout = 10 * time.Second
	}
	return opts
}

// Processor reserves exactly one message for each available worker.
type Processor[M any, D any] struct{ batch *BatchProcessor[M, D] }

func NewProcessor[M any, D any](store *Store[M, D], fn ProcessFunc[M, D], opts ProcessorOptions) (*Processor[M, D], error) {
	if fn == nil {
		return nil, ErrProcessorFuncNil
	}
	batch, err := NewBatchProcessor(store, func(ctx context.Context, messages []Message[M, D], results chan<- BatchProcessResult) error {
		for _, msg := range messages {
			results <- BatchProcessResult{ID: msg.ID, Err: invokeOne(ContextForMessage(ctx, msg.ID), fn, msg)}
		}
		return nil
	}, BatchProcessorOptions{ProcessorOptions: opts, ClaimBatchSize: 1})
	if err != nil {
		return nil, err
	}
	return &Processor[M, D]{batch: batch}, nil
}
func invokeOne[M any, D any](ctx context.Context, fn ProcessFunc[M, D], msg Message[M, D]) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	return fn(ctx, msg)
}
func (p *Processor[M, D]) Run(ctx context.Context) error { return p.batch.Run(ctx) }
