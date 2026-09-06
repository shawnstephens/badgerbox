package badgerbox

import (
	"context"
	"time"
)

type ProcessFunc[M any, D any] func(context.Context, Message[M, D]) error
type ProcessorOptions struct {
	Concurrency            int
	ClaimBatchSize         int
	PollInterval           time.Duration
	LeaseDuration          time.Duration
	RetryBaseDelay         time.Duration
	RetryMaxDelay          time.Duration
	MaxAttempts            int
	RequeuePageSize        int
	BatchSettlementTimeout time.Duration
}

type claimedRecord[M any, D any] struct {
	Message      Message[M, D]
	LeaseToken   string
	LeaseUntil   time.Time
	TraceCarrier map[string]string
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
	if opts.RequeuePageSize <= 0 {
		opts.RequeuePageSize = defaultRequeuePageSize
	}
	if opts.BatchSettlementTimeout <= 0 {
		opts.BatchSettlementTimeout = 10 * time.Second
	}
	return opts
}

// Processor adapts a per-message function to the batch engine. Each worker
// invokes the function sequentially for the messages in its claimed batch.
type Processor[M any, D any] struct{ batch *BatchProcessor[M, D] }

func NewProcessor[M any, D any](store *Store[M, D], fn ProcessFunc[M, D], opts ProcessorOptions) (*Processor[M, D], error) {
	if fn == nil {
		return nil, ErrProcessorFuncNil
	}
	batch, err := NewBatchProcessor(store, func(ctx context.Context, messages []Message[M, D], results chan<- BatchProcessResult) error {
		for _, msg := range messages {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			results <- BatchProcessResult{ID: msg.ID, Err: invokeOne(ctx, fn, msg)}
		}
		return nil
	}, opts)
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
