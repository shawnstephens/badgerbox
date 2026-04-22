package badgerbox

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type ProcessFunc[M any, D any] func(ctx context.Context, msg Message[M, D]) error

type ProcessorOptions struct {
	Concurrency    int
	ClaimBatchSize int
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
	MaxAttempts    int
}

type Processor[M any, D any] struct {
	store *Store[M, D]
	fn    ProcessFunc[M, D]
	opts  ProcessorOptions
}

type claimedRecord[M any, D any] struct {
	Message      Message[M, D]
	LeaseToken   string
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
	return opts
}

func NewProcessor[M any, D any](store *Store[M, D], fn ProcessFunc[M, D], opts ProcessorOptions) (*Processor[M, D], error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if fn == nil {
		return nil, ErrProcessorFuncNil
	}

	processor := &Processor[M, D]{
		store: store,
		fn:    fn,
		opts:  normalizeProcessorOptions(opts),
	}
	return processor, nil
}

func (p *Processor[M, D]) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := p.store.StartObservability(runCtx); err != nil {
		return err
	}

	notifyCh := make(chan struct{}, 1)
	listenerID := p.store.registerListener(notifyCh)
	defer p.store.unregisterListener(listenerID)

	workCh := make(chan claimedRecord[M, D], p.opts.Concurrency*p.opts.ClaimBatchSize)
	errCh := make(chan error, p.opts.Concurrency+2)

	var wg sync.WaitGroup
	start := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(runCtx); err != nil && ctxErr(runCtx) == nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}

	start(func(ctx context.Context) error {
		return p.dispatchLoop(ctx, notifyCh, workCh)
	})
	start(func(ctx context.Context) error {
		return p.reaperLoop(ctx)
	})
	for i := 0; i < p.opts.Concurrency; i++ {
		start(func(ctx context.Context) error {
			return p.workerLoop(ctx, workCh)
		})
	}

	var err error
	select {
	case err = <-errCh:
		cancel()
	case <-ctx.Done():
		cancel()
	}

	wg.Wait()
	if err != nil {
		return err
	}
	return nil
}

func (p *Processor[M, D]) dispatchLoop(ctx context.Context, notifyCh <-chan struct{}, workCh chan<- claimedRecord[M, D]) error {
	ticker := p.store.runtime.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()

	for {
		if err := p.dispatchAvailable(ctx, workCh); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.Chan():
		case <-notifyCh:
		}
	}
}

func (p *Processor[M, D]) dispatchAvailable(ctx context.Context, workCh chan<- claimedRecord[M, D]) error {
	for {
		claimed, err := p.store.claimReadyBatch(ctx, p.store.runtime.Now().UTC(), p.opts.ClaimBatchSize, p.opts.LeaseDuration, p.opts.MaxAttempts)
		if err != nil {
			return err
		}
		if len(claimed) == 0 {
			return nil
		}

		for _, record := range claimed {
			select {
			case <-ctx.Done():
				return nil
			case workCh <- record:
				p.store.obs.workQueued()
			}
		}

		if len(claimed) < p.opts.ClaimBatchSize {
			return nil
		}
	}
}

func (p *Processor[M, D]) reaperLoop(ctx context.Context) error {
	ticker := p.store.runtime.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()

	for {
		if _, err := p.store.requeueExpired(ctx, p.store.runtime.Now().UTC()); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.Chan():
		}
	}
}

func (p *Processor[M, D]) workerLoop(ctx context.Context, workCh <-chan claimedRecord[M, D]) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case work := <-workCh:
			p.store.obs.workStarted()
			if err := p.processOne(ctx, work); err != nil {
				p.store.obs.workFinished()
				return err
			}
			p.store.obs.workFinished()
		}
	}
}

func (p *Processor[M, D]) processOne(ctx context.Context, work claimedRecord[M, D]) (err error) {
	ctx, span := p.store.obs.startProcessSpan(ctx, work.Message.ID, work.Message.Attempt, work.Message.MaxAttempts, work.Message.CreatedAt, work.Message.AvailableAt, work.TraceCarrier)
	start := p.store.runtime.Now().UTC()

	defer func() {
		if recovered := recover(); recovered != nil {
			processErr := panicError(recovered)
			span.AddEvent("panic_recovered")
			span.RecordError(processErr)
			result, failErr := p.store.failProcessing(ctx, work.Message.ID, work.LeaseToken, processErr, p.opts.RetryBaseDelay, p.opts.RetryMaxDelay)
			if failErr != nil {
				err = failErr
				span.RecordError(failErr)
				p.store.obs.endSpan(span, "error")
				return
			}
			p.finishProcessing(ctx, span, start, processErr, result)
			err = nil
		}
	}()

	if err = p.fn(ctx, work.Message); err != nil {
		span.RecordError(err)
		result, failErr := p.store.failProcessing(ctx, work.Message.ID, work.LeaseToken, err, p.opts.RetryBaseDelay, p.opts.RetryMaxDelay)
		if failErr != nil {
			span.RecordError(failErr)
			p.store.obs.endSpan(span, "error")
			return failErr
		}
		p.finishProcessing(ctx, span, start, err, result)
		return nil
	}

	if err = p.store.acknowledge(ctx, work.Message.ID, work.LeaseToken); err != nil {
		span.RecordError(err)
		p.store.obs.endSpan(span, "error")
		return err
	}

	p.finishProcessing(ctx, span, start, nil, failProcessingResult{outcome: metricOutcomeSuccess})
	return nil
}

func (p *Processor[M, D]) finishProcessing(ctx context.Context, span oteltrace.Span, started time.Time, processErr error, result failProcessingResult) {
	duration := positiveDuration(p.store.runtime.Now().UTC().Sub(started))

	switch result.outcome {
	case metricOutcomeRetried:
		p.store.obs.recordProcessRetried(ctx, processErr, duration)
		p.store.obs.recordRetryScheduled(ctx, processErr, result.retryDelay)
		span.AddEvent("retry_scheduled", oteltrace.WithAttributes(attribute.String("badgerbox.retry_delay", result.retryDelay.String())))
	case metricOutcomeDeadLetter:
		p.store.obs.recordProcessDeadLetter(ctx, processErr, duration)
		p.store.obs.recordDeadLetter(ctx, processErr)
		span.AddEvent("dead_lettered", oteltrace.WithAttributes(attribute.String("badgerbox.failure_kind", failureKind(processErr))))
	default:
		p.store.obs.recordProcessSuccess(ctx, duration)
	}

	p.store.obs.endSpan(span, result.outcome)
}
