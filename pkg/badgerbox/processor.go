package badgerbox

import (
	"context"
	"sync"
	"time"
)

type Processor[M any, D any] struct {
	store *Store[M, D]
	fn    ProcessFunc[M, D]
	opts  ProcessorOptions
}

type claimedRecord[M any, D any] struct {
	Message    Message[M, D]
	LeaseToken string
}

func NewProcessor[M any, D any](store *Store[M, D], fn ProcessFunc[M, D], opts ProcessorOptions) (*Processor[M, D], error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if fn == nil {
		return nil, ErrProcessorFuncNil
	}

	return &Processor[M, D]{
		store: store,
		fn:    fn,
		opts:  normalizeProcessorOptions(opts),
	}, nil
}

func (p *Processor[M, D]) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

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
	ticker := time.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()

	for {
		if err := p.dispatchAvailable(ctx, workCh); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-notifyCh:
		}
	}
}

func (p *Processor[M, D]) dispatchAvailable(ctx context.Context, workCh chan<- claimedRecord[M, D]) error {
	for {
		claimed, err := p.store.claimReadyBatch(ctx, time.Now().UTC(), p.opts.ClaimBatchSize, p.opts.LeaseDuration, p.opts.MaxAttempts)
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
			}
		}

		if len(claimed) < p.opts.ClaimBatchSize {
			return nil
		}
	}
}

func (p *Processor[M, D]) reaperLoop(ctx context.Context) error {
	ticker := time.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()

	for {
		if _, err := p.store.requeueExpired(ctx, time.Now().UTC()); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *Processor[M, D]) workerLoop(ctx context.Context, workCh <-chan claimedRecord[M, D]) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case work := <-workCh:
			if err := p.processOne(ctx, work); err != nil {
				return err
			}
		}
	}
}

func (p *Processor[M, D]) processOne(ctx context.Context, work claimedRecord[M, D]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = p.store.failProcessing(ctx, work.Message.ID, work.LeaseToken, panicError(recovered), p.opts.RetryBaseDelay, p.opts.RetryMaxDelay)
		}
	}()

	if err = p.fn(ctx, work.Message); err != nil {
		return p.store.failProcessing(ctx, work.Message.ID, work.LeaseToken, err, p.opts.RetryBaseDelay, p.opts.RetryMaxDelay)
	}

	return p.store.acknowledge(ctx, work.Message.ID, work.LeaseToken)
}
