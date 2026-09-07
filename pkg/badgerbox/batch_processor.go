package badgerbox

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	oteltrace "go.opentelemetry.io/otel/trace"
)

type BatchProcessResult struct {
	// ID identifies the claimed message this result settles.
	ID MessageID
	// Err is nil for successful processing or the per-message processing error.
	Err error
}

// BatchProcessFunc handles a claimed batch of badgerbox messages.
//
// The processor calls the function once per claimed batch. Implementations may
// either process the messages synchronously or schedule asynchronous work and
// return before that work completes. In both cases, they must stream exactly one
// BatchProcessResult per claimed message into results. Sending additional results
// violates the contract and can block after the batch-sized buffer fills.
//
// The function must return when its context is canceled. Run joins function
// invocations before returning; caller-owned asynchronous clients must be flushed
// and closed after Run returns.
//
// The processor owns Badger settlement. It acknowledges, retries, or dead-letters
// records as results arrive; the batch function must not mutate the store. The
// results channel is owned by the processor, is buffered to the batch size, and
// must not be closed by the implementation because asynchronous callbacks may
// still be writing to it after the function returns. Asynchronous senders that
// can outlive the function should select between sending and ctx.Done so batch
// cancellation cannot leave the sender blocked.
//
// If the function returns an error, panics, or the processor context is canceled,
// records that have not yet produced a result are retried. Records that already
// produced a result are settled from that result. This preserves at-least-once
// delivery: explicit successes are acknowledged, unresolved work is retried, and
// late results from a previous attempt are ignored by that attempt's worker.
// A settlement error does not discard other results. Remaining outcomes are
// settled before the joined settlement errors are returned; failed settlements
// retain their database leases for expiry recovery.
// On a terminal batch error, cancellation, or lease expiry, the callback context
// is canceled before settlement. The final drain is limited to results already
// buffered at that point; ongoing duplicate or unknown results cannot extend it.
// Permanent applies only to BatchProcessResult.Err; a function-level error cannot
// assign a permanent disposition to individual records that produced no result.
type BatchProcessFunc[M any, D any] func(ctx context.Context, messages []Message[M, D], results chan<- BatchProcessResult) error

// BatchProcessorOptions adds batch sizing to the shared worker options.
type BatchProcessorOptions struct {
	ProcessorOptions
	ClaimBatchSize int
}

func normalizeBatchProcessorOptions(opts BatchProcessorOptions) BatchProcessorOptions {
	opts.ProcessorOptions = normalizeProcessorOptions(opts.ProcessorOptions)
	if opts.ClaimBatchSize <= 0 {
		opts.ClaimBatchSize = defaultClaimBatchSize
	}
	return opts
}

// BatchProcessor claims ready messages from a Store in batches and delivers each
// batch to a BatchProcessFunc.
//
// ProcessorOptions.Concurrency controls how many batch workers run concurrently.
// BatchProcessorOptions.ClaimBatchSize controls the maximum number of messages claimed
// for one BatchProcessFunc call. Settlement remains per-message even though
// processing is scheduled per-batch.
type BatchProcessor[M any, D any] struct {
	store     *Store[M, D]
	fn        BatchProcessFunc[M, D]
	opts      BatchProcessorOptions
	callbacks sync.WaitGroup
	runMu     sync.Mutex
}

// NewBatchProcessor builds a BatchProcessor for store and fn.
//
// Zero-value options are replaced with package defaults.
func NewBatchProcessor[M any, D any](store *Store[M, D], fn BatchProcessFunc[M, D], opts BatchProcessorOptions) (*BatchProcessor[M, D], error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if fn == nil {
		return nil, ErrProcessorFuncNil
	}

	processor := &BatchProcessor[M, D]{
		store: store,
		fn:    fn,
		opts:  normalizeBatchProcessorOptions(opts),
	}
	return processor, nil
}

func (p *BatchProcessor[M, D]) processBatch(ctx context.Context, work []claimedRecord[M, D]) error {
	if len(work) == 0 {
		return nil
	}

	messages := make([]Message[M, D], len(work))
	for i, record := range work {
		messages[i] = record.Message
	}

	start := p.store.runtime.Now().UTC()
	defer func() {
		duration := positiveDuration(p.store.runtime.Now().UTC().Sub(start))
		p.store.obs.RecordProcessBatch(ctx, len(work), duration)
	}()

	if ctxErr(ctx) != nil || p.resultWaitDuration(work) <= 0 {
		return p.releaseClaimedBatch(ctx, work)
	}
	ctx, endTraces := p.startMessageTraces(ctx, work, start)
	defer endTraces()
	processCtx, cancelProcess := context.WithCancel(contextWithOTelInstrumentation(ctx, p.store.obs))
	defer cancelProcess()
	// The result channel is sized to the claimed batch so producer callbacks can
	// report one late result per claimed message without blocking after this worker
	// stops waiting.
	resultCh := make(chan BatchProcessResult, len(work))
	// The processBatch goroutine exclusively owns pending. The batch function and
	// asynchronous callbacks communicate only through resultCh.
	pending := make(map[MessageID]claimedRecord[M, D], len(work))
	for _, record := range work {
		pending[record.Message.ID] = record
	}
	resultWaitDuration := p.resultWaitDuration(work)
	if resultWaitDuration <= 0 {
		return p.releaseClaimedBatch(ctx, work)
	}

	processDone := make(chan error, 1)
	invoked := make(chan bool, 1)
	p.callbacks.Add(1)
	go func() {
		defer p.callbacks.Done()
		// Commit to invoking the callback only after the final cancellation
		// and lease check. After sending true, always call it, even if canceled.
		if ctxErr(processCtx) != nil || p.resultWaitDuration(work) <= 0 {
			invoked <- false
			return
		}
		invoked <- true
		processDone <- p.invokeBatchProcess(processCtx, messages, resultCh)
	}()

	if !<-invoked {
		return p.releaseClaimedBatch(ctx, work)
	}
	resultWait := time.NewTimer(p.resultWaitDuration(work))
	defer resultWait.Stop()

	var processDoneCh <-chan error = processDone
	var resultInput <-chan BatchProcessResult = resultCh
	processReturned := false
	var settlementErrors []error
	for len(pending) > 0 || !processReturned {
		select {
		case processErr := <-processDoneCh:
			processDoneCh = nil
			processReturned = true
			if processErr != nil {
				cancelProcess()
				drainErr := p.drainAvailableBatchResults(ctx, resultInput, pending, start)
				pendingErr := p.failPendingBatchResults(ctx, pending, start, processErr)
				return errors.Join(append(settlementErrors, drainErr, pendingErr)...)
			}
		case result, ok := <-resultInput:
			if !ok {
				cancelProcess()
				resultInput = nil
				if err := p.failPendingBatchResults(ctx, pending, start, ErrBatchResultMissing); err != nil {
					settlementErrors = append(settlementErrors, err)
				}
				continue
			}
			if err := p.settleBatchResult(ctx, result, pending, start); err != nil {
				settlementErrors = append(settlementErrors, err)
			}
		case <-ctx.Done():
			cancelProcess()
			return errors.Join(append(settlementErrors, p.cancelPendingBatchResults(ctx, resultInput, pending, start))...)
		case <-resultWait.C:
			cancelProcess()
			return errors.Join(append(settlementErrors, p.timeoutPendingBatchResults(ctx, resultInput, pending, start))...)
		}
	}
	cancelProcess()
	return errors.Join(append(settlementErrors, p.drainAvailableBatchResults(ctx, resultInput, pending, start))...)
}

func (p *BatchProcessor[M, D]) invokeBatchProcess(ctx context.Context, messages []Message[M, D], resultCh chan<- BatchProcessResult) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = panicError(recovered)
		}
	}()
	return p.fn(ctx, messages, resultCh)
}

func (p *BatchProcessor[M, D]) resultWaitDuration(work []claimedRecord[M, D]) time.Duration {
	leaseUntil := work[0].LeaseUntil
	for _, record := range work[1:] {
		if record.LeaseUntil.Before(leaseUntil) {
			leaseUntil = record.LeaseUntil
		}
	}
	remaining := leaseUntil.Sub(p.store.runtime.Now().UTC())
	if remaining <= 0 {
		return 0
	}
	return remaining
}

func (p *BatchProcessor[M, D]) cancelPendingBatchResults(ctx context.Context, resultCh <-chan BatchProcessResult, pending map[MessageID]claimedRecord[M, D], started time.Time) error {
	drainErr := p.drainAvailableBatchResults(ctx, resultCh, pending, started)
	return errors.Join(drainErr, p.failPendingBatchResults(ctx, pending, started, ctx.Err()))
}

func (p *BatchProcessor[M, D]) timeoutPendingBatchResults(ctx context.Context, resultCh <-chan BatchProcessResult, pending map[MessageID]claimedRecord[M, D], started time.Time) error {
	drainErr := p.drainAvailableBatchResults(ctx, resultCh, pending, started)
	return errors.Join(drainErr, p.failPendingBatchResults(ctx, pending, started, ErrBatchResultMissing))
}

// drainAvailableBatchResults settles expected results and counts invalid results
// buffered at entry. The worker is the only receiver, so this budget includes all
// available outcomes without following a producer that keeps refilling the
// channel. Remaining pending records are retried by failPendingBatchResults.
func (p *BatchProcessor[M, D]) drainAvailableBatchResults(ctx context.Context, resultCh <-chan BatchProcessResult, pending map[MessageID]claimedRecord[M, D], started time.Time) error {
	var settlementErrors []error
	for range len(resultCh) {
		select {
		case result, ok := <-resultCh:
			if !ok {
				return errors.Join(append(settlementErrors, p.failPendingBatchResults(ctx, pending, started, ErrBatchResultMissing))...)
			}
			if err := p.settleBatchResult(ctx, result, pending, started); err != nil {
				settlementErrors = append(settlementErrors, err)
			}
		default:
			return errors.Join(settlementErrors...)
		}
	}
	return errors.Join(settlementErrors...)
}

// settleBatchResult applies one streamed result to its matching claimed record.
// Unknown IDs and duplicate results cannot safely ack or retry additional
// records, so they are counted and ignored. Any expected records left pending
// are counted once when they are retried. The matching record is settled with a
// context detached from cancellation so a processor shutdown does not strand
// already-completed work in processing state. Remove the result from pending even
// on settlement failure: its DB lease stays intact for recovery, and a duplicate
// result or batch-level failure must not assign it another disposition.
func (p *BatchProcessor[M, D]) settleBatchResult(ctx context.Context, result BatchProcessResult, pending map[MessageID]claimedRecord[M, D], started time.Time) error {
	work, ok := pending[result.ID]
	if !ok {
		p.store.obs.RecordProcessBatchResultInvalid(ctx, 1)
		return nil
	}
	delete(pending, result.ID)
	settlementCtx, cancel := p.batchSettlementContext(ctx)
	defer cancel()
	return p.settleOne(settlementCtx, work, started, result.Err)
}

// failPendingBatchResults retries every claimed record that never produced a
// result. It records missing-result metrics for observability and uses the
// supplied error as the retry cause so callers can distinguish batch-function
// errors, panics, closed result channels, and context cancellation. Each record
// gets one settlement attempt; errors are joined after all attempts, and failed
// records retain their database leases for recovery.
func (p *BatchProcessor[M, D]) failPendingBatchResults(ctx context.Context, pending map[MessageID]claimedRecord[M, D], started time.Time, processErr error) error {
	if len(pending) == 0 {
		return nil
	}
	processErr = markBatchErrorRetryable(processErr)

	settlementCtx, cancel := p.batchSettlementContext(ctx)
	defer cancel()

	p.store.obs.RecordProcessBatchResultMissing(settlementCtx, len(pending))
	var settlementErrors []error
	for id, record := range pending {
		delete(pending, id)
		if err := p.settleOne(settlementCtx, record, started, processErr); err != nil {
			settlementErrors = append(settlementErrors, err)
		}
	}

	return errors.Join(settlementErrors...)
}

// batchSettlementContext strips caller cancellation before Badger settlement and
// replaces it with ProcessorOptions.SettlementTimeout. Once a result has
// arrived, or once pending work must be retried during shutdown, settlement
// should get a bounded chance to complete instead of immediately failing with
// context.Canceled and waiting for lease expiry.
func (p *BatchProcessor[M, D]) batchSettlementContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), p.opts.SettlementTimeout)
}

func (p *BatchProcessor[M, D]) settleOne(ctx context.Context, work claimedRecord[M, D], started time.Time, processErr error) error {
	ctx = ContextForMessage(ctx, work.Message.ID)
	delivery := ctx.Value(messageTracesKey{}).(messageTraces)[work.Message.ID]
	span := delivery.span
	if processErr != nil {
		span.RecordError(processErr)
		result, failErr := p.store.failProcessing(ctx, work.Message.ID, work.LeaseToken, processErr, p.opts.RetryBaseDelay, p.opts.RetryMaxDelay)
		if failErr != nil {
			span.RecordError(failErr)
			delivery.end("error", p.store.runtime.Now().UTC())
			return markSettlementError(boxErrorf("settle message %s: %w", work.Message.ID, failErr))
		}
		p.finishProcessing(ctx, delivery, started, processErr, result)
		return nil
	}

	acknowledged, err := p.store.acknowledgeOwned(ctx, work.Message.ID, work.LeaseToken)
	if err != nil {
		span.RecordError(err)
		delivery.end("error", p.store.runtime.Now().UTC())
		return markSettlementError(boxErrorf("settle message %s: %w", work.Message.ID, err))
	}

	result := failProcessingResult{}
	if acknowledged {
		result.outcome = metricOutcomeSuccess
	}
	p.finishProcessing(ctx, delivery, started, nil, result)
	return nil
}

func (p *BatchProcessor[M, D]) finishProcessing(ctx context.Context, delivery *messageTrace, started time.Time, processErr error, result failProcessingResult) {
	span := delivery.span
	finished := p.store.runtime.Now().UTC()
	duration := positiveDuration(finished.Sub(started))

	switch result.outcome {
	case metricOutcomeSuccess:
		p.store.obs.RecordProcessSuccess(ctx, duration)
	case metricOutcomeRetried:
		p.store.obs.RecordProcessRetried(ctx, processErr, duration)
		p.store.obs.RecordRetryScheduled(ctx, processErr, result.retryDelay)
		span.AddEvent("retry_scheduled", oteltrace.WithAttributes(attribute.String("retry_delay", result.retryDelay.String())))
	case metricOutcomeDeadLetter:
		p.store.obs.RecordProcessDeadLetter(ctx, processErr, duration)
		p.store.obs.RecordDeadLetter(ctx, processErr)
		span.AddEvent("dead_lettered", oteltrace.WithAttributes(attribute.String("failure_kind", failureKind(processErr))))
	}

	delivery.end(result.outcome, finished)
}

func (p *BatchProcessor[M, D]) dispatchLoop(ctx context.Context, notifyCh <-chan struct{}, workCh chan<- []claimedRecord[M, D], workerSlots chan struct{}) error {
	ticker := p.store.runtime.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()

	for {
		if err := p.dispatchAvailable(ctx, workCh, workerSlots); err != nil {
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
func (p *BatchProcessor[M, D]) dispatchAvailable(ctx context.Context, workCh chan<- []claimedRecord[M, D], workerSlots chan struct{}) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-workerSlots:
		}

		claimedAt := p.store.runtime.Now().UTC()
		claimed, effectiveBatchSize, err := p.store.claimReadyBatchWithEffectiveLimit(ctx, claimedAt, p.opts.ClaimBatchSize, p.opts.LeaseDuration, p.opts.MaxAttempts)
		if err != nil {
			workerSlots <- struct{}{}
			return err
		}
		if len(claimed) == 0 {
			workerSlots <- struct{}{}
			return nil
		}
		if err := ctxErr(ctx); err != nil {
			workerSlots <- struct{}{}
			return p.releaseClaimedBatch(ctx, claimed)
		}

		p.store.obs.WorkQueuedBatch(len(claimed))
		select {
		case <-ctx.Done():
			p.store.obs.WorkDequeuedBatch(len(claimed))
			workerSlots <- struct{}{}
			return p.releaseClaimedBatch(ctx, claimed)
		case workCh <- claimed:
		}

		if len(claimed) < effectiveBatchSize {
			return nil
		}
	}
}
func (p *BatchProcessor[M, D]) releaseClaimedBatch(ctx context.Context, work []claimedRecord[M, D]) error {
	if len(work) == 0 {
		return nil
	}

	settlementCtx, cancel := p.batchSettlementContext(ctx)
	defer cancel()
	_, err := p.store.releaseClaimed(settlementCtx, work)
	return markSettlementError(err)
}
func (p *BatchProcessor[M, D]) releaseQueuedBatch(ctx context.Context, work []claimedRecord[M, D]) error {
	if len(work) == 0 {
		return nil
	}

	p.store.obs.WorkDequeuedBatch(len(work))
	return p.releaseClaimedBatch(ctx, work)
}
func (p *BatchProcessor[M, D]) workerLoop(ctx context.Context, workCh <-chan []claimedRecord[M, D], workerSlots chan struct{}) error {
	for {
		select {
		case <-ctx.Done():
			return p.drainQueuedWork(ctx, workCh, workerSlots)
		case work, ok := <-workCh:
			if !ok {
				return nil
			}
			if err := ctxErr(ctx); err != nil {
				releaseErr := p.releaseQueuedBatch(ctx, work)
				workerSlots <- struct{}{}
				return releaseErr
			}
			err := p.processWorkerBatch(ctx, work)
			workerSlots <- struct{}{}
			if err != nil {
				return err
			}
		}
	}
}
func (p *BatchProcessor[M, D]) processWorkerBatch(ctx context.Context, work []claimedRecord[M, D]) error {
	p.store.obs.WorkStartedBatch(len(work))
	defer p.store.obs.WorkFinished()
	return p.processBatch(ctx, work)
}
func (p *BatchProcessor[M, D]) drainQueuedWork(ctx context.Context, workCh <-chan []claimedRecord[M, D], workerSlots chan struct{}) error {
	var releaseErrors []error
	for {
		select {
		case work, ok := <-workCh:
			if !ok {
				return errors.Join(releaseErrors...)
			}
			err := p.releaseQueuedBatch(ctx, work)
			workerSlots <- struct{}{}
			if err != nil {
				releaseErrors = append(releaseErrors, err)
			}
		default:
			return errors.Join(releaseErrors...)
		}
	}
}
func (p *BatchProcessor[M, D]) Run(ctx context.Context) error {
	if !p.runMu.TryLock() {
		return boxErrorf("processor is already running")
	}
	defer p.runMu.Unlock()
	if err := ctxErr(ctx); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := p.store.StartObservability(runCtx); err != nil {
		return err
	}

	notifyCh := make(chan struct{}, 1)
	listenerID := p.store.registerListener(notifyCh)
	defer p.store.unregisterListener(listenerID)

	// workerSlots is the admission limit for durable claims. Each claimed batch
	// owns one removed token until it is processed or released, so buffering
	// workCh decouples dispatch from worker scheduling without allowing more than
	// Concurrency batches to be claimed.
	workCh := make(chan []claimedRecord[M, D], p.opts.Concurrency)
	workerSlots := make(chan struct{}, p.opts.Concurrency)
	for range p.opts.Concurrency {
		workerSlots <- struct{}{}
	}
	errCh := make(chan error, p.opts.Concurrency+2)

	var wg sync.WaitGroup
	start := func(fn func(context.Context) error) {
		wg.Go(func() {
			if err := fn(runCtx); reportableLoopError(runCtx, err) {
				// Each loop sends at most one error; errCh has one slot per loop.
				errCh <- err
				cancel()
			}
		})
	}

	start(func(ctx context.Context) error {
		return p.dispatchLoop(ctx, notifyCh, workCh, workerSlots)
	})
	start(func(ctx context.Context) error {
		return p.reaperLoop(ctx)
	})
	for range p.opts.Concurrency {
		start(func(ctx context.Context) error {
			return p.workerLoop(ctx, workCh, workerSlots)
		})
	}

	var runErrors []error
	select {
	case err := <-errCh:
		runErrors = append(runErrors, err)
	case <-ctx.Done():
	}
	cancel()

	wg.Wait()
	p.callbacks.Wait()
	runErrors = append(runErrors, p.drainQueuedWork(runCtx, workCh, workerSlots))
	close(errCh)
	for err := range errCh {
		runErrors = append(runErrors, err)
	}
	return errors.Join(runErrors...)
}
func reportableLoopError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	var settlement settlementError
	if errors.As(err, &settlement) || ctx == nil || ctx.Err() == nil {
		return true
	}
	return !onlyContextErrors(err)
}

// Check every branch of a joined error: cancellation in one branch cannot hide
// a storage or application failure in another.
func onlyContextErrors(err error) bool {
	switch wrapped := err.(type) {
	case interface{ Unwrap() []error }:
		children := wrapped.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if child != nil && !onlyContextErrors(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		if cause := wrapped.Unwrap(); cause != nil {
			return onlyContextErrors(cause)
		}
	}
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
func (p *BatchProcessor[M, D]) reaperLoop(ctx context.Context) error {
	ticker := p.store.runtime.NewTicker(p.opts.PollInterval)
	defer ticker.Stop()
	for {
		if _, err := p.store.requeueExpired(ctx, p.store.runtime.Now().UTC(), p.opts.RequeuePageSize); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.Chan():
		}
	}
}

func (p *BatchProcessor[M, D]) startMessageTraces(ctx context.Context, work []claimedRecord[M, D], start time.Time) (context.Context, func()) {
	traces := make(messageTraces, len(work))
	for _, record := range work {
		messageCtx, span := p.store.obs.StartProcessSpan(ctx, record.Message.ID, record.Message.Attempt, record.Message.MaxAttempts, record.Message.CreatedAt, record.Message.AvailableAt, record.TraceCarrier, oteltrace.WithTimestamp(start))
		traces[record.Message.ID] = newMessageTrace(messageCtx, span)
	}
	ctx = context.WithValue(ctx, messageTracesKey{}, traces)
	return ctx, func() {
		for _, delivery := range traces {
			delivery.end("", p.store.runtime.Now().UTC())
		}
	}
}
