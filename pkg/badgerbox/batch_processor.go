package badgerbox

import (
	"context"
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
// Permanent applies only to BatchProcessResult.Err; a function-level error cannot
// assign a permanent disposition to individual records that produced no result.
type BatchProcessFunc[M any, D any] func(ctx context.Context, messages []Message[M, D], results chan<- BatchProcessResult) error

// BatchProcessor claims ready messages from a Store in batches and delivers each
// batch to a BatchProcessFunc.
//
// ProcessorOptions.Concurrency controls how many batch workers run concurrently.
// ProcessorOptions.ClaimBatchSize controls the maximum number of messages claimed
// for one BatchProcessFunc call. Settlement remains per-message even though
// processing is scheduled per-batch.
type BatchProcessor[M any, D any] struct {
	store *Store[M, D]
	fn    BatchProcessFunc[M, D]
	opts  ProcessorOptions
}

// NewBatchProcessor builds a BatchProcessor for store and fn.
//
// Zero-value options are replaced with package defaults.
func NewBatchProcessor[M any, D any](store *Store[M, D], fn BatchProcessFunc[M, D], opts ProcessorOptions) (*BatchProcessor[M, D], error) {
	if store == nil {
		return nil, ErrNilStore
	}
	if fn == nil {
		return nil, ErrProcessorFuncNil
	}

	processor := &BatchProcessor[M, D]{
		store: store,
		fn:    fn,
		opts:  normalizeProcessorOptions(opts),
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
		p.store.obs.recordProcessBatch(ctx, len(work), duration)
	}()

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
		return p.failPendingBatchResults(ctx, pending, start, ErrBatchResultMissing)
	}

	processDone := make(chan error, 1)
	go func() {
		processDone <- p.invokeBatchProcess(processCtx, messages, resultCh)
	}()

	resultWait := time.NewTimer(resultWaitDuration)
	defer resultWait.Stop()

	var processDoneCh <-chan error = processDone
	var resultInput <-chan BatchProcessResult = resultCh
	processReturned := false
	for len(pending) > 0 || !processReturned {
		select {
		case processErr := <-processDoneCh:
			processDoneCh = nil
			processReturned = true
			if processErr != nil {
				if err := p.drainAvailableBatchResults(ctx, resultInput, pending, start); err != nil {
					return err
				}
				return p.failPendingBatchResults(ctx, pending, start, processErr)
			}
		case result, ok := <-resultInput:
			if !ok {
				resultInput = nil
				if err := p.failPendingBatchResults(ctx, pending, start, ErrBatchResultMissing); err != nil {
					return err
				}
				continue
			}
			if err := p.settleBatchResult(ctx, result, pending, start); err != nil {
				return err
			}
		case <-ctx.Done():
			return p.cancelPendingBatchResults(ctx, resultInput, pending, start)
		case <-resultWait.C:
			return p.timeoutPendingBatchResults(ctx, resultInput, pending, start)
		}
	}
	return p.drainAvailableBatchResults(ctx, resultInput, pending, start)
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
	if err := p.drainAvailableBatchResults(ctx, resultCh, pending, started); err != nil {
		return err
	}
	return p.failPendingBatchResults(ctx, pending, started, ctx.Err())
}

func (p *BatchProcessor[M, D]) timeoutPendingBatchResults(ctx context.Context, resultCh <-chan BatchProcessResult, pending map[MessageID]claimedRecord[M, D], started time.Time) error {
	if err := p.drainAvailableBatchResults(ctx, resultCh, pending, started); err != nil {
		return err
	}
	return p.failPendingBatchResults(ctx, pending, started, ErrBatchResultMissing)
}

// drainAvailableBatchResults settles expected results and counts invalid results
// that were emitted before a batch-level error, panic, cancellation, timeout, or
// successful synchronous function return was observed. Remaining pending records
// are retried by failPendingBatchResults with the batch-level error.
func (p *BatchProcessor[M, D]) drainAvailableBatchResults(ctx context.Context, resultCh <-chan BatchProcessResult, pending map[MessageID]claimedRecord[M, D], started time.Time) error {
	for {
		select {
		case result, ok := <-resultCh:
			if !ok {
				if len(pending) > 0 {
					return p.failPendingBatchResults(ctx, pending, started, ErrBatchResultMissing)
				}
				return nil
			}
			if err := p.settleBatchResult(ctx, result, pending, started); err != nil {
				return err
			}
		default:
			return nil
		}
	}
}

// settleBatchResult applies one streamed result to its matching claimed record.
// Unknown IDs and duplicate results cannot safely ack or retry additional
// records, so they are counted and ignored. Any expected records left pending
// are counted once when they are retried. The matching record is settled with a
// context detached from cancellation so a processor shutdown does not strand
// already-completed work in processing state.
func (p *BatchProcessor[M, D]) settleBatchResult(ctx context.Context, result BatchProcessResult, pending map[MessageID]claimedRecord[M, D], started time.Time) error {
	work, ok := pending[result.ID]
	if !ok {
		p.store.obs.recordProcessBatchResultInvalid(ctx, 1)
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
// errors, panics, closed result channels, and context cancellation.
func (p *BatchProcessor[M, D]) failPendingBatchResults(ctx context.Context, pending map[MessageID]claimedRecord[M, D], started time.Time, processErr error) error {
	if len(pending) == 0 {
		return nil
	}
	processErr = markBatchErrorRetryable(processErr)

	settlementCtx, cancel := p.batchSettlementContext(ctx)
	defer cancel()

	p.store.obs.recordProcessBatchResultMissing(settlementCtx, len(pending))
	for id, record := range pending {
		if err := p.settleOne(settlementCtx, record, started, processErr); err != nil {
			return err
		}
		delete(pending, id)
	}

	return nil
}

// batchSettlementContext strips caller cancellation before Badger settlement and
// replaces it with ProcessorOptions.BatchSettlementTimeout. Once a result has
// arrived, or once pending work must be retried during shutdown, settlement
// should get a bounded chance to complete instead of immediately failing with
// context.Canceled and waiting for lease expiry.
func (p *BatchProcessor[M, D]) batchSettlementContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), p.opts.BatchSettlementTimeout)
}

func (p *BatchProcessor[M, D]) settleOne(ctx context.Context, work claimedRecord[M, D], started time.Time, processErr error) error {
	ctx, span := p.store.obs.startProcessSpan(ctx, work.Message.ID, work.Message.Attempt, work.Message.MaxAttempts, work.Message.CreatedAt, work.Message.AvailableAt, work.TraceCarrier, oteltrace.WithTimestamp(started))
	if processErr != nil {
		span.RecordError(processErr)
		result, failErr := p.store.failProcessing(ctx, work.Message.ID, work.LeaseToken, processErr, p.opts.RetryBaseDelay, p.opts.RetryMaxDelay)
		if failErr != nil {
			span.RecordError(failErr)
			p.store.obs.endSpan(span, "error", oteltrace.WithTimestamp(p.store.runtime.Now().UTC()))
			return failErr
		}
		p.finishProcessing(ctx, span, started, processErr, result)
		return nil
	}

	acknowledged, err := p.store.acknowledgeOwned(ctx, work.Message.ID, work.LeaseToken)
	if err != nil {
		span.RecordError(err)
		p.store.obs.endSpan(span, "error", oteltrace.WithTimestamp(p.store.runtime.Now().UTC()))
		return err
	}

	result := failProcessingResult{}
	if acknowledged {
		result.outcome = metricOutcomeSuccess
	}
	p.finishProcessing(ctx, span, started, nil, result)
	return nil
}

func (p *BatchProcessor[M, D]) finishProcessing(ctx context.Context, span oteltrace.Span, started time.Time, processErr error, result failProcessingResult) {
	finished := p.store.runtime.Now().UTC()
	duration := positiveDuration(finished.Sub(started))

	switch result.outcome {
	case metricOutcomeSuccess:
		p.store.obs.recordProcessSuccess(ctx, duration)
	case metricOutcomeRetried:
		p.store.obs.recordProcessRetried(ctx, processErr, duration)
		p.store.obs.recordRetryScheduled(ctx, processErr, result.retryDelay)
		span.AddEvent("retry_scheduled", oteltrace.WithAttributes(attribute.String("retry_delay", result.retryDelay.String())))
	case metricOutcomeDeadLetter:
		p.store.obs.recordProcessDeadLetter(ctx, processErr, duration)
		p.store.obs.recordDeadLetter(ctx, processErr)
		span.AddEvent("dead_lettered", oteltrace.WithAttributes(attribute.String("failure_kind", failureKind(processErr))))
	}

	p.store.obs.endSpan(span, result.outcome, oteltrace.WithTimestamp(finished))
}
