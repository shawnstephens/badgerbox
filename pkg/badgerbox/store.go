package badgerbox

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
)

const (
	recordStatusPending    = MessageStateReady
	recordStatusProcessing = MessageStateProcessing
)

var emptyValue = []byte{}

type Options struct {
	Namespace     string
	IDLeaseSize   uint64
	Observability ObservabilityOptions
	Runtime       Runtime
	// EnqueueGuard runs before allocation, serialization, or transaction writes
	// for every enqueue attempt, including EnqueueTx. It must be concurrency-safe
	// and respect context cancellation. Returning an error rejects new intake;
	// settlement and dead-letter recovery do not invoke it. This advisory hook
	// does not reserve external resources or roll back caller-owned writes.
	EnqueueGuard func(context.Context) error
	// AdmissionLimits must match persisted namespace limits when reopening.
	AdmissionLimits AdmissionLimits
}

type Store[M any, D any] struct {
	db      *badger.DB
	serde   Serde[M, D]
	opts    Options
	keys    keyspace
	seq     *messageSequence
	obs     *otelInstrumentation
	runtime Runtime
	closed  atomic.Bool

	admissionIdentity [16]byte

	closeOnce sync.Once
	closeErr  error

	listenerMu   sync.Mutex
	nextListener int
	listeners    map[int]chan struct{}
}

type storedRecord struct {
	ID               MessageID         `json:"id"`
	PayloadBytes     []byte            `json:"payload_bytes"`
	DestinationBytes []byte            `json:"destination_bytes"`
	TraceCarrier     map[string]string `json:"trace_carrier,omitempty"`
	CreatedAtUnix    int64             `json:"created_at_unix_nano"`
	AvailableAtUnix  int64             `json:"available_at_unix_nano"`
	Attempt          int               `json:"attempt"`
	MaxAttempts      int               `json:"max_attempts"`
	Status           MessageState      `json:"status"`
	LeaseToken       string            `json:"lease_token,omitempty"`
	LeaseUntilUnix   int64             `json:"lease_until_unix_nano"`
}

type storedDeadLetter struct {
	Record            storedRecord             `json:"record"`
	QuarantinedSource *storedQuarantinedSource `json:"quarantined_source,omitempty"`
	FailedAt          int64                    `json:"failed_at_unix_nano"`
	Error             string                   `json:"error"`
	Permanent         bool                     `json:"permanent"`
}

type enqueueResult struct {
	id     MessageID
	record storedRecord
}

type failProcessingResult struct {
	outcome    string
	retryDelay time.Duration
}

func normalizeOptions(opts Options) Options {
	if opts.Namespace == "" {
		opts.Namespace = defaultNamespace
	}
	if opts.IDLeaseSize == 0 {
		opts.IDLeaseSize = defaultIDLeaseSize
	}
	if opts.Runtime == nil {
		opts.Runtime = SystemRuntime{}
	}
	opts.Observability = normalizeObservabilityOptions(opts.Observability)
	return opts
}

func normalizedAvailableAt(now time.Time, availableAt time.Time) time.Time {
	if availableAt.IsZero() {
		return now.UTC()
	}
	return availableAt.UTC()
}

func New[M any, D any](db *badger.DB, serde Serde[M, D], opts Options) (*Store[M, D], error) {
	if db == nil {
		return nil, ErrNilDB
	}

	if !db.Opts().DetectConflicts {
		return nil, ErrConflictDetectionRequired
	}

	opts = normalizeOptions(opts)
	serde = normalizeSerde(serde)

	if strings.Contains(opts.Namespace, "/") {
		return nil, ErrInvalidNamespace
	}
	store := &Store[M, D]{db: db, serde: serde, opts: opts, keys: newKeyspace(opts.Namespace), runtime: opts.Runtime, listeners: make(map[int]chan struct{})}
	if err := store.initializeQueueState(); err != nil {
		return nil, err
	}
	var seq *messageSequence
	err := withConflictRetry(context.Background(), store.runtime, func() error {
		var err error
		seq, err = newMessageSequence(db, store.keys.sequenceKey, opts.IDLeaseSize)
		return err
	})
	if err != nil {
		return nil, err
	}
	store.seq = seq

	store.obs, err = newOTelInstrumentation(opts.Observability, opts.Namespace, store.queueSnapshot)
	if err != nil {
		_ = seq.Release()
		return nil, err
	}

	return store, nil
}

func (s *Store[M, D]) Close() error {
	s.closeOnce.Do(func() {
		if s.obs != nil {
			s.closeErr = s.obs.Close()
		}
		s.closed.Store(true)
		if s.seq != nil {
			s.closeErr = errors.Join(s.closeErr, withConflictRetry(context.Background(), s.runtime, s.seq.Release))
		}
	})
	return s.closeErr
}

func (s *Store[M, D]) StartObservability(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	return s.obs.Start(ctx, func(d time.Duration) (<-chan time.Time, func()) { t := s.runtime.NewTicker(d); return t.Chan(), t.Stop }, s.opts.Observability.PollInterval)
}

func (s *Store[M, D]) RecordObservabilitySnapshot(ctx context.Context) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	return s.obs.RecordSnapshot(ctx)
}

// Enqueue returns ErrMessageTooLarge before queue writes when the encoded record
// cannot fit its later lifecycle transitions under the current Badger options.
func (s *Store[M, D]) Enqueue(ctx context.Context, req EnqueueRequest[M, D]) (MessageID, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}

	start := s.runtime.Now().UTC()
	availableAt := normalizedAvailableAt(start, req.AvailableAt)
	traceCtx, traceSpan, traceCarrier := s.obs.StartEnqueueSpan(ctx, availableAt, defaultMaxAttempts)

	var result enqueueResult
	err := withConflictRetryObserved(ctx, s.runtime, func() {
		s.obs.RecordConflictRetry(traceCtx)
		traceSpan.AddEvent("conflict_retry")
	}, func() error {
		// Wait for external capacity before opening a snapshot that could hold
		// back Badger's version reclamation during a slow filesystem probe.
		if err := s.checkEnqueueGuard(ctx); err != nil {
			return err
		}
		return s.db.Update(func(txn *badger.Txn) error {
			var updateErr error
			result, updateErr = s.enqueueTx(ctx, txn, req, defaultMaxAttempts, traceCarrier)
			return updateErr
		})
	})
	if err != nil {
		traceSpan.RecordError(err)
		s.obs.EndSpan(traceSpan, "error")
		return 0, err
	}

	s.obs.SetMessageSpanAttributes(traceSpan, result.record.ID, result.record.Attempt, result.record.MaxAttempts, time.Unix(0, result.record.CreatedAtUnix).UTC(), time.Unix(0, result.record.AvailableAtUnix).UTC())
	s.obs.EndSpan(traceSpan, metricOutcomeCommitted)
	s.obs.RecordEnqueueCommitted(traceCtx, positiveDuration(s.runtime.Now().UTC().Sub(start)))
	s.notifyListeners()
	return result.id, nil
}

// EnqueueTx checks lifecycle and namespace admission budgets before adding queue
// entries to txn. The transaction must belong to this Store's database. Prepared
// messages consume capacity only if the caller successfully commits the entire
// transaction; conflicts require retrying the whole application transaction.
// As with Badger writes, a storage error can leave partial pending writes: the
// caller must abort the entire transaction whenever EnqueueTx returns an error.
// EnqueueGuard runs while this caller-owned transaction is open; a bounded or
// cached guard avoids holding its read snapshot during a slow external check.
func (s *Store[M, D]) EnqueueTx(ctx context.Context, txn *badger.Txn, req EnqueueRequest[M, D]) (MessageID, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	if err := ctxErr(ctx); err != nil {
		return 0, err
	}

	start := s.runtime.Now().UTC()
	availableAt := normalizedAvailableAt(start, req.AvailableAt)
	traceCtx, traceSpan, traceCarrier := s.obs.StartEnqueueSpan(ctx, availableAt, defaultMaxAttempts)

	var result enqueueResult
	var err error
	if txn == nil {
		err = ErrNilTxn
	} else {
		err = s.checkEnqueueGuard(ctx)
	}
	if err == nil {
		result, err = s.enqueueTx(ctx, txn, req, defaultMaxAttempts, traceCarrier)
	}
	if err != nil {
		traceSpan.RecordError(err)
		s.obs.EndSpan(traceSpan, "error")
		return 0, err
	}

	s.obs.SetMessageSpanAttributes(traceSpan, result.record.ID, result.record.Attempt, result.record.MaxAttempts, time.Unix(0, result.record.CreatedAtUnix).UTC(), time.Unix(0, result.record.AvailableAtUnix).UTC())
	s.obs.EndSpan(traceSpan, metricOutcomePrepared)
	s.obs.RecordEnqueuePrepared(traceCtx, positiveDuration(s.runtime.Now().UTC().Sub(start)))
	return result.id, nil
}

func (s *Store[M, D]) Get(ctx context.Context, id MessageID) (Message[M, D], error) {
	var message Message[M, D]
	if err := s.ensureOpen(); err != nil {
		return message, err
	}
	if err := ctxErr(ctx); err != nil {
		return message, err
	}

	err := s.db.View(func(txn *badger.Txn) error {
		record, err := s.loadRecord(txn, id)
		if err != nil {
			return err
		}

		message, err = s.recordToMessage(record)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return message, ErrNotFound
	}

	return message, err
}

func (s *Store[M, D]) ListDeadLetters(ctx context.Context, limit int, cursor []byte) ([]DeadLetter[M, D], []byte, error) {
	return s.ListDeadLettersWithOptions(ctx, DeadLetterListOptions{Limit: limit, Cursor: cursor})
}

func (s *Store[M, D]) ListDeadLettersWithOptions(ctx context.Context, options DeadLetterListOptions) ([]DeadLetter[M, D], []byte, error) {
	if err := s.ensureOpen(); err != nil {
		return nil, nil, err
	}
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	if options.MaxBytes < 0 {
		return nil, nil, boxErrorf("dead-letter page max bytes must be nonnegative")
	}
	limit, cursor := options.Limit, options.Cursor
	if len(cursor) > 0 {
		if _, _, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, cursor); err != nil {
			return nil, nil, err
		}
	}
	if limit <= 0 {
		return nil, nil, nil
	}

	deadLetters := make([]DeadLetter[M, D], 0, limit)
	var nextCursor []byte
	var lastKey []byte
	remaining := options.MaxBytes

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		start := s.keys.deadLetterPrefix
		if len(cursor) > 0 {
			start = cloneBytes(cursor)
		}

		skipCurrent := len(cursor) > 0
		for it.Seek(start); it.ValidForPrefix(s.keys.deadLetterPrefix); it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}

			key := it.Item().KeyCopy(nil)
			if skipCurrent && bytes.Equal(key, cursor) {
				skipCurrent = false
				continue
			}
			skipCurrent = false

			// ValueSize reads metadata, not the value. In particular, reject an
			// oversized first record without allocating or decoding its payload.
			if options.MaxBytes > 0 {
				size := storedValueUpperBound(it.Item())
				if size > remaining {
					if len(deadLetters) == 0 {
						return ErrDeadLetterTooLarge
					}
					nextCursor = lastKey
					break
				}
				remaining -= size
			}
			value, err := copyStoredValue(it.Item())
			if err != nil {
				return err
			}

			stored, err := decodeStoredDeadLetter(value)
			if err != nil {
				return err
			}
			failedAt, id, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, key)
			if err != nil {
				return err
			}
			if stored.QuarantinedSource != nil {
				source, err := s.referencedSourceItem(txn, key, stored)
				if err != nil {
					return err
				}
				if options.MaxBytes > 0 {
					if storedValueUpperBound(source) > remaining {
						if len(deadLetters) == 0 {
							return ErrDeadLetterTooLarge
						}
						nextCursor = lastKey
						break
					}
					remaining -= storedValueUpperBound(source)
				}
				stored.Record, err = decodeRecordItem(source, id)
				if err != nil {
					return err
				}
				if err := validateQuarantineSource(stored.Record, stored.QuarantinedSource); err != nil {
					return err
				}
			} else if stored.Record.ID != id || stored.FailedAt != failedAt.UnixNano() {
				return boxErrorf("dead-letter key disagrees with record")
			}
			deadLetter, err := s.deadLetterToMessage(stored)
			if err != nil {
				return err
			}

			deadLetters = append(deadLetters, deadLetter)
			lastKey = key
			if len(deadLetters) == limit {
				it.Next()
				if it.ValidForPrefix(s.keys.deadLetterPrefix) {
					nextCursor = key
				}
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return deadLetters, nextCursor, nil
}

func (s *Store[M, D]) RequeueDeadLetter(ctx context.Context, id MessageID, failedAt time.Time, at time.Time) error {
	return s.RequeueDeadLetterWithOptions(ctx, id, failedAt, DeadLetterRequeueOptions{AvailableAt: at})
}

// RequeueDeadLetterWithOptions checks the stored size before loading an exact record.
func (s *Store[M, D]) RequeueDeadLetterWithOptions(ctx context.Context, id MessageID, failedAt time.Time, options DeadLetterRequeueOptions) error {
	if options.MaxBytes < 0 {
		return boxErrorf("requeue max bytes must be nonnegative")
	}
	at := options.AvailableAt
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	if failedAt.IsZero() {
		return boxErrorf("failed_at is required")
	}

	failedAt = failedAt.UTC()
	if at.IsZero() {
		at = s.runtime.Now().UTC()
	} else {
		at = at.UTC()
	}

	var requeued bool
	err := withConflictRetryObserved(ctx, s.runtime, func() {
		s.obs.RecordConflictRetry(ctx)
	}, func() error {
		requeued = false
		return s.db.Update(func(txn *badger.Txn) error {
			if err := ctxErr(ctx); err != nil {
				return err
			}

			key := s.keys.deadLetterKey(failedAt, id)
			item, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrNotFound
			}
			if err != nil {
				return err
			}

			if options.MaxBytes > 0 && storedValueUpperBound(item) > options.MaxBytes {
				return ErrDeadLetterTooLarge
			}

			value, err := copyStoredValue(item)
			if err != nil {
				return err
			}

			deadLetter, unmarshalErr := decodeStoredDeadLetter(value)
			if unmarshalErr != nil {
				return unmarshalErr
			}

			record := deadLetter.Record
			if deadLetter.QuarantinedSource != nil {
				source, err := s.referencedSourceItem(txn, key, deadLetter)
				if err != nil {
					return err
				}
				if options.MaxBytes > 0 && storedValueUpperBound(source) > options.MaxBytes-storedValueUpperBound(item) {
					return ErrDeadLetterTooLarge
				}
				record, err = decodeRecordItem(source, id)
				if err != nil {
					return err
				}
				if err := validateQuarantineSource(record, deadLetter.QuarantinedSource); err != nil {
					return err
				}
				if _, err := txn.Get(s.keys.readyKey(time.Unix(0, record.AvailableAtUnix), id)); err == nil {
					return fmt.Errorf("%w: quarantined source still scheduled", ErrInconsistentIndex)
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			} else {
				if record.ID != id || deadLetter.FailedAt != failedAt.UnixNano() {
					return boxErrorf("dead-letter key disagrees with record")
				}
				if _, err := txn.Get(s.keys.messageKey(id)); err == nil {
					return ErrLiveMessageExists
				} else if !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
			}
			record.Attempt = 0
			record.Status = recordStatusPending
			record.LeaseToken = ""
			record.LeaseUntilUnix = 0
			record.AvailableAtUnix = at.UnixNano()

			encoded, err := json.Marshal(record)
			if err != nil {
				return err
			}
			if err := s.validateRecordSize(record, len(encoded)); err != nil {
				return err
			}
			if err := txn.Set(s.keys.messageKey(record.ID), encoded); err != nil {
				return err
			}
			if err := txn.Set(s.keys.readyKey(at, record.ID), emptyValue); err != nil {
				return err
			}
			if err := txn.Set(s.keys.readyCreatedKey(time.Unix(0, record.CreatedAtUnix).UTC(), record.ID), emptyValue); err != nil {
				return err
			}

			if err := txn.Delete(key); err != nil {
				return err
			}
			if deadLetter.QuarantinedSource != nil {
				if err := txn.Delete(s.keys.quarantineKey(id)); err != nil {
					return err
				}
			}

			requeued = true
			return nil
		})
	})
	if err != nil {
		return err
	}
	if requeued {
		s.obs.RecordManualRequeue(ctx)
		s.notifyListeners()
	}
	return nil
}

func (s *Store[M, D]) checkEnqueueGuard(ctx context.Context) error {
	if s.opts.EnqueueGuard != nil {
		if err := s.opts.EnqueueGuard(ctx); err != nil {
			return fmt.Errorf("enqueue guard: %w", err)
		}
	}
	return ctxErr(ctx)
}

func (s *Store[M, D]) enqueueTx(ctx context.Context, txn *badger.Txn, req EnqueueRequest[M, D], maxAttempts int, traceCarrier map[string]string) (enqueueResult, error) {
	var result enqueueResult
	if txn == nil {
		return result, ErrNilTxn
	}
	if err := ctxErr(ctx); err != nil {
		return result, err
	}

	nextID, err := s.seq.Next()
	if err != nil {
		return result, err
	}
	id := MessageID(nextID)

	now := s.runtime.Now().UTC()
	availableAt := normalizedAvailableAt(now, req.AvailableAt)

	payloadBytes, err := s.serde.Message.Marshal(req.Payload)
	if err != nil {
		return result, err
	}
	destinationBytes, err := s.serde.Destination.Marshal(req.Destination)
	if err != nil {
		return result, err
	}

	record := storedRecord{
		ID:               id,
		PayloadBytes:     payloadBytes,
		DestinationBytes: destinationBytes,
		TraceCarrier:     cloneStringMap(traceCarrier),
		CreatedAtUnix:    now.UnixNano(),
		AvailableAtUnix:  availableAt.UnixNano(),
		Attempt:          0,
		MaxAttempts:      maxAttempts,
		Status:           recordStatusPending,
	}

	encodedRecord, err := json.Marshal(record)
	if err != nil {
		return result, err
	}

	if err := s.validateRecordSize(record, len(encodedRecord)); err != nil {
		return result, err
	}
	usage, err := s.prepareAdmission(txn, record)
	if err != nil {
		return result, err
	}
	if err := txn.Set(s.keys.messageKey(id), encodedRecord); err != nil {
		return result, err
	}
	if err := txn.Set(s.keys.readyKey(availableAt, id), emptyValue); err != nil {
		return result, err
	}

	if err := txn.Set(s.keys.readyCreatedKey(now, id), emptyValue); err != nil {
		return result, err
	}

	if err := s.storeAdmissionState(txn, usage); err != nil {
		return result, err
	}

	result = enqueueResult{
		id:     id,
		record: record,
	}
	return result, nil
}

func (s *Store[M, D]) claimReadyBatch(ctx context.Context, now time.Time, batchSize int, leaseDuration time.Duration, maxAttempts int) ([]claimedRecord[M, D], error) {
	claimed, _, err := s.claimReadyBatchWithEffectiveLimit(ctx, now, batchSize, leaseDuration, maxAttempts)
	return claimed, err
}

func (s *Store[M, D]) acknowledge(ctx context.Context, id MessageID, leaseToken string) error {
	return withConflictRetryObserved(ctx, s.runtime, func() {
		s.obs.RecordConflictRetry(ctx)
	}, func() error {
		return s.db.Update(func(txn *badger.Txn) error {
			record, err := s.loadRecord(txn, id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusProcessing || record.LeaseToken != leaseToken {
				return nil
			}

			if err := s.releaseAdmission(txn, record); err != nil {
				return err
			}

			if err := txn.Delete(s.keys.messageKey(id)); err != nil {
				return err
			}
			if err := txn.Delete(s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			createdAt := time.Unix(0, record.CreatedAtUnix).UTC()
			if err := txn.Delete(s.keys.processingCreatedKey(createdAt, id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			return nil
		})
	})
}

func (s *Store[M, D]) failProcessing(ctx context.Context, id MessageID, leaseToken string, processErr error, retryBase, retryMax time.Duration) (failProcessingResult, error) {
	var result failProcessingResult
	if processErr == nil {
		processErr = boxErrorf("process error is nil")
	}
	processErrMessage := storedFailureText(processErr)
	processErrPermanent := IsPermanent(processErr)
	now := s.runtime.Now().UTC()
	err := withConflictRetryObserved(ctx, s.runtime, func() {
		s.obs.RecordConflictRetry(ctx)
	}, func() error {
		attemptResult := failProcessingResult{}
		err := s.db.Update(func(txn *badger.Txn) error {
			record, err := s.loadRecord(txn, id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusProcessing || record.LeaseToken != leaseToken {
				return nil
			}

			processingKey := s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), id)

			if processErrPermanent || record.Attempt >= record.MaxAttempts {
				dlq := storedDeadLetter{
					Record:    record,
					FailedAt:  now.UnixNano(),
					Error:     processErrMessage,
					Permanent: processErrPermanent,
				}
				encodedDeadLetter, err := json.Marshal(dlq)
				if err != nil {
					return err
				}

				if err := txn.Set(s.keys.deadLetterKey(now, id), encodedDeadLetter); err != nil {
					return err
				}
				if err := txn.Delete(s.keys.messageKey(id)); err != nil {
					return err
				}
				if err := txn.Delete(processingKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				createdAt := time.Unix(0, record.CreatedAtUnix).UTC()
				if err := txn.Delete(s.keys.processingCreatedKey(createdAt, id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}

				attemptResult.outcome = metricOutcomeDeadLetter
				return nil
			}

			delay := retryDelay(retryBase, retryMax, record.Attempt)
			record.Status = recordStatusPending
			record.LeaseToken = ""
			record.LeaseUntilUnix = 0
			record.AvailableAtUnix = now.Add(delay).UnixNano()

			if err := s.storeRecord(txn, record); err != nil {
				return err
			}
			if err := txn.Delete(processingKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if err := txn.Set(s.keys.readyKey(time.Unix(0, record.AvailableAtUnix).UTC(), id), emptyValue); err != nil {
				return err
			}
			createdAt := time.Unix(0, record.CreatedAtUnix).UTC()
			if err := txn.Delete(s.keys.processingCreatedKey(createdAt, id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			if err := txn.Set(s.keys.readyCreatedKey(createdAt, id), emptyValue); err != nil {
				return err
			}

			attemptResult.outcome = metricOutcomeRetried
			attemptResult.retryDelay = delay
			return nil
		})
		if err != nil {
			return err
		}
		result = attemptResult
		return nil
	})
	if err != nil {
		return failProcessingResult{}, err
	}
	if result.outcome == metricOutcomeRetried {
		s.notifyListeners()
	}
	return result, nil
}

func (s *Store[M, D]) requeueExpired(ctx context.Context, now time.Time, pageSizes ...int) (int, error) {
	pageSize := defaultRequeuePageSize
	if len(pageSizes) > 0 {
		pageSize = pageSizes[0]
	}
	var requeued int
	now = now.UTC()
	candidates, err := s.collectExpiredProcessingCandidates(ctx, now, pageSize)
	if err != nil {
		return 0, err
	}
	defer func() {
		if requeued > 0 {
			s.obs.RecordExpiredLeaseRequeue(ctx, requeued)
			s.notifyListeners()
		}
	}()

	for _, candidate := range candidates {
		candidateRequeued, err := s.requeueExpiredCandidate(ctx, now, candidate)
		if err != nil {
			return requeued, err
		}
		if candidateRequeued {
			requeued++
		}
	}
	return requeued, nil
}

func (s *Store[M, D]) ensureOpen() error {
	if s.closed.Load() {
		return ErrStoreClosed
	}
	return nil
}

func (s *Store[M, D]) loadRecord(txn *badger.Txn, id MessageID) (storedRecord, error) {
	if key, err := s.quarantineDLQKey(txn, id); err != nil {
		return storedRecord{}, err
	} else if key != nil {
		return storedRecord{}, ErrMessageQuarantined
	}
	item, err := txn.Get(s.keys.messageKey(id))
	if err != nil {
		return storedRecord{}, err
	}
	return decodeRecordItem(item, id)
}

func decodeRecordItem(item *badger.Item, id MessageID) (storedRecord, error) {
	var record storedRecord
	value, err := copyStoredValue(item)
	if err != nil {
		return record, err
	}
	record, err = decodeStoredRecord(value)
	if err != nil {
		return storedRecord{}, fmt.Errorf("badgerbox: message %s: %w", id, err)
	}
	if record.ID != id {
		return record, fmt.Errorf("badgerbox: record identity differs from key %d", id)
	}
	return record, nil
}

func (s *Store[M, D]) storeRecord(txn *badger.Txn, record storedRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return txn.Set(s.keys.messageKey(record.ID), encoded)
}

func (s *Store[M, D]) recordToMessage(record storedRecord) (Message[M, D], error) {
	var zero Message[M, D]

	payload, err := decodeWithCodec(s.serde.Message, record.PayloadBytes, "payload")
	if err != nil {
		return zero, err
	}
	destination, err := decodeWithCodec(s.serde.Destination, record.DestinationBytes, "destination")
	if err != nil {
		return zero, err
	}

	return Message[M, D]{
		ID:          record.ID,
		Payload:     payload,
		Destination: destination,
		CreatedAt:   time.Unix(0, record.CreatedAtUnix).UTC(),
		AvailableAt: time.Unix(0, record.AvailableAtUnix).UTC(),
		Attempt:     record.Attempt,
		MaxAttempts: record.MaxAttempts,
		State:       record.Status,
	}, nil
}

func (s *Store[M, D]) deadLetterToMessage(encoded storedDeadLetter) (DeadLetter[M, D], error) {
	var result DeadLetter[M, D]
	message, err := s.recordToMessage(encoded.Record)
	if err != nil {
		return result, err
	}

	return DeadLetter[M, D]{
		Message:   message,
		FailedAt:  time.Unix(0, encoded.FailedAt).UTC(),
		Error:     encoded.Error,
		Permanent: encoded.Permanent,
	}, nil
}

func normalizeSerde[M any, D any](serde Serde[M, D]) Serde[M, D] {
	if serde.Message == nil {
		serde.Message = JSONCodec[M]{}
	}
	if serde.Destination == nil {
		serde.Destination = JSONCodec[D]{}
	}
	return serde
}

func (s *Store[M, D]) registerListener(ch chan struct{}) int {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()

	id := s.nextListener
	s.nextListener++
	s.listeners[id] = ch
	return id
}

func (s *Store[M, D]) unregisterListener(id int) {
	s.listenerMu.Lock()
	defer s.listenerMu.Unlock()
	delete(s.listeners, id)
}

func (s *Store[M, D]) notifyListeners() {
	s.listenerMu.Lock()
	listeners := make([]chan struct{}, 0, len(s.listeners))
	for _, listener := range s.listeners {
		listeners = append(listeners, listener)
	}
	s.listenerMu.Unlock()

	for _, listener := range listeners {
		select {
		case listener <- struct{}{}:
		default:
		}
	}
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	return ctx.Err()
}

func (s *Store[M, D]) claimReadyBatchWithEffectiveLimit(ctx context.Context, now time.Time, batchSize int, leaseDuration time.Duration, maxAttempts int) ([]claimedRecord[M, D], int, error) {
	return s.claimReadyBatchWithLimits(ctx, now, batchSize, 0, leaseDuration, maxAttempts)
}

func (s *Store[M, D]) claimReadyBatchWithLimits(ctx context.Context, now time.Time, batchSize int, maxBytes int64, leaseDuration time.Duration, maxAttempts int) ([]claimedRecord[M, D], int, error) {
	if maxBytes < 0 {
		return nil, 0, boxErrorf("claim max bytes must be nonnegative")
	}
	if batchSize < 0 {
		return nil, 0, boxErrorf("claim batch size must be non-negative: %d", batchSize)
	}
	if leaseDuration <= 0 {
		leaseDuration = defaultLeaseDuration
	}
	if maxAttempts <= 0 {
		maxAttempts = defaultMaxAttempts
	}

	claimed := make([]claimedRecord[M, D], 0, min(batchSize, defaultClaimBatchSize))
	effectiveBatchSize := batchSize
	var quarantined []error
	var byteLimited bool
	now = now.UTC()

	for {
		err := withConflictRetryObserved(ctx, s.runtime, func() {
			s.obs.RecordConflictRetry(ctx)
		}, func() error {
			clear(claimed)
			claimed = claimed[:0]
			quarantined = quarantined[:0]
			byteLimited = false
			var readBytes int64
			disposedIDs := make(map[MessageID]bool)
			return s.db.Update(func(txn *badger.Txn) error {
				opts := badger.DefaultIteratorOptions
				opts.PrefetchValues = false
				it := txn.NewIterator(opts)
				defer it.Close()
				examined := 0
				for it.Seek(s.keys.readyPrefix); it.ValidForPrefix(s.keys.readyPrefix) && examined < effectiveBatchSize; it.Next() {
					if err := ctxErr(ctx); err != nil {
						return err
					}

					key := it.Item().KeyCopy(nil)
					availableAt, id, err := parseTimeAndIDKey(s.keys.readyPrefix, key)
					if err != nil {
						return err
					}
					if availableAt.After(now) {
						break
					}
					examined++

					if disposedIDs[id] {
						if err := txn.Delete(key); err != nil {
							return err
						}
						continue
					}
					if marker, err := s.quarantineDLQKey(txn, id); err != nil {
						return err
					} else if marker != nil {
						// A stale scheduling index cannot reactivate quarantine.
						if err := txn.Delete(key); err != nil {
							return err
						}
						continue
					}
					item, err := txn.Get(s.keys.messageKey(id))
					if errors.Is(err, badger.ErrKeyNotFound) {
						deleteErr := txn.Delete(key)
						if deleteErr != nil {
							return deleteErr
						}
						continue
					}
					if err != nil {
						return err
					}

					size := storedValueUpperBound(item)
					if maxBytes > 0 {
						if size > maxBytes {
							if err := s.quarantineOversizedRecord(txn, id, availableAt, now, size, maxBytes, item.Version()); err != nil {
								return err
							}
							disposedIDs[id] = true
							quarantined = append(quarantined, claimSizeError(size, maxBytes))
							continue
						}
						if size > maxBytes-readBytes {
							byteLimited = true
							break
						}
						readBytes += size
					}
					record, err := decodeRecordItem(item, id)
					if err != nil {
						return err
					}

					if record.Status != recordStatusPending {
						deleteErr := txn.Delete(key)
						if deleteErr != nil {
							return deleteErr
						}
						continue
					}
					if record.AvailableAtUnix != availableAt.UnixNano() {
						deleteErr := txn.Delete(key)
						if deleteErr != nil {
							return deleteErr
						}
						setErr := txn.Set(s.keys.readyKey(time.Unix(0, record.AvailableAtUnix).UTC(), id), emptyValue)
						if setErr != nil {
							return setErr
						}
						continue
					}

					record.Attempt++
					record.MaxAttempts = maxAttempts
					record.Status = recordStatusProcessing
					record.LeaseUntilUnix = now.Add(leaseDuration).UnixNano()

					token, err := s.runtime.NewLeaseToken()
					if err != nil {
						return err
					}
					if len(token) == 0 || len(token) > maxLeaseTokenBytes {
						return boxErrorf("runtime lease token must contain 1 to %d bytes", maxLeaseTokenBytes)
					}
					if !utf8.ValidString(token) {
						return boxErrorf("runtime lease token must be valid UTF-8")
					}
					record.LeaseToken = token

					message, err := s.recordToMessage(record)
					if err != nil {
						if err := s.quarantineReadyRecord(txn, record, now, err); err != nil {
							return err
						}
						disposedIDs[id] = true
						quarantined = append(quarantined, err)
						continue
					}

					storeErr := s.storeRecord(txn, record)
					if storeErr != nil {
						return storeErr
					}
					deleteErr := txn.Delete(key)
					if deleteErr != nil {
						return deleteErr
					}
					setErr := txn.Set(s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), record.ID), []byte(token))
					if setErr != nil {
						return setErr
					}
					createdAt := time.Unix(0, record.CreatedAtUnix).UTC()
					deleteErr = txn.Delete(s.keys.readyCreatedKey(createdAt, record.ID))
					if deleteErr != nil && !errors.Is(deleteErr, badger.ErrKeyNotFound) {
						return deleteErr
					}
					setErr = txn.Set(s.keys.processingCreatedKey(createdAt, record.ID), emptyValue)
					if setErr != nil {
						return setErr
					}
					disposedIDs[id] = true
					claimed = append(claimed, claimedRecord[M, D]{
						Message:      message,
						LeaseToken:   token,
						LeaseUntil:   time.Unix(0, record.LeaseUntilUnix).UTC(),
						TraceCarrier: cloneStringMap(record.TraceCarrier),
					})
				}

				return nil
			})
		})

		if !errors.Is(err, badger.ErrTxnTooBig) {
			if err != nil {
				return nil, effectiveBatchSize, err
			}
			break
		}

		if effectiveBatchSize <= 1 {
			s.obs.RecordClaimTransactionTooBig(ctx, 0)
			return nil, effectiveBatchSize, err
		}
		retryBatchSize := max(1, effectiveBatchSize/2)
		s.obs.RecordClaimTransactionTooBig(ctx, retryBatchSize)
		effectiveBatchSize = retryBatchSize
	}

	for _, err := range quarantined {
		s.obs.RecordDeadLetter(ctx, err)
	}
	if len(quarantined) > 0 {
		// Quarantine may consume the scan page before a healthy record is found.
		// Wake the dispatcher immediately instead of waiting for the poll interval.
		s.notifyListeners()
	}
	if len(claimed) > 0 {
		s.obs.RecordClaimBatch(ctx, len(claimed))
		for _, record := range claimed {
			s.obs.RecordClaimTiming(ctx, positiveDuration(now.Sub(record.Message.AvailableAt)), positiveDuration(now.Sub(record.Message.CreatedAt)))
		}
	}

	if byteLimited && len(claimed) > 0 {
		// A smaller batch caused by bytes is full for dispatch purposes; continue
		// immediately when another worker slot is available, without a poll delay.
		effectiveBatchSize = len(claimed)
	}
	return claimed, effectiveBatchSize, nil
}
func (s *Store[M, D]) collectExpiredProcessingCandidates(ctx context.Context, now time.Time, pageSize int) ([]expiredProcessingCandidate, error) {
	if pageSize <= 0 {
		pageSize = defaultRequeuePageSize
	}
	candidates := make([]expiredProcessingCandidate, 0, pageSize)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(s.keys.processingPrefix); it.ValidForPrefix(s.keys.processingPrefix) && len(candidates) < pageSize; it.Next() {
			if err := ctxErr(ctx); err != nil {
				return err
			}
			key := it.Item().KeyCopy(nil)
			leaseUntil, id, err := parseTimeAndIDKey(s.keys.processingPrefix, key)
			if err != nil {
				return err
			}
			if leaseUntil.After(now) {
				break
			}
			candidates = append(candidates, expiredProcessingCandidate{
				key:        key,
				leaseUntil: leaseUntil,
				id:         id,
			})
		}
		return nil
	})
	return candidates, err
}
func (s *Store[M, D]) requeueExpiredCandidate(ctx context.Context, now time.Time, candidate expiredProcessingCandidate) (bool, error) {
	var requeued bool
	err := withConflictRetryObserved(ctx, s.runtime, func() {
		s.obs.RecordConflictRetry(ctx)
	}, func() error {
		requeued = false
		return s.db.Update(func(txn *badger.Txn) error {
			item, err := txn.Get(candidate.key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			tokenBytes, err := copyStoredValue(item)
			if err != nil {
				return err
			}

			record, err := s.loadRecord(txn, candidate.id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return txn.Delete(candidate.key)
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusProcessing || record.LeaseToken != string(tokenBytes) || record.LeaseUntilUnix != candidate.leaseUntil.UnixNano() {
				return txn.Delete(candidate.key)
			}

			record.Status = recordStatusPending
			record.LeaseToken = ""
			record.LeaseUntilUnix = 0
			record.AvailableAtUnix = now.UnixNano()
			if err := s.storeRecord(txn, record); err != nil {
				return err
			}
			if err := txn.Delete(candidate.key); err != nil {
				return err
			}
			if err := txn.Set(s.keys.readyKey(now, candidate.id), emptyValue); err != nil {
				return err
			}
			createdAt := time.Unix(0, record.CreatedAtUnix).UTC()
			if err := txn.Delete(s.keys.processingCreatedKey(createdAt, candidate.id)); err != nil {
				return err
			}
			if err := txn.Set(s.keys.readyCreatedKey(createdAt, candidate.id), emptyValue); err != nil {
				return err
			}
			requeued = true
			return nil
		})
	})
	return requeued, err
}

type expiredProcessingCandidate struct {
	key        []byte
	leaseUntil time.Time
	id         MessageID
}

func (s *Store[M, D]) acknowledgeOwned(ctx context.Context, id MessageID, leaseToken string) (bool, error) {
	return s.acknowledgeUsingUpdate(ctx, id, leaseToken, s.db.Update)
}
func (s *Store[M, D]) acknowledgeUsingUpdate(ctx context.Context, id MessageID, leaseToken string, update func(func(*badger.Txn) error) error) (bool, error) {
	var acknowledged bool
	err := withConflictRetryObserved(ctx, s.runtime, func() {
		s.obs.RecordConflictRetry(ctx)
	}, func() error {
		acknowledged = false
		err := update(func(txn *badger.Txn) error {
			record, err := s.loadRecord(txn, id)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			if record.Status != recordStatusProcessing || record.LeaseToken != leaseToken {
				return nil
			}

			if err := s.releaseAdmission(txn, record); err != nil {
				return err
			}

			if err := txn.Delete(s.keys.messageKey(id)); err != nil {
				return err
			}
			if err := txn.Delete(s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			createdAt := time.Unix(0, record.CreatedAtUnix).UTC()
			if err := txn.Delete(s.keys.processingCreatedKey(createdAt, id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}

			acknowledged = true
			return nil
		})
		return err
	})
	if err != nil {
		return false, err
	}
	return acknowledged, nil
}

func (s *Store[M, D]) releaseClaimed(ctx context.Context, work []claimedRecord[M, D]) (int, error) {
	if len(work) == 0 {
		return 0, nil
	}

	var released int
	err := withConflictRetryObserved(ctx, s.runtime, func() {
		s.obs.RecordConflictRetry(ctx)
	}, func() error {
		released = 0
		return s.db.Update(func(txn *badger.Txn) error {
			for _, claimed := range work {
				if err := ctxErr(ctx); err != nil {
					return err
				}

				record, err := s.loadRecord(txn, claimed.Message.ID)
				if errors.Is(err, badger.ErrKeyNotFound) {
					continue
				}
				if err != nil {
					return err
				}
				if record.Status != recordStatusProcessing || record.LeaseToken != claimed.LeaseToken {
					continue
				}

				processingKey := s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), record.ID)
				createdAt := time.Unix(0, record.CreatedAtUnix).UTC()
				availableAt := time.Unix(0, record.AvailableAtUnix).UTC()
				record.Attempt = max(0, record.Attempt-1)
				record.Status = recordStatusPending
				record.LeaseToken = ""
				record.LeaseUntilUnix = 0

				if err := s.storeRecord(txn, record); err != nil {
					return err
				}
				if err := txn.Delete(processingKey); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				if err := txn.Set(s.keys.readyKey(availableAt, record.ID), emptyValue); err != nil {
					return err
				}
				if err := txn.Delete(s.keys.processingCreatedKey(createdAt, record.ID)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				if err := txn.Set(s.keys.readyCreatedKey(createdAt, record.ID), emptyValue); err != nil {
					return err
				}
				released++
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	if released > 0 {
		s.notifyListeners()
	}
	return released, nil
}
