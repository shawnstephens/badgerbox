package badgerbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	recordStatusPending    = "pending"
	recordStatusProcessing = "processing"
)

var emptyValue = []byte{}

type Store[M any, D any] struct {
	db     *badger.DB
	serde  Serde[M, D]
	opts   Options
	keys   keyspace
	seq    *badger.Sequence
	obs    *otelInstrumentation
	deps   runtimeDeps
	closed atomic.Bool

	closeOnce sync.Once

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
	Status           string            `json:"status"`
	LeaseToken       string            `json:"lease_token,omitempty"`
	LeaseUntilUnix   int64             `json:"lease_until_unix_nano,omitempty"`
}

type storedDeadLetter struct {
	Record    storedRecord `json:"record"`
	FailedAt  int64        `json:"failed_at_unix_nano"`
	Error     string       `json:"error"`
	Permanent bool         `json:"permanent"`
}

type enqueueResult struct {
	id     MessageID
	record storedRecord
}

type failProcessingResult struct {
	outcome    string
	retryDelay time.Duration
}

func New[M any, D any](db *badger.DB, serde Serde[M, D], opts Options) (*Store[M, D], error) {
	if db == nil {
		return nil, ErrNilDB
	}

	opts = normalizeOptions(opts)
	serde = normalizeSerde(serde)

	seq, err := db.GetSequence(newKeyspace(opts.Namespace).sequenceKey, opts.IDLeaseSize)
	if err != nil {
		return nil, err
	}

	store := &Store[M, D]{
		db:        db,
		serde:     serde,
		opts:      opts,
		keys:      newKeyspace(opts.Namespace),
		seq:       seq,
		deps:      defaultRuntimeDeps(),
		listeners: make(map[int]chan struct{}),
	}

	store.obs, err = newOTelInstrumentation(opts.Observability, opts.Namespace, store.queueSnapshot)
	if err != nil {
		_ = seq.Release()
		return nil, err
	}

	return store, nil
}

func (s *Store[M, D]) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.closed.Store(true)
		if s.seq != nil {
			err = errors.Join(err, s.seq.Release())
		}
		if s.obs != nil {
			err = errors.Join(err, s.obs.close())
		}
	})
	return err
}

func (s *Store[M, D]) Enqueue(ctx context.Context, req EnqueueRequest[M, D]) (MessageID, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	start := s.deps.now().UTC()
	availableAt := normalizedAvailableAt(start, req.AvailableAt)
	traceCtx, traceSpan, traceCarrier := s.obs.startEnqueueSpan(ctx, availableAt, defaultMaxAttempts)

	var result enqueueResult
	err := withConflictRetryObserved(ctx, s.deps, func() {
		s.obs.recordConflictRetry(traceCtx)
		traceSpan.AddEvent("conflict_retry")
	}, func() error {
		return s.db.Update(func(txn *badger.Txn) error {
			var updateErr error
			result, updateErr = s.enqueueTx(ctx, txn, req, defaultMaxAttempts, traceCarrier)
			return updateErr
		})
	})
	if err != nil {
		traceSpan.RecordError(err)
		s.obs.endSpan(traceSpan, "error")
		return 0, err
	}

	s.obs.setMessageSpanAttributes(traceSpan, result.record.ID, result.record.Attempt, result.record.MaxAttempts, time.Unix(0, result.record.CreatedAtUnix).UTC(), time.Unix(0, result.record.AvailableAtUnix).UTC())
	s.obs.endSpan(traceSpan, metricOutcomeCommitted)
	s.obs.recordEnqueueCommitted(traceCtx, time.Since(start))
	s.notifyListeners()
	return result.id, nil
}

func (s *Store[M, D]) EnqueueTx(ctx context.Context, txn *badger.Txn, req EnqueueRequest[M, D]) (MessageID, error) {
	if err := s.ensureOpen(); err != nil {
		return 0, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	start := s.deps.now().UTC()
	availableAt := normalizedAvailableAt(start, req.AvailableAt)
	traceCtx, traceSpan, traceCarrier := s.obs.startEnqueueSpan(ctx, availableAt, defaultMaxAttempts)

	result, err := s.enqueueTx(ctx, txn, req, defaultMaxAttempts, traceCarrier)
	if err != nil {
		traceSpan.RecordError(err)
		s.obs.endSpan(traceSpan, "error")
		return 0, err
	}

	s.obs.setMessageSpanAttributes(traceSpan, result.record.ID, result.record.Attempt, result.record.MaxAttempts, time.Unix(0, result.record.CreatedAtUnix).UTC(), time.Unix(0, result.record.AvailableAtUnix).UTC())
	s.obs.endSpan(traceSpan, metricOutcomePrepared)
	s.obs.recordEnqueuePrepared(traceCtx, time.Since(start))
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
	if err := s.ensureOpen(); err != nil {
		return nil, nil, err
	}
	if err := ctxErr(ctx); err != nil {
		return nil, nil, err
	}
	if limit <= 0 {
		return nil, nil, nil
	}

	deadLetters := make([]DeadLetter[M, D], 0, limit)
	var nextCursor []byte

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
			if skipCurrent && string(key) == string(cursor) {
				skipCurrent = false
				continue
			}
			skipCurrent = false

			value, err := it.Item().ValueCopy(nil)
			if err != nil {
				return err
			}

			deadLetter, err := s.decodeDeadLetter(value)
			if err != nil {
				return err
			}

			deadLetters = append(deadLetters, deadLetter)
			nextCursor = key
			if len(deadLetters) == limit {
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

func (s *Store[M, D]) RequeueDeadLetter(ctx context.Context, id MessageID, at time.Time) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}

	if at.IsZero() {
		at = s.deps.now().UTC()
	} else {
		at = at.UTC()
	}

	var requeued bool
	err := withConflictRetryObserved(ctx, s.deps, func() {
		s.obs.recordConflictRetry(ctx)
	}, func() error {
		return s.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek(s.keys.deadLetterPrefix); it.ValidForPrefix(s.keys.deadLetterPrefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				_, currentID, err := parseTimeAndIDKey(s.keys.deadLetterPrefix, key)
				if err != nil {
					return err
				}
				if currentID != id {
					continue
				}

				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}

				var deadLetter storedDeadLetter
				if err := json.Unmarshal(value, &deadLetter); err != nil {
					return err
				}

				record := deadLetter.Record
				record.Attempt = 0
				record.Status = recordStatusPending
				record.LeaseToken = ""
				record.LeaseUntilUnix = 0
				record.AvailableAtUnix = at.UnixNano()
				if record.MaxAttempts <= 0 {
					record.MaxAttempts = defaultMaxAttempts
				}

				encodedRecord, err := json.Marshal(record)
				if err != nil {
					return err
				}

				if err := txn.Set(s.keys.messageKey(record.ID), encodedRecord); err != nil {
					return err
				}
				if err := txn.Set(s.keys.readyKey(at, record.ID), emptyValue); err != nil {
					return err
				}
				if err := txn.Delete(key); err != nil {
					return err
				}

				requeued = true
				return nil
			}

			return ErrNotFound
		})
	})
	if err != nil {
		return err
	}
	if requeued {
		s.obs.recordManualRequeue(ctx)
		s.notifyListeners()
	}
	return nil
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

	now := s.deps.now().UTC()
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

	if err := txn.Set(s.keys.messageKey(id), encodedRecord); err != nil {
		return result, err
	}
	if err := txn.Set(s.keys.readyKey(availableAt, id), emptyValue); err != nil {
		return result, err
	}

	result = enqueueResult{
		id:     id,
		record: record,
	}
	return result, nil
}

func (s *Store[M, D]) claimReadyBatch(ctx context.Context, now time.Time, batchSize int, leaseDuration time.Duration, maxAttempts int) ([]claimedRecord[M, D], error) {
	claimed := make([]claimedRecord[M, D], 0, batchSize)
	now = now.UTC()

	err := withConflictRetryObserved(ctx, s.deps, func() {
		s.obs.recordConflictRetry(ctx)
	}, func() error {
		claimed = claimed[:0]
		return s.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek(s.keys.readyPrefix); it.ValidForPrefix(s.keys.readyPrefix) && len(claimed) < batchSize; it.Next() {
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

				record, err := s.loadRecord(txn, id)
				if errors.Is(err, badger.ErrKeyNotFound) {
					if err := txn.Delete(key); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return err
				}

				if record.Status == "" {
					record.Status = recordStatusPending
				}
				if record.Status != recordStatusPending {
					if err := txn.Delete(key); err != nil {
						return err
					}
					continue
				}
				if record.AvailableAtUnix != availableAt.UnixNano() {
					if err := txn.Delete(key); err != nil {
						return err
					}
					if err := txn.Set(s.keys.readyKey(time.Unix(0, record.AvailableAtUnix).UTC(), id), emptyValue); err != nil {
						return err
					}
					continue
				}

				record.Attempt++
				record.MaxAttempts = maxAttempts
				record.Status = recordStatusProcessing
				record.LeaseUntilUnix = now.Add(leaseDuration).UnixNano()

				token, err := s.deps.newLeaseToken()
				if err != nil {
					return err
				}
				record.LeaseToken = token

				if err := s.storeRecord(txn, record); err != nil {
					return err
				}
				if err := txn.Delete(key); err != nil {
					return err
				}
				if err := txn.Set(s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), record.ID), []byte(token)); err != nil {
					return err
				}

				message, err := s.recordToMessage(record)
				if err != nil {
					return err
				}

				claimed = append(claimed, claimedRecord[M, D]{
					Message:      message,
					LeaseToken:   token,
					TraceCarrier: cloneStringMap(record.TraceCarrier),
				})
			}

			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	if len(claimed) > 0 {
		s.obs.recordClaimBatch(ctx, len(claimed))
		for _, record := range claimed {
			s.obs.recordClaimTiming(ctx, positiveDuration(now.Sub(record.Message.AvailableAt)), positiveDuration(now.Sub(record.Message.CreatedAt)))
		}
	}

	return claimed, nil
}

func (s *Store[M, D]) acknowledge(ctx context.Context, id MessageID, leaseToken string) error {
	return withConflictRetryObserved(ctx, s.deps, func() {
		s.obs.recordConflictRetry(ctx)
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

			if err := txn.Delete(s.keys.messageKey(id)); err != nil {
				return err
			}
			if err := txn.Delete(s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), id)); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
				return err
			}
			return nil
		})
	})
}

func (s *Store[M, D]) failProcessing(ctx context.Context, id MessageID, leaseToken string, processErr error, retryBase, retryMax time.Duration) (failProcessingResult, error) {
	var result failProcessingResult
	now := s.deps.now().UTC()
	err := withConflictRetryObserved(ctx, s.deps, func() {
		s.obs.recordConflictRetry(ctx)
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

			processingKey := s.keys.processingKey(time.Unix(0, record.LeaseUntilUnix).UTC(), id)

			if IsPermanent(processErr) || record.Attempt >= record.MaxAttempts {
				dlq := storedDeadLetter{
					Record:    record,
					FailedAt:  now.UnixNano(),
					Error:     processErr.Error(),
					Permanent: IsPermanent(processErr),
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
				result.outcome = metricOutcomeDeadLetter
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
			result.outcome = metricOutcomeRetried
			result.retryDelay = delay
			return nil
		})
	})
	if err != nil {
		return failProcessingResult{}, err
	}
	return result, nil
}

func (s *Store[M, D]) requeueExpired(ctx context.Context, now time.Time) (int, error) {
	var requeued int
	now = now.UTC()

	err := withConflictRetryObserved(ctx, s.deps, func() {
		s.obs.recordConflictRetry(ctx)
	}, func() error {
		requeued = 0
		return s.db.Update(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchValues = false
			it := txn.NewIterator(opts)
			defer it.Close()

			for it.Seek(s.keys.processingPrefix); it.ValidForPrefix(s.keys.processingPrefix); it.Next() {
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

				tokenBytes, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}

				record, err := s.loadRecord(txn, id)
				if errors.Is(err, badger.ErrKeyNotFound) {
					if err := txn.Delete(key); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return err
				}

				if record.Status != recordStatusProcessing || record.LeaseToken != string(tokenBytes) || record.LeaseUntilUnix != leaseUntil.UnixNano() {
					if err := txn.Delete(key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
						return err
					}
					continue
				}

				record.Status = recordStatusPending
				record.LeaseToken = ""
				record.LeaseUntilUnix = 0
				record.AvailableAtUnix = now.UnixNano()

				if err := s.storeRecord(txn, record); err != nil {
					return err
				}
				if err := txn.Delete(key); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
					return err
				}
				if err := txn.Set(s.keys.readyKey(now, id), emptyValue); err != nil {
					return err
				}
				requeued++
			}
			return nil
		})
	})
	if err != nil {
		return 0, err
	}

	if requeued > 0 {
		s.obs.recordExpiredLeaseRequeue(ctx, requeued)
		s.notifyListeners()
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
	var record storedRecord
	item, err := txn.Get(s.keys.messageKey(id))
	if err != nil {
		return record, err
	}

	value, err := item.ValueCopy(nil)
	if err != nil {
		return record, err
	}
	if err := json.Unmarshal(value, &record); err != nil {
		return record, err
	}
	if record.Status == "" {
		record.Status = recordStatusPending
	}
	if record.ID == 0 {
		record.ID = id
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

	payload, err := s.serde.Message.Unmarshal(record.PayloadBytes)
	if err != nil {
		return zero, err
	}
	destination, err := s.serde.Destination.Unmarshal(record.DestinationBytes)
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
	}, nil
}

func (s *Store[M, D]) decodeDeadLetter(data []byte) (DeadLetter[M, D], error) {
	var encoded storedDeadLetter
	var result DeadLetter[M, D]

	if err := json.Unmarshal(data, &encoded); err != nil {
		return result, err
	}

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

func newLeaseToken() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("badgerbox: generate lease token: %w", err)
	}
	return hex.EncodeToString(data[:]), nil
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
		return nil
	}
	return ctx.Err()
}
