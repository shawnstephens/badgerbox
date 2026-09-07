package badgerbox

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
)

var (
	// ErrAdmissionLimit means the namespace cannot retain another message within
	// its configured limits. Retry after capacity is released or limits change.
	ErrAdmissionLimit = errors.New("badgerbox: namespace admission limit reached")
	// ErrAdmissionLimitsMismatch means requested limits differ from persisted limits.
	ErrAdmissionLimitsMismatch = errors.New("badgerbox: admission limits differ from persisted configuration")
	// ErrAdmissionState means required accounting metadata is missing or invalid.
	ErrAdmissionState = errors.New("badgerbox: invalid admission accounting state")
	// ErrAdmissionOverflow means accounting cannot represent another message.
	ErrAdmissionOverflow = errors.New("badgerbox: admission accounting overflow")
	// ErrConflictDetectionRequired means Badger must be opened with DetectConflicts.
	ErrConflictDetectionRequired = errors.New("badgerbox: Badger conflict detection must be enabled")
	// ErrForeignTransaction means a transaction does not identify this namespace.
	ErrForeignTransaction = errors.New("badgerbox: transaction belongs to another database or namespace generation")
)

// AdmissionLimits cap all retained messages, including delayed messages, active
// leases, retries, and dead letters. Each zero field explicitly means unlimited.
// Limits are persisted at creation; subsequent New calls must match them exactly.
// Use CompareAndSwapAdmissionLimits to change them safely on an existing namespace.
type AdmissionLimits struct {
	MaxRetainedMessages uint64 `json:"max_retained_messages"`
	// MaxRetainedBytes caps logical bytes, not disk or heap usage. A message is
	// charged the JSON size of its canonical ready record: attempt, availability
	// and lease fields are zero, max_attempts is one, and status is ready. Payload,
	// destination, trace context, ID and creation time are preserved. This charge
	// is nonzero even for empty payloads and is stable through retries and requeue.
	// Indexes, failure text, lifecycle growth and Badger overhead are excluded.
	MaxRetainedBytes uint64 `json:"max_retained_bytes"`
}

// UsageSnapshot is transactionally consistent persisted namespace accounting.
// A successful acknowledgement releases capacity. Dead-lettering does not;
// requeueing the exact dead letter preserves its existing charge.
type UsageSnapshot struct {
	Limits           AdmissionLimits `json:"limits"`
	RetainedMessages uint64          `json:"retained_messages"`
	RetainedBytes    uint64          `json:"retained_bytes"`
}

// AdmissionLimitError identifies a failed admission without including contents.
type AdmissionLimitError struct {
	Resource  string
	Limit     uint64
	Used      uint64
	Requested uint64
}

func (e *AdmissionLimitError) Error() string {
	return fmt.Sprintf("%v: %s limit=%d used=%d requested=%d", ErrAdmissionLimit, e.Resource, e.Limit, e.Used, e.Requested)
}
func (e *AdmissionLimitError) Unwrap() error { return ErrAdmissionLimit }

// AdmissionLimitsMismatchError reports the actual limits so callers can refresh
// their configuration before reopening or retrying a compare-and-swap operation.
type AdmissionLimitsMismatchError struct {
	Expected AdmissionLimits
	Actual   AdmissionLimits
}

func (e *AdmissionLimitsMismatchError) Error() string {
	return fmt.Sprintf("%v: expected=%+v actual=%+v", ErrAdmissionLimitsMismatch, e.Expected, e.Actual)
}
func (e *AdmissionLimitsMismatchError) Unwrap() error { return ErrAdmissionLimitsMismatch }

const admissionStateBytes = 48

type admissionState struct {
	UsageSnapshot
	identity [16]byte
}

// Usage reads a fixed-size metadata record without scanning or decoding payloads.
func (s *Store[M, D]) Usage(ctx context.Context) (UsageSnapshot, error) {
	if err := s.ensureOpen(); err != nil {
		return UsageSnapshot{}, err
	}
	if err := ctxErr(ctx); err != nil {
		return UsageSnapshot{}, err
	}
	var state admissionState
	err := s.db.View(func(txn *badger.Txn) error {
		var err error
		state, err = s.loadAdmissionState(txn)
		return err
	})
	if err != nil {
		return UsageSnapshot{}, err
	}
	return state.UsageSnapshot, nil
}

// CompareAndSwapAdmissionLimits atomically changes namespace limits only when
// the current configuration equals expected. All open Stores enforce the new
// limits on their next committed enqueue. In-flight transactions conflict and
// must retry. Reducing a limit below current usage preserves existing messages
// and rejects new admissions until sufficient capacity is released. Explicitly
// setting a field to zero disables that limit. Reopening must use the new limits.
func (s *Store[M, D]) CompareAndSwapAdmissionLimits(ctx context.Context, expected, next AdmissionLimits) error {
	if err := s.ensureOpen(); err != nil {
		return err
	}
	if err := ctxErr(ctx); err != nil {
		return err
	}
	return withConflictRetryObserved(ctx, s.runtime, func() { s.obs.RecordConflictRetry(ctx) }, func() error {
		return s.db.Update(func(txn *badger.Txn) error {
			state, err := s.loadAdmissionState(txn)
			if err != nil {
				return err
			}
			if state.Limits != expected {
				return &AdmissionLimitsMismatchError{Expected: expected, Actual: state.Limits}
			}
			state.Limits = next
			return s.storeAdmissionState(txn, state)
		})
	})
}

func readAdmissionState(txn *badger.Txn, key []byte) (admissionState, error) {
	var state admissionState
	item, err := txn.Get(key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return state, fmt.Errorf("%w: missing metadata", ErrAdmissionState)
	}
	if err != nil {
		return state, err
	}
	// ValueSize is approximate for value-log entries and zero for pending
	// writes in this transaction. Reject large persisted values before loading;
	// the callback verifies exact length without copying caller-owned writes.
	if item.ValueSize() > admissionStateBytes+16 {
		return state, fmt.Errorf("%w: unexpected metadata size", ErrAdmissionState)
	}
	err = item.Value(func(value []byte) error {
		if len(value) != admissionStateBytes {
			return ErrAdmissionState
		}
		copy(state.identity[:], value[:16])
		state.Limits.MaxRetainedMessages = binary.BigEndian.Uint64(value[16:24])
		state.Limits.MaxRetainedBytes = binary.BigEndian.Uint64(value[24:32])
		state.RetainedMessages = binary.BigEndian.Uint64(value[32:40])
		state.RetainedBytes = binary.BigEndian.Uint64(value[40:48])
		if state.identity == ([16]byte{}) || (state.RetainedMessages == 0) != (state.RetainedBytes == 0) {
			return ErrAdmissionState
		}
		return nil
	})
	return state, err
}

func (s *Store[M, D]) loadAdmissionState(txn *badger.Txn) (admissionState, error) {
	state, err := readAdmissionState(txn, s.keys.admissionKey)
	if err != nil {
		return state, err
	}
	if state.identity != s.admissionIdentity {
		return admissionState{}, ErrForeignTransaction
	}
	return state, nil
}

func (s *Store[M, D]) storeAdmissionState(txn *badger.Txn, state admissionState) error {
	var value [admissionStateBytes]byte
	copy(value[:16], state.identity[:])
	binary.BigEndian.PutUint64(value[16:24], state.Limits.MaxRetainedMessages)
	binary.BigEndian.PutUint64(value[24:32], state.Limits.MaxRetainedBytes)
	binary.BigEndian.PutUint64(value[32:40], state.RetainedMessages)
	binary.BigEndian.PutUint64(value[40:48], state.RetainedBytes)
	return txn.Set(s.keys.admissionKey, value[:])
}

func retainedRecordBytes(record storedRecord) (uint64, error) {
	// JSON replaces invalid UTF-8, potentially collapsing distinct map keys.
	// Reject it before admission so the persisted and original representations
	// cannot acquire different accounting charges after their first decode.
	for key, value := range record.TraceCarrier {
		if !utf8.ValidString(key) || !utf8.ValidString(value) {
			return 0, boxErrorf("trace carrier must contain valid UTF-8")
		}
	}
	record.AvailableAtUnix = 0
	record.Attempt = 0
	record.MaxAttempts = 1
	record.Status = recordStatusPending
	record.LeaseToken = ""
	record.LeaseUntilUnix = 0
	value, err := json.Marshal(record)
	return uint64(len(value)), err
}

func (s *Store[M, D]) prepareAdmission(txn *badger.Txn, record storedRecord) (admissionState, error) {
	state, err := s.loadAdmissionState(txn)
	if err != nil {
		return state, err
	}
	bytes, err := retainedRecordBytes(record)
	if err != nil {
		return state, err
	}
	if state.RetainedMessages == math.MaxUint64 || bytes > math.MaxUint64-state.RetainedBytes {
		return state, ErrAdmissionOverflow
	}
	for _, budget := range []struct {
		name                 string
		limit, used, request uint64
	}{
		{"messages", state.Limits.MaxRetainedMessages, state.RetainedMessages, 1},
		{"bytes", state.Limits.MaxRetainedBytes, state.RetainedBytes, bytes},
	} {
		if budget.limit != 0 && (budget.used > budget.limit || budget.request > budget.limit-budget.used) {
			return state, &AdmissionLimitError{Resource: budget.name, Limit: budget.limit, Used: budget.used, Requested: budget.request}
		}
	}
	state.RetainedMessages++
	state.RetainedBytes += bytes
	return state, nil
}

func (s *Store[M, D]) releaseAdmission(txn *badger.Txn, record storedRecord) error {
	state, err := s.loadAdmissionState(txn)
	if err != nil {
		return err
	}
	bytes, err := retainedRecordBytes(record)
	if err != nil {
		return err
	}
	if state.RetainedMessages == 0 || state.RetainedBytes < bytes {
		return fmt.Errorf("%w: acknowledgement would underflow usage", ErrAdmissionState)
	}
	state.RetainedMessages--
	state.RetainedBytes -= bytes
	if (state.RetainedMessages == 0) != (state.RetainedBytes == 0) {
		return fmt.Errorf("%w: acknowledgement leaves inconsistent usage", ErrAdmissionState)
	}
	return s.storeAdmissionState(txn, state)
}
