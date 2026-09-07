package badgerbox

import (
	"errors"
	"fmt"
)

var (
	ErrNilDB            = errors.New("badgerbox: db is nil")
	ErrNilTxn           = errors.New("badgerbox: txn is nil")
	ErrNilStore         = errors.New("badgerbox: store is nil")
	ErrStoreClosed      = errors.New("badgerbox: store is closed")
	ErrNotFound         = errors.New("badgerbox: message not found")
	ErrProcessorFuncNil = errors.New("badgerbox: process func is nil")
)

type permanentError struct {
	err error
}

func (e permanentError) Error() string {
	return e.err.Error()
}

func (e permanentError) Unwrap() error {
	return e.err
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return permanentError{err: err}
}

func IsPermanent(err error) bool {
	var retryable retryableBatchError
	if errors.As(err, &retryable) {
		return false
	}

	var target permanentError
	return errors.As(err, &target)
}

func panicError(recovered any) error {
	return fmt.Errorf("badgerbox: process panic: %v", recovered)
}

var (
	ErrNilContext         = errors.New("badgerbox: context is nil")
	ErrInvalidNamespace   = errors.New("badgerbox: namespace contains separator")
	ErrIncompatibleFormat = errors.New("badgerbox: incompatible storage format; use a new directory")
	// ErrInconsistentIndex means lifecycle indexes cannot provide a reliable snapshot.
	ErrInconsistentIndex = errors.New("badgerbox: inconsistent queue index")
	// ErrMessageTooLarge means a record cannot safely fit its lifecycle transitions.
	ErrMessageTooLarge = errors.New("badgerbox: message exceeds lifecycle storage budget")
)

func boxErrorf(format string, args ...any) error { return fmt.Errorf("badgerbox: "+format, args...) }

var ErrBatchResultMissing = errors.New("badgerbox: batch process result missing")

type retryableBatchError struct{ err error }

func markBatchErrorRetryable(err error) error {
	if err == nil {
		return nil
	}
	return retryableBatchError{err: err}
}
func (e retryableBatchError) Error() string { return e.err.Error() }
func (e retryableBatchError) Unwrap() error { return e.err }

var ErrDeadLetterTooLarge = errors.New("badgerbox: dead letter exceeds page byte limit")
var ErrLiveMessageExists = errors.New("badgerbox: message already exists in live queue")
