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
	var target permanentError
	return errors.As(err, &target)
}

func panicError(recovered any) error {
	return fmt.Errorf("badgerbox: process panic: %v", recovered)
}
