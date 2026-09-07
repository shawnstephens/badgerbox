package runner

import "fmt"

// QueueFailurePolicy controls how a queue worker failure affects other queues.
type QueueFailurePolicy int

const (
	// IsolateQueue stops only the failing queue. This is the default policy.
	IsolateQueue QueueFailurePolicy = iota
	// FailFast stops all queue workers and periodic maintenance after any failure.
	FailFast
)

// QueueError identifies the failed namespace while preserving the original error.
type QueueError struct {
	Namespace string
	Err       error
}

func (e *QueueError) Error() string {
	return fmt.Sprintf("badgerbox runner: queue %q: %v", e.Namespace, e.Err)
}
func (e *QueueError) Unwrap() error { return e.Err }
