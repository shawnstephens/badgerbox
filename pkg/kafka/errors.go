package kafka

import (
	"context"
	"errors"
	"io"
	"net"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// ClassifyProducerError marks transport outages and exhausted publish timeouts
// for the generic circuit breaker. It preserves the original error chain.
// The processor distinguishes shutdown from a live run's publish deadline.
func ClassifyProducerError(err error) error {
	if err == nil || badgerbox.IsPermanent(err) || errors.Is(err, context.Canceled) || badgerbox.IsUnavailable(err) {
		return err
	}
	var op *net.OpError
	if errors.As(err, &op) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrRecordTimeout) ||
		errors.Is(err, kerr.RequestTimedOut) || errors.Is(err, kerr.NetworkException) || errors.Is(err, kerr.BrokerNotAvailable) {
		return badgerbox.Unavailable(err)
	}
	return err
}
