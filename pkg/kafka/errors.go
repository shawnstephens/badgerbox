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
	// franz-go can wrap a delivery timeout together with the last metadata
	// error. A known message/destination failure must retain its retry budget
	// even when the surrounding operation ran out of time.
	for _, cause := range []error{
		kerr.UnknownTopicOrPartition, kerr.UnknownTopicID, kerr.InvalidTopicException,
		kerr.TopicAuthorizationFailed, kerr.MessageTooLarge, kerr.RecordListTooLarge,
		kerr.InvalidRecord, kerr.UnsupportedForMessageFormat,
	} {
		if errors.Is(err, cause) {
			return err
		}
	}
	var op *net.OpError
	if errors.As(err, &op) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, kgo.ErrRecordTimeout) ||
		errors.Is(err, kerr.RequestTimedOut) || errors.Is(err, kerr.NetworkException) || errors.Is(err, kerr.BrokerNotAvailable) {
		return badgerbox.Unavailable(err)
	}
	return err
}
