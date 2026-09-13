package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestClassifyProducerError(t *testing.T) {
	for _, err := range []error{context.DeadlineExceeded, kgo.ErrRecordTimeout, kerr.RequestTimedOut, kerr.NetworkException, kerr.BrokerNotAvailable, io.EOF, io.ErrUnexpectedEOF, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}} {
		t.Run(err.Error(), func(t *testing.T) {
			wrapped := fmt.Errorf("publish: %w", err)
			got := ClassifyProducerError(wrapped)
			if !badgerbox.IsUnavailable(got) || !errors.Is(got, err) {
				t.Fatalf("classification lost outage or identity: %v", got)
			}
		})
	}
	for _, err := range []error{nil, context.Canceled, ErrNilClient, ErrTopicRequired, kerr.MessageTooLarge, kerr.TopicAuthorizationFailed, kerr.UnknownTopicOrPartition, kerr.NotLeaderForPartition, kgo.ErrClientClosed, badgerbox.Permanent(context.DeadlineExceeded)} {
		if got := ClassifyProducerError(err); got != err {
			t.Fatalf("changed non-outage %v", err)
		}
	}
	marked := badgerbox.Unavailable(io.EOF)
	if got := ClassifyProducerError(marked); got != marked {
		t.Fatal("double-wrapped unavailable")
	}
}
