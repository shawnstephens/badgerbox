package main

import (
	"time"

	"github.com/shawnstephens/badgerbox/cmd/badgerbox-demo/internal/demo"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"
)

func logProcessFailure(
	logger *demo.Logger,
	now time.Time,
	msg badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination],
	err error,
	retryBaseDelay, retryMaxDelay time.Duration,
) {
	if logger == nil {
		return
	}

	key := string(msg.Payload.Key)
	permanent := badgerbox.IsPermanent(err)
	if permanent || msg.Attempt >= msg.MaxAttempts {
		logger.Printf("warning", "event=dlq_pending msg_id=%d key=%s topic=%s attempt=%d max_attempts=%d permanent=%t err=%q", msg.ID, key, msg.Destination.Topic, msg.Attempt, msg.MaxAttempts, permanent, err)
		return
	}

	delay := demo.RetryDelay(retryBaseDelay, retryMaxDelay, msg.Attempt)
	availableAt := now.UTC().Add(delay)
	logger.Printf("warning", "event=retry_scheduled msg_id=%d key=%s topic=%s attempt=%d next_attempt=%d delay=%s available_at=%s max_attempts=%d err=%q", msg.ID, key, msg.Destination.Topic, msg.Attempt, msg.Attempt+1, delay, availableAt.Format(time.RFC3339Nano), msg.MaxAttempts, err)
}
