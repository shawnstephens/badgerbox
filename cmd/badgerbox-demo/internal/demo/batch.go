package demo

import (
	"context"
	"time"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
)

func NewBatchProcessFunc(p BatchPublisher, timeout time.Duration, logger *Logger) badgerbox.BatchProcessFunc[kafka.KafkaMessage, kafka.KafkaDestination] {
	deliver := p.Deliver
	return func(parentCtx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) (deliveryErr error) {
		if err := parentCtx.Err(); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(parentCtx, timeout)
		failed := false
		forward := func(result badgerbox.BatchProcessResult) error {
			if result.Err != nil {
				failed = true
				if logger != nil {
					logger.Printf("warning", "event=publish_failed msg_id=%s err=%q", result.ID, result.Err)
				}
			} else if logger != nil {
				logger.Printf("publish", "event=success msg_id=%s", result.ID)
			}
			// Preserve known results when the worker's buffer has room, even when
			// publishing just canceled. Otherwise forwarding must honor cancellation.
			select {
			case results <- result:
				return nil
			default:
			}
			select {
			case results <- result:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		// One slot per callback allows every late result to complete after timeout.
		incoming := make(chan badgerbox.BatchProcessResult, len(messages))
		defer func() {
			// Cancel asynchronous publishing before closing/replacing its client. A
			// publish timeout permits recovery; parent cancellation means shutdown.
			cancel()
			// Snapshot the buffer after cancellation; late callbacks cannot extend
			// cleanup indefinitely. Preserve known outcomes on every terminal exit.
			for range len(incoming) {
				result, ok := <-incoming
				if !ok || forward(result) != nil {
					break
				}
			}
			if parentCtx.Err() != nil || (!failed && deliveryErr == nil) {
				return
			}
			if reloader, ok := p.(interface{ ReloadFromState() (ReloadResult, error) }); ok {
				result, err := reloader.ReloadFromState()
				if err != nil && logger != nil {
					logger.Printf("warning", "event=reload_state err=%q", err)
				} else if result.BrokersChanged && logger != nil {
					logger.Printf("ready", "event=reconnected brokers=%s state_topic=%s", ShortBrokerList(result.Brokers), result.Topic)
				}
			}
		}()
		if err := deliver(ctx, messages, incoming); err != nil {
			return err
		}
		for range messages {
			if err := ctx.Err(); err != nil {
				return err
			}
			select {
			case result, ok := <-incoming:
				if !ok {
					return badgerbox.ErrBatchResultMissing
				}
				if err := forward(result); err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return ctx.Err()
	}
}
