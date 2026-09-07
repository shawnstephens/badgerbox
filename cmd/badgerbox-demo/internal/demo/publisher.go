package demo

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Publisher interface {
	Publish(context.Context, badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]) error
	Close() error
}

type producerClient interface {
	ProduceSync(context.Context, *kgo.Record) error
	Close() error
}

type franzProducerClient struct {
	client  *kgo.Client
	deliver badgerbox.BatchProcessFunc[kafka.KafkaMessage, kafka.KafkaDestination]
}

func (c *franzProducerClient) ProduceSync(ctx context.Context, record *kgo.Record) error {
	return c.client.ProduceSync(ctx, record).FirstErr()
}

func (c *franzProducerClient) Close() error {
	c.client.Close()
	return nil
}

type LoggingPublisher struct {
	logger *Logger
}

func NewLoggingPublisher(logger *Logger) *LoggingPublisher {
	return &LoggingPublisher{logger: logger}
}

func (p *LoggingPublisher) Publish(_ context.Context, msg badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]) error {
	if msg.Destination.Topic == "" {
		return badgerbox.Permanent(kafka.ErrTopicRequired)
	}

	if p.logger != nil {
		p.logger.Printf("publish", "event=logged msg_id=%d key=%s topic=%s attempt=%d payload_bytes=%d header_count=%d", msg.ID, string(msg.Payload.Key), msg.Destination.Topic, msg.Attempt, len(msg.Payload.Value), len(msg.Payload.Headers))
	}
	return nil
}

func (p *LoggingPublisher) Close() error {
	return nil
}

type ReloadResult struct {
	Reloaded       bool
	BrokersChanged bool
	TopicChanged   bool
	Brokers        []string
	Topic          string
}

type ReloadingPublisher struct {
	stateFile string
	logger    *Logger

	mu         sync.Mutex
	brokers    []string
	stateTopic string
	client     producerClient
	closed     bool

	newClient func([]string) (producerClient, error)
	readState func(string) (State, error)
}

func NewReloadingPublisher(stateFile string, brokers []string, stateTopic string, logger *Logger) *ReloadingPublisher {
	return &ReloadingPublisher{
		stateFile:  stateFile,
		logger:     logger,
		brokers:    append([]string(nil), brokers...),
		stateTopic: stateTopic,
		newClient: func(brokers []string) (producerClient, error) {
			client, err := NewKafkaClient(brokers)
			if err != nil {
				return nil, err
			}
			deliver, err := kafka.NewBatchProducerFunc(client)
			if err != nil {
				client.Close()
				return nil, err
			}
			return &franzProducerClient{client: client, deliver: deliver}, nil
		},
		readState: ReadState,
	}
}

func (p *ReloadingPublisher) Close() error {
	p.mu.Lock()
	p.closed = true
	defer p.mu.Unlock()

	if p.client == nil {
		return nil
	}
	err := p.client.Close()
	p.client = nil
	return err
}

func (p *ReloadingPublisher) Publish(ctx context.Context, msg badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]) error {
	if msg.Destination.Topic == "" {
		return badgerbox.Permanent(kafka.ErrTopicRequired)
	}

	client, brokers, err := p.ensureClient()
	if err != nil {
		return err
	}

	record := buildKafkaRecord(msg)
	err = client.ProduceSync(ctx, record)
	if err == nil {
		return nil
	}

	if p.logger != nil {
		p.logger.Printf("warning", "event=publish_failed msg_id=%d key=%s topic=%s attempt=%d max_attempts=%d brokers=%s err=%q", msg.ID, string(msg.Payload.Key), msg.Destination.Topic, msg.Attempt, msg.MaxAttempts, ShortBrokerList(brokers), err)
	}

	result, reloadErr := p.ReloadFromState()
	if reloadErr != nil {
		if p.logger != nil {
			p.logger.Printf("warning", "event=reload_state state_file=%s err=%q", p.stateFile, reloadErr)
		}
		return err
	}
	if result.BrokersChanged && p.logger != nil {
		p.logger.Printf("ready", "event=reconnected brokers=%s state_topic=%s", ShortBrokerList(result.Brokers), result.Topic)
	}

	return err
}

func (p *ReloadingPublisher) ReloadFromState() (ReloadResult, error) {
	state, err := p.readState(p.stateFile)
	if err != nil {
		return ReloadResult{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	result := ReloadResult{
		Brokers: append([]string(nil), p.brokers...),
		Topic:   p.stateTopic,
	}
	if p.closed {
		return ReloadResult{}, errors.New("publisher is closed")
	}
	brokersChanged := !slices.Equal(p.brokers, state.Brokers)
	topicChanged := p.stateTopic != state.Topic
	result.BrokersChanged = brokersChanged
	result.TopicChanged = topicChanged

	if !brokersChanged && !topicChanged {
		return result, nil
	}

	if p.logger != nil {
		p.logger.Printf("warning", "event=reload_state state_file=%s old_brokers=%s new_brokers=%s old_topic=%s new_topic=%s", p.stateFile, ShortBrokerList(p.brokers), ShortBrokerList(state.Brokers), p.stateTopic, state.Topic)
	}

	if brokersChanged {
		if p.client != nil {
			_ = p.client.Close()
			p.client = nil
		}
	}

	p.brokers = append([]string(nil), state.Brokers...)
	p.stateTopic = state.Topic
	result.Brokers = append([]string(nil), p.brokers...)
	result.Topic = p.stateTopic
	result.Reloaded = true

	if brokersChanged {
		client, err := p.newClient(p.brokers)
		if err != nil {
			return ReloadResult{}, fmt.Errorf("recreate kafka client: %w", err)
		}
		p.client = client
	}

	return result, nil
}

func (p *ReloadingPublisher) ensureClient() (producerClient, []string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, nil, errors.New("publisher is closed")
	}
	if len(p.brokers) == 0 {
		return nil, nil, errors.New("no Kafka brokers configured")
	}
	if p.client == nil {
		client, err := p.newClient(p.brokers)
		if err != nil {
			return nil, nil, fmt.Errorf("create kafka client: %w", err)
		}
		p.client = client
	}

	return p.client, append([]string(nil), p.brokers...), nil
}

func buildKafkaRecord(msg badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination]) *kgo.Record {
	record := &kgo.Record{
		Topic:     msg.Destination.Topic,
		Partition: -1,
		Key:       cloneBytes(msg.Payload.Key),
		Value:     cloneBytes(msg.Payload.Value),
	}
	if msg.Destination.Partition != nil {
		record.Partition = *msg.Destination.Partition
	}

	headerKeys := make([]string, 0, len(msg.Payload.Headers))
	for key := range msg.Payload.Headers {
		headerKeys = append(headerKeys, key)
	}
	sort.Strings(headerKeys)
	for _, key := range headerKeys {
		record.Headers = append(record.Headers, kgo.RecordHeader{
			Key:   key,
			Value: cloneBytes(msg.Payload.Headers[key]),
		})
	}

	return record
}

func cloneBytes(data []byte) []byte {
	if data == nil {
		return nil
	}
	cloned := make([]byte, len(data))
	copy(cloned, data)
	return cloned
}

// BatchPublisher delivers queue messages through a validated Kafka adapter or
// the explicit logging transport. It also owns flush and close operations.
type BatchPublisher interface {
	Deliver(context.Context, []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], chan<- badgerbox.BatchProcessResult) error
	Flush(context.Context) error
	Close() error
}

func (c *franzProducerClient) Deliver(ctx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
	return c.deliver(ctx, messages, results)
}
func (c *franzProducerClient) Flush(ctx context.Context) error { return c.client.Flush(ctx) }
func (p *LoggingPublisher) Deliver(ctx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
	for _, msg := range messages {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		if msg.Destination.Partition != nil && *msg.Destination.Partition < 0 {
			err = badgerbox.Permanent(kafka.ErrInvalidPartition)
		} else {
			err = p.Publish(badgerbox.ContextForMessage(ctx, msg.ID), msg)
		}
		results <- badgerbox.BatchProcessResult{ID: msg.ID, Err: err}
	}
	return nil
}
func (p *LoggingPublisher) Flush(context.Context) error { return nil }
func (p *ReloadingPublisher) Deliver(ctx context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
	client, _, err := p.ensureClient()
	if err != nil {
		return err
	}
	delivery, ok := client.(BatchPublisher)
	if !ok {
		return errors.New("producer does not support validated batch delivery")
	}
	return delivery.Deliver(ctx, messages, results)
}
func (p *ReloadingPublisher) Flush(ctx context.Context) error {
	p.mu.Lock()
	client := p.client
	p.mu.Unlock()
	if client == nil {
		return nil
	}
	if flusher, ok := client.(interface{ Flush(context.Context) error }); ok {
		return flusher.Flush(ctx)
	}
	return nil
}
