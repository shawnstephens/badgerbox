package demo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func StartKafka(ctx context.Context, image, clusterID string) (*kafka.KafkaContainer, []string, error) {
	container, err := kafka.Run(ctx, image, kafka.WithClusterID(clusterID))
	if err != nil {
		return nil, nil, err
	}

	brokers, err := container.Brokers(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		return nil, nil, fmt.Errorf("fetch kafka brokers: %w", err)
	}
	return container, brokers, nil
}

func NewKafkaClient(brokers []string, opts ...kgo.Opt) (*kgo.Client, error) {
	allOpts := append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)
	return kgo.NewClient(allOpts...)
}

func CreateTopic(ctx context.Context, client *kgo.Client, topic string, partitions int32) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	reqTopic := kmsg.NewCreateTopicsRequestTopic()
	reqTopic.Topic = topic
	reqTopic.NumPartitions = partitions
	reqTopic.ReplicationFactor = 1
	req.Topics = append(req.Topics, reqTopic)

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return fmt.Errorf("create topic %q: %w", topic, err)
	}
	if len(resp.Topics) != 1 {
		return fmt.Errorf("create topic %q: unexpected response size %d", topic, len(resp.Topics))
	}
	if err := kerr.ErrorForCode(resp.Topics[0].ErrorCode); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %q response: %w", topic, err)
	}
	return nil
}

func ParseBrokers(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			brokers = append(brokers, part)
		}
	}
	return brokers
}

func FirstFatalFetchError(errs []kgo.FetchError) error {
	for _, fetchErr := range errs {
		if errors.Is(fetchErr.Err, kerr.UnknownTopicOrPartition) {
			continue
		}
		if errors.Is(fetchErr.Err, kerr.LeaderNotAvailable) || errors.Is(fetchErr.Err, kerr.NotLeaderForPartition) {
			continue
		}
		return fetchErr.Err
	}
	return nil
}

func PollForRecords(ctx context.Context, client *kgo.Client) ([]*kgo.Record, error) {
	fetches := client.PollFetches(ctx)
	if err := FirstFatalFetchError(fetches.Errors()); err != nil {
		return nil, err
	}
	records := make([]*kgo.Record, 0)
	iter := fetches.RecordIter()
	for !iter.Done() {
		records = append(records, iter.Next())
	}
	return records, nil
}

func ShortBrokerList(brokers []string) string {
	return strings.Join(brokers, ",")
}

func DefaultKafkaTimeout() time.Duration {
	return 30 * time.Second
}
