package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDeliveryClientsRejectUnsafeConfigurations(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []kgo.Opt
		want error
	}{
		{"no acknowledgement", []kgo.Opt{kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.NoAck())}, ErrAllISRAcksRequired},
		{"leader acknowledgement", []kgo.Opt{kgo.DisableIdempotentWrite(), kgo.RequiredAcks(kgo.LeaderAck())}, ErrAllISRAcksRequired},
		{"uncommitted transaction", []kgo.Opt{kgo.TransactionalID("outbox-transaction")}, ErrTransactionalClient},
		{"destination topic override", []kgo.Opt{kgo.DefaultProduceTopic("wrong-topic"), kgo.DefaultProduceTopicAlways()}, ErrTopicOverride},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]kgo.Opt{kgo.SeedBrokers("127.0.0.1:1"), kgo.RecordPartitioner(KafkaPartitioner())}, tc.opts...)
			client, err := kgo.NewClient(opts...)
			if err != nil {
				t.Fatalf("configuration must be accepted by franz-go to exercise adapter validation: %v", err)
			}
			defer client.Close()
			if fn, err := NewProcessFunc(client, Options{}); fn != nil || !errors.Is(err, tc.want) {
				t.Fatalf("single-message adapter: fn nil=%t, err=%v, want %v", fn == nil, err, tc.want)
			}
			if fn, err := NewBatchProducerFunc(client); fn != nil || !errors.Is(err, tc.want) {
				t.Fatalf("batch adapter: fn nil=%t, err=%v, want %v", fn == nil, err, tc.want)
			}
			created, err := NewClient(opts...)
			if created != nil {
				created.Close()
			}
			if created != nil || !errors.Is(err, tc.want) {
				t.Fatalf("client constructor: client nil=%t, err=%v, want %v", created == nil, err, tc.want)
			}
		})
	}
}

func TestDeliveryClientAllowsSafeProducerTuning(t *testing.T) {
	client, err := NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.DefaultProduceTopic("fallback"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),
		kgo.MaxBufferedRecords(16),
		kgo.MaxBufferedBytes(1<<20),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := NewProcessFunc(client, Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBatchProducerFunc(client); err != nil {
		t.Fatal(err)
	}
}
