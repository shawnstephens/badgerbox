package kafka

import (
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

func TestDeliveryFactoriesRequirePartitioner(t *testing.T) {
	plain, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	configured, err := NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer configured.Close()
	manual, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.RecordPartitioner(KafkaPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer manual.Close()
	for _, tc := range []struct {
		name   string
		client *kgo.Client
		want   error
	}{{"nil", nil, ErrNilClient}, {"default", plain, ErrPartitionerRequired}, {"supported constructor", configured, nil}, {"explicit configuration", manual, nil}} {
		t.Run(tc.name, func(t *testing.T) {
			single, err := NewProcessFunc(tc.client, Options{})
			if !errors.Is(err, tc.want) || (single == nil) != (tc.want != nil) {
				t.Fatalf("single err=%v", err)
			}
			batch, err := NewBatchProducerFunc(tc.client)
			if !errors.Is(err, tc.want) || (batch == nil) != (tc.want != nil) {
				t.Fatalf("batch err=%v", err)
			}
		})
	}
}
