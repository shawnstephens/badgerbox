package kafka

import (
	"context"
	"errors"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
)

type asyncStub struct {
	records   []*kgo.Record
	callbacks []func(*kgo.Record, error)
}

func (s *asyncStub) Produce(_ context.Context, r *kgo.Record, cb func(*kgo.Record, error)) {
	s.records = append(s.records, r)
	s.callbacks = append(s.callbacks, cb)
}
func TestAsyncBatchValidationAndLateResult(t *testing.T) {
	producer := &asyncStub{}
	fn := NewBatchProducerFunc(producer)
	results := make(chan badgerbox.BatchProcessResult, 2)
	partition := int32(-1)
	messages := []badgerbox.Message[KafkaMessage, KafkaDestination]{{ID: 1, Payload: KafkaMessage{Value: []byte{255}}, Destination: KafkaDestination{Topic: "test"}}, {ID: 2, Destination: KafkaDestination{Topic: "test", Partition: &partition}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := fn(ctx, messages, results); err != nil {
		t.Fatal(err)
	}
	if len(producer.records) != 1 || producer.records[0].Partition != -1 {
		t.Fatal("incorrect scheduled records")
	}
	invalid := <-results
	if invalid.ID != 2 || !badgerbox.IsPermanent(invalid.Err) {
		t.Fatalf("invalid=%+v", invalid)
	}
	messages[0].Payload.Value[0] = 0
	if producer.records[0].Value[0] != 255 {
		t.Fatal("payload alias")
	}
	cancel()
	producer.callbacks[0](producer.records[0], errors.New("late failure"))
	late := <-results
	if late.ID != 1 || late.Err == nil {
		t.Fatalf("late=%+v", late)
	}
	p := KafkaPartitioner().ForTopic("test")
	if got := p.Partition(&kgo.Record{Partition: 2}, 3); got != 2 {
		t.Fatalf("partition=%d", got)
	}
}
