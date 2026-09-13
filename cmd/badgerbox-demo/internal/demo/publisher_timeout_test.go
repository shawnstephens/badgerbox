package demo

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestReloadPreservesRecordDeliveryTimeout(t *testing.T) {
	p := NewReloadingPublisher("state", []string{"old:9092"}, "topic", nil, kgo.RecordDeliveryTimeout(time.Second))
	defer p.Close()
	old, _, err := p.ensureClient()
	if err != nil {
		t.Fatal(err)
	}
	p.readState = func(string) (State, error) { return State{Brokers: []string{"new:9092"}, Topic: "topic"}, nil }
	if result, err := p.ReloadFromState(); err != nil || !result.BrokersChanged {
		t.Fatalf("reload=%+v err=%v", result, err)
	}
	next, _, err := p.ensureClient()
	if err != nil || old == next {
		t.Fatalf("client was not replaced: %v", err)
	}
	for _, client := range []producerClient{old, next} {
		if got := client.(*franzProducerClient).client.OptValue(kgo.RecordDeliveryTimeout); got != time.Second {
			t.Fatalf("record timeout lost during reload: %v", got)
		}
	}
}
