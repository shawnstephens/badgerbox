package demo

import (
	"context"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafka"
	"sync"
	"testing"
	"time"
)

type asyncTestPublisher struct {
	mu        sync.Mutex
	callbacks []func()
	scheduled chan struct{}
}

func (p *asyncTestPublisher) Deliver(_ context.Context, messages []badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], results chan<- badgerbox.BatchProcessResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, msg := range messages {
		p.callbacks = append(p.callbacks, func() { results <- badgerbox.BatchProcessResult{ID: msg.ID} })
	}
	close(p.scheduled)
	return nil
}
func (p *asyncTestPublisher) Flush(context.Context) error { return nil }
func (p *asyncTestPublisher) Close() error                { return nil }
func TestDemoBatchSchedulesBeforeWaitingForCallbacks(t *testing.T) {
	p := &asyncTestPublisher{scheduled: make(chan struct{})}
	fn := NewBatchProcessFunc(p, time.Second, nil)
	messages := make([]badgerbox.Message[kafka.KafkaMessage, kafka.KafkaDestination], 3)
	for i := range messages {
		messages[i].ID = badgerbox.MessageID(i)
		messages[i].Destination.Topic = "demo"
	}
	results := make(chan badgerbox.BatchProcessResult, 3)
	done := make(chan error, 1)
	go func() { done <- fn(t.Context(), messages, results) }()
	select {
	case <-p.scheduled:
	case <-time.After(time.Second):
		t.Fatal("delivery did not schedule full batch")
	}
	p.mu.Lock()
	callbacks := append([]func(){}, p.callbacks...)
	p.mu.Unlock()
	for _, callback := range callbacks {
		callback()
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatal("results missing")
	}
}
