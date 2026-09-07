//go:build integration

package integration_test

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"sync/atomic"
	"testing"
	"time"
)

func TestDrainLargeQueue(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := badgerbox.New[int, string](db, badgerbox.Serde[int, string]{}, badgerbox.Options{Namespace: "large"})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	const count = 10000
	for start := 0; start < count; start += 100 {
		if err = db.Update(func(txn *badger.Txn) error {
			for i := start; i < start+100; i++ {
				if _, err := store.EnqueueTx(t.Context(), txn, badgerbox.EnqueueRequest[int, string]{Payload: i, Destination: "sink"}); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := store.QueueSnapshot(t.Context())
	if err != nil || snapshot.ReadyDepth != count {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	var delivered atomic.Int64
	seen := make([]atomic.Bool, count)
	processor, err := badgerbox.NewBatchProcessor(store, func(_ context.Context, m []badgerbox.Message[int, string], results chan<- badgerbox.BatchProcessResult) error {
		for _, message := range m {
			if message.Payload < 0 || message.Payload >= count || seen[message.Payload].Swap(true) {
				t.Errorf("invalid or duplicate delivery: %d", message.Payload)
			}
			delivered.Add(1)
			results <- badgerbox.BatchProcessResult{ID: message.ID}
		}
		return nil
	}, badgerbox.BatchProcessorOptions{ClaimBatchSize: 64, ProcessorOptions: badgerbox.ProcessorOptions{Concurrency: 4, PollInterval: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- processor.Run(ctx) }()
	for {
		snapshot, err = store.QueueSnapshot(ctx)
		if err != nil {
			cancel()
			<-done
			t.Fatal(err)
		}
		if snapshot.ReadyDepth == 0 && snapshot.ProcessingDepth == 0 {
			break
		}
		select {
		case <-ctx.Done():
			cancel()
			<-done
			t.Fatal("drain timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if delivered.Load() != count {
		t.Fatal(delivered.Load())
	}
	report, err := store.Audit(t.Context(), badgerbox.AuditOptions{})
	if err != nil || report.LiveRows != 0 || report.DeadLetters.Rows != 0 || len(report.Samples.Anomalies) != 0 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}
