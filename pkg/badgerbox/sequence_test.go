package badgerbox

import (
	"fmt"
	"sync"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestConcurrentStoresNeverReuseMessageIDs(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const writers, messages = 8, 100
	stores := make([]*Store[string, string], writers)
	for i := range stores {
		stores[i], err = New[string, string](db, Serde[string, string]{}, Options{Namespace: "shared", IDLeaseSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		defer stores[i].Close()
	}
	type enqueued struct {
		id      MessageID
		payload string
	}
	results := make(chan enqueued, writers*messages)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i, store := range stores {
		wg.Go(func() {
			<-start
			for j := range messages {
				payload := fmt.Sprintf("writer=%d message=%d", i, j)
				id, err := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: payload})
				if err != nil {
					t.Error(err)
					return
				}
				results <- enqueued{id, payload}
			}
		})
	}
	close(start)
	wg.Wait()
	close(results)
	seen := make(map[MessageID]string)
	for result := range results {
		if prior, exists := seen[result.id]; exists {
			t.Fatalf("message ID %s reused for %q and %q", result.id, prior, result.payload)
		}
		seen[result.id] = result.payload
		got, err := stores[0].Get(t.Context(), result.id)
		if err != nil || got.Payload != result.payload {
			t.Fatalf("message ID %s overwritten: payload=%q want=%q err=%v", result.id, got.Payload, result.payload, err)
		}
	}
	if len(seen) != writers*messages {
		t.Fatalf("committed messages=%d want=%d", len(seen), writers*messages)
	}
}

func TestConcurrentStoreLifecyclesNeverReuseMessageIDs(t *testing.T) {
	db := openSequenceTestDB(t)
	const writers, cycles = 8, 20
	ids := make(chan MessageID, writers*cycles)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			<-start
			for range cycles {
				store, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: "reopening", IDLeaseSize: 2})
				if err != nil {
					t.Error(err)
					return
				}
				id, enqueueErr := store.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "retained"})
				closeErr := store.Close()
				if enqueueErr != nil || closeErr != nil {
					t.Errorf("enqueue=%v close=%v", enqueueErr, closeErr)
					return
				}
				ids <- id
			}
		})
	}
	close(start)
	wg.Wait()
	close(ids)
	seen := make(map[MessageID]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("store close/reopen reused message ID %s", id)
		}
		seen[id] = true
	}
	if len(seen) != writers*cycles {
		t.Fatalf("committed messages=%d want=%d", len(seen), writers*cycles)
	}
}
