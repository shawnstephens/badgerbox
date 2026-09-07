package badgerbox_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

func ExampleStore_Usage() {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	limits := badgerbox.AdmissionLimits{MaxRetainedMessages: 1, MaxRetainedBytes: 1 << 20}
	store, err := badgerbox.New[string, string](db, badgerbox.Serde[string, string]{}, badgerbox.Options{
		Namespace: "bounded", AdmissionLimits: limits,
	})
	if err != nil {
		panic(err)
	}
	defer store.Close()
	ctx := context.Background()
	req := badgerbox.EnqueueRequest[string, string]{Payload: "event", Destination: "sink"}
	if _, err := store.Enqueue(ctx, req); err != nil {
		panic(err)
	}
	_, err = store.Enqueue(ctx, req)
	fmt.Println("capacity reached:", errors.Is(err, badgerbox.ErrAdmissionLimit))
	usage, err := store.Usage(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("retained messages:", usage.RetainedMessages)

	// Change the persisted limit explicitly; every Store of this namespace will
	// enforce it. A stale expected configuration fails instead of overwriting it.
	next := usage.Limits
	next.MaxRetainedMessages = 2
	if err := store.CompareAndSwapAdmissionLimits(ctx, usage.Limits, next); err != nil {
		panic(err)
	}
	if _, err := store.Enqueue(ctx, req); err != nil {
		panic(err)
	}
	usage, err = store.Usage(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Println("after tuning:", usage.RetainedMessages)
	// Output:
	// capacity reached: true
	// retained messages: 1
	// after tuning: 2
}
