package runner_test

import (
	"context"
	"runtime"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/runner"
)

func TestImmediateShutdownHasNoQueueFailures(t *testing.T) {
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)
	r, err := runner.Open(t.Context(), runner.Options{Badger: badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	_, err = runner.Register(r, badgerbox.Serde[int, int]{}, runner.QueueOptions{Store: badgerbox.Options{Namespace: "healthy"}}, func(context.Context, []badgerbox.Message[int, int], chan<- badgerbox.BatchProcessResult) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, stop := range []func(context.Context) error{r.Stop, r.Shutdown, r.Shutdown} {
		if err = stop(t.Context()); err != nil {
			t.Fatalf("healthy shutdown reported a queue failure: %v", err)
		}
	}
	for err := range r.Errors() {
		t.Errorf("healthy queue emitted failure: %v", err)
	}
}
