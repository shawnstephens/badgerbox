package runner

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

func TestFailFastDoesNotReportHealthyStartupCancellation(t *testing.T) {
	r, err := Open(t.Context(), Options{Badger: badger.DefaultOptions("").WithInMemory(true).WithLogger(nil), QueueFailurePolicy: FailFast})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	_, err = Register(r, badgerbox.Serde[int, int]{}, QueueOptions{Store: badgerbox.Options{Namespace: "healthy"}}, func(context.Context, []badgerbox.Message[int, int], chan<- badgerbox.BatchProcessResult) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Hold the real processor at its entry boundary until fail-fast cancels it.
	run := r.queues[0].run
	r.queues[0].run = func(ctx context.Context) error { <-ctx.Done(); return run(ctx) }
	cause := errors.New("broken queue")
	r.queues = append(r.queues, queue{namespace: "broken", run: func(context.Context) error { return cause }, close: func() error { return nil }})
	if err = r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	joined := make(chan struct{})
	go func() { r.workers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(5 * time.Second):
		t.Fatal("fail-fast did not cancel startup")
	}
	if err = r.Shutdown(t.Context()); !errors.Is(err, cause) || strings.Contains(err.Error(), "healthy") {
		t.Fatalf("shutdown=%v", err)
	}
	for err := range r.Errors() {
		var queueErr *QueueError
		if !errors.As(err, &queueErr) || queueErr.Namespace != "broken" {
			t.Fatalf("notification=%v", err)
		}
	}
}

func TestCanceledRunnerRetainsWrappedAndJoinedFailures(t *testing.T) {
	cause := errors.New("storage failed")
	for _, failure := range []error{
		fmt.Errorf("settlement: %w", context.Canceled),
		errors.Join(context.Canceled),
		errors.Join(context.Canceled, cause),
		fmt.Errorf("settlement: %w", context.DeadlineExceeded),
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			r, err := Open(t.Context(), Options{Badger: badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Shutdown(context.Background())
			r.queues = []queue{{namespace: "failed", run: func(ctx context.Context) error { <-ctx.Done(); return failure }, close: func() error { return nil }}}
			if err = r.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err = r.Shutdown(t.Context()); !errors.Is(err, failure) {
				t.Fatalf("lost failure: %v", err)
			}
			if event := <-r.Errors(); !errors.Is(event, failure) {
				t.Fatalf("lost notification: %v", event)
			}
		})
	}
}

func TestActiveRunnerRetainsBareCancellationFailure(t *testing.T) {
	r, err := Open(t.Context(), Options{Badger: badger.DefaultOptions("").WithInMemory(true).WithLogger(nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	r.queues = []queue{{namespace: "failed", run: func(context.Context) error { return context.Canceled }, close: func() error { return nil }}}
	if err = r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-r.Errors():
		if !errors.Is(event, context.Canceled) {
			t.Fatal(event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("failure suppressed while runner active")
	}
	if err = r.Shutdown(t.Context()); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown=%v", err)
	}
}
