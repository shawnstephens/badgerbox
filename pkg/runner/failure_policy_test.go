package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/telemetry"
)

func TestQueueFailurePolicies(t *testing.T) {
	for _, policy := range []QueueFailurePolicy{IsolateQueue, FailFast} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			r, err := Open(t.Context(), Options{Badger: badger.DefaultOptions(t.TempDir()).WithLogger(nil), QueueFailurePolicy: policy})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Shutdown(context.Background())
			cause := errors.New("decode failed")
			fail := make(chan struct{})
			ready := make(chan context.Context, 1)
			stopped := make(chan struct{})
			work, delivered := make(chan struct{}), make(chan struct{})
			var failures atomic.Int32
			r.queues = []queue{
				{namespace: "broken", run: func(context.Context) error { <-fail; failures.Add(1); return cause }, close: func() error { return nil }},
				{namespace: "healthy", run: func(ctx context.Context) error {
					defer close(stopped)
					ready <- ctx
					for {
						select {
						case <-ctx.Done():
							return nil
						case <-work:
							delivered <- struct{}{}
						}
					}
				}, close: func() error { return nil }},
			}
			if err = r.Start(t.Context()); err != nil {
				t.Fatal(err)
			}
			var healthyCtx context.Context
			select {
			case healthyCtx = <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("healthy queue did not start")
			}
			close(fail)
			select {
			case err = <-r.Errors():
				var qe *QueueError
				if !errors.Is(err, cause) || !errors.As(err, &qe) || qe.Namespace != "broken" {
					t.Fatalf("event=%v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("missing error event")
			}
			if policy == IsolateQueue {
				if err := healthyCtx.Err(); err != nil {
					t.Fatalf("healthy queue canceled: %v", err)
				}
				select {
				case work <- struct{}{}:
				case <-time.After(5 * time.Second):
					t.Fatal("healthy queue stopped accepting work")
				}
				select {
				case <-delivered:
				case <-time.After(5 * time.Second):
					t.Fatal("healthy queue stopped delivery")
				}
			} else {
				select {
				case <-stopped:
				case <-time.After(5 * time.Second):
					t.Fatal("fail-fast did not stop healthy queue")
				}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err = r.Shutdown(ctx); !errors.Is(err, cause) {
				t.Fatalf("shutdown=%v", err)
			}
			if failures.Load() != 1 {
				t.Fatal("failed queue restarted")
			}
			if _, ok := <-r.Errors(); ok {
				t.Fatal("error channel not closed")
			}
		})
	}
}

func TestAllQueueFailuresRetainedWithoutReadingNotifications(t *testing.T) {
	r, err := Open(t.Context(), Options{Badger: badger.DefaultOptions(t.TempDir()).WithLogger(nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	causes := []error{errors.New("first"), errors.New("second"), errors.New("third")}
	for i, cause := range causes {
		r.queues = append(r.queues, queue{namespace: fmt.Sprint(i), run: func(context.Context) error { return cause }, close: func() error { return nil }})
	}
	if err = r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for _, stop := range []func(context.Context) error{r.Stop, r.Shutdown, r.Shutdown} {
		err = stop(ctx)
		for i, cause := range causes {
			if !errors.Is(err, cause) || !strings.Contains(err.Error(), fmt.Sprintf("queue %q", fmt.Sprint(i))) {
				t.Fatalf("missing failure %d: %v", i, err)
			}
		}
	}
}

func TestInvalidRunnerOptionsBeforeOpen(t *testing.T) {
	for _, opts := range []Options{{QueueFailurePolicy: -1}, {QueueFailurePolicy: 2}, {Telemetry: telemetry.Options{DurationMaxWindow: -time.Second}}} {
		dir := filepath.Join(t.TempDir(), "not-created")
		opts.Badger = badger.DefaultOptions(dir)
		if r, err := Open(t.Context(), opts); err == nil {
			r.Shutdown(context.Background())
			t.Fatal("invalid options accepted")
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("database created: %v", err)
		}
	}
}

func TestNegativeQueueDurationWindowNotHiddenByInheritance(t *testing.T) {
	r, err := Open(t.Context(), Options{Badger: badger.DefaultOptions(t.TempDir()).WithLogger(nil), Telemetry: telemetry.Options{DurationMaxWindow: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	_, err = Register(r, badgerbox.Serde[int, int]{}, QueueOptions{Store: badgerbox.Options{Namespace: "bad", Observability: telemetry.Options{DurationMaxWindow: -time.Second}}}, func(context.Context, []badgerbox.Message[int, int], chan<- badgerbox.BatchProcessResult) error {
		return nil
	})
	if err == nil {
		t.Fatal("negative queue window accepted")
	}
}
