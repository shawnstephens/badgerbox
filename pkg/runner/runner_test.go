package runner_test

import (
	"bytes"
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/maintenance"
	"github.com/shawnstephens/badgerbox/pkg/runner"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type binaryCodec struct{}

func (binaryCodec) Marshal(v []byte) ([]byte, error)   { return bytes.Clone(v), nil }
func (binaryCodec) Unmarshal(v []byte) ([]byte, error) { return bytes.Clone(v), nil }
func options(namespace string) runner.QueueOptions {
	return runner.QueueOptions{Store: badgerbox.Options{Namespace: namespace}, Processor: badgerbox.BatchProcessorOptions{ClaimBatchSize: 1, ProcessorOptions: badgerbox.ProcessorOptions{PollInterval: time.Millisecond}}}
}
func TestTypedQueuesAndOwnedShutdown(t *testing.T) {
	r, err := runner.Open(t.Context(), runner.Options{Badger: badger.DefaultOptions(t.TempDir()).WithLogger(nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Shutdown(context.Background())
	received := make(chan []byte, 1)
	a, err := runner.Register(r, badgerbox.Serde[[]byte, string]{Message: binaryCodec{}}, options("binary"), func(_ context.Context, m []badgerbox.Message[[]byte, string], results chan<- badgerbox.BatchProcessResult) error {
		received <- m[0].Payload
		results <- badgerbox.BatchProcessResult{ID: m[0].ID}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Register(r, badgerbox.Serde[int, int]{}, options("numbers"), func(_ context.Context, m []badgerbox.Message[int, int], results chan<- badgerbox.BatchProcessResult) error {
		for _, v := range m {
			results <- badgerbox.BatchProcessResult{ID: v.ID}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var flushes, closes atomic.Int32
	err = r.RegisterDelivery("shared", runner.DeliveryHooks{Flush: func(ctx context.Context) error {
		flushes.Add(1)
		q, e := a.QueueSnapshot(ctx)
		if e != nil {
			return e
		}
		if q.ProcessingDepth != 0 {
			return errors.New("processing remained during flush")
		}
		return nil
	}, Close: func() error { closes.Add(1); return nil }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.Register(r, badgerbox.Serde[[]byte, string]{}, options("binary"), func(context.Context, []badgerbox.Message[[]byte, string], chan<- badgerbox.BatchProcessResult) error {
		return nil
	})
	if err == nil {
		t.Fatal("duplicate queue accepted")
	}
	intake, cancel := context.WithCancel(t.Context())
	if err = r.Start(intake); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err = a.Enqueue(t.Context(), badgerbox.EnqueueRequest[[]byte, string]{Payload: []byte{255, 0, 128}, Destination: "arbitrary"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if !bytes.Equal(got, []byte{255, 0, 128}) {
			t.Fatal(got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("workers followed intake cancellation")
	}
	if err = r.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = r.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if flushes.Load() != 1 || closes.Load() != 1 {
		t.Fatalf("hooks: %d %d", flushes.Load(), closes.Load())
	}
	if _, err = a.QueueSnapshot(t.Context()); !errors.Is(err, badgerbox.ErrStoreClosed) {
		t.Fatal(err)
	}
}
func TestShutdownTimeoutKeepsDependenciesOpen(t *testing.T) {
	r, err := runner.Open(t.Context(), runner.Options{Badger: badger.DefaultOptions(t.TempDir()).WithLogger(nil)})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	s, err := runner.Register(r, badgerbox.Serde[string, string]{}, options("blocked"), func(ctx context.Context, _ []badgerbox.Message[string, string], _ chan<- badgerbox.BatchProcessResult) error {
		close(entered)
		<-release
		return ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	var closed atomic.Bool
	if err = r.RegisterDelivery("blocked-client", runner.DeliveryHooks{Close: func() error { closed.Store(true); return nil }}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Enqueue(t.Context(), badgerbox.EnqueueRequest[string, string]{}); err != nil {
		t.Fatal(err)
	}
	if err = r.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-entered
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err = r.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if closed.Load() {
		t.Fatal("dependency closed while callback active")
	}
	if _, err = s.QueueSnapshot(t.Context()); err != nil {
		t.Fatal("store closed during timeout:", err)
	}
	close(release)
	if err = r.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !closed.Load() {
		t.Fatal("dependency not closed")
	}
}
func TestInvalidMaintenanceDoesNotCreateFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "must-not-exist")
	_, err := runner.Open(t.Context(), runner.Options{Badger: badger.DefaultOptions(path), Maintenance: maintenance.Options{ValueLogGCInterval: -time.Second}})
	if err == nil {
		t.Fatal("invalid maintenance accepted")
	}
	if _, err = os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("directory created: %v", err)
	}
}
