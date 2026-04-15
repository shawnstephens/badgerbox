package badgerbox

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type testPayload struct {
	Name string `json:"name"`
}

type testDestination struct {
	Route string `json:"route"`
}

func openTestStore[M any, D any](t *testing.T, namespace string, serde Serde[M, D]) (*badger.DB, *Store[M, D], func()) {
	return openTestStoreWithOptions(t, namespace, serde, Options{})
}

func openTestStoreWithOptions[M any, D any](t *testing.T, namespace string, serde Serde[M, D], opts Options) (*badger.DB, *Store[M, D], func()) {
	t.Helper()

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}

	opts.Namespace = namespace
	store, err := New[M, D](db, serde, opts)
	if err != nil {
		_ = db.Close()
		t.Fatalf("new store: %v", err)
	}

	cleanup := func() {
		if err := store.Close(); err != nil {
			t.Fatalf("close store: %v", err)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("close db: %v", err)
		}
	}

	return db, store, cleanup
}

func runProcessor[M any, D any](processor *Processor[M, D]) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- processor.Run(ctx)
	}()
	return cancel, done
}

func stopProcessor(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("processor stopped with error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for processor shutdown")
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition not met before timeout")
}

func waitForChannel(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting on channel")
	}
}

func countKeysWithPrefix(t *testing.T, db *badger.DB, prefix []byte) int {
	t.Helper()

	count := 0
	err := db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			count++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("count keys: %v", err)
	}
	return count
}

func assertMessageDeleted(t *testing.T, store *Store[testPayload, testDestination]) {
	t.Helper()

	waitFor(t, func() bool {
		messageCount := countKeysWithPrefix(t, store.db, store.keys.messagePrefix)
		readyCount := countKeysWithPrefix(t, store.db, store.keys.readyPrefix)
		processingCount := countKeysWithPrefix(t, store.db, store.keys.processingPrefix)
		return messageCount == 0 && readyCount == 0 && processingCount == 0
	})
}

type fakeRuntime struct {
	mu              sync.Mutex
	now             time.Time
	tickers         []*fakeTicker
	tickerIntervals []time.Duration
	sleepCalls      []time.Duration
	sleepFunc       func(context.Context, time.Duration) error
	tokenFunc       func() (string, error)
	tokenCounter    int
}

func newFakeRuntime(now time.Time) *fakeRuntime {
	return &fakeRuntime{now: now.UTC()}
}

func (r *fakeRuntime) Now() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.now
}

func (r *fakeRuntime) SetNow(now time.Time) {
	r.mu.Lock()
	r.now = now.UTC()
	r.mu.Unlock()
}

func (r *fakeRuntime) Advance(delta time.Duration) time.Time {
	r.mu.Lock()
	r.now = r.now.Add(delta)
	now := r.now
	r.mu.Unlock()
	return now
}

func (r *fakeRuntime) Sleep(ctx context.Context, delay time.Duration) error {
	r.mu.Lock()
	r.sleepCalls = append(r.sleepCalls, delay)
	sleepFunc := r.sleepFunc
	r.mu.Unlock()
	if sleepFunc != nil {
		return sleepFunc(ctx, delay)
	}
	return nil
}

func (r *fakeRuntime) NewTicker(interval time.Duration) Ticker {
	ticker := &fakeTicker{
		ch:       make(chan time.Time, 1),
		interval: interval,
	}
	r.mu.Lock()
	r.tickers = append(r.tickers, ticker)
	r.tickerIntervals = append(r.tickerIntervals, interval)
	r.mu.Unlock()
	return ticker
}

func (r *fakeRuntime) NewLeaseToken() (string, error) {
	r.mu.Lock()
	tokenFunc := r.tokenFunc
	if tokenFunc == nil {
		r.tokenCounter++
		token := fmt.Sprintf("token-%d", r.tokenCounter)
		r.mu.Unlock()
		return token, nil
	}
	r.mu.Unlock()
	return tokenFunc()
}

func (r *fakeRuntime) Tick(index int) {
	r.mu.Lock()
	now := r.now
	ticker := r.tickers[index]
	r.mu.Unlock()
	ticker.tick(now)
}

func (r *fakeRuntime) TickAll() {
	r.mu.Lock()
	now := r.now
	tickers := append([]*fakeTicker(nil), r.tickers...)
	r.mu.Unlock()
	for _, ticker := range tickers {
		ticker.tick(now)
	}
}

func (r *fakeRuntime) TickerCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.tickers)
}

func (r *fakeRuntime) TickerIntervals() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.tickerIntervals...)
}

func (r *fakeRuntime) SleepCalls() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.sleepCalls...)
}

type fakeTicker struct {
	mu       sync.Mutex
	ch       chan time.Time
	interval time.Duration
	stopped  bool
}

func (t *fakeTicker) Chan() <-chan time.Time {
	return t.ch
}

func (t *fakeTicker) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

func (t *fakeTicker) tick(now time.Time) {
	t.mu.Lock()
	stopped := t.stopped
	t.mu.Unlock()
	if stopped {
		return
	}

	select {
	case t.ch <- now:
	default:
	}
}
