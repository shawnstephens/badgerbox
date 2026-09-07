package maintenance

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

type testTicker struct {
	c       chan time.Time
	stopped atomic.Bool
}

func (t *testTicker) Chan() <-chan time.Time { return t.c }
func (t *testTicker) Stop()                  { t.stopped.Store(true) }

type testClock struct{ ticker *testTicker }

func (c testClock) Now() time.Time                 { return time.Now() }
func (c testClock) NewTicker(time.Duration) Ticker { return c.ticker }

type testDB struct {
	entered, release chan struct{}
	calls            atomic.Int32
	flatten          atomic.Int32
}

func (d *testDB) Flatten(n int) error { d.flatten.Store(int32(n)); return nil }
func (d *testDB) RunValueLogGC(float64) error {
	d.calls.Add(1)
	close(d.entered)
	<-d.release
	return badger.ErrNoRewrite
}

type observer struct{ results chan Result }

func (o observer) ObserveMaintenance(_ context.Context, r Result) { o.results <- r }
func TestStopJoinsInFlightMaintenance(t *testing.T) {
	db := &testDB{entered: make(chan struct{}), release: make(chan struct{})}
	ticker := &testTicker{c: make(chan time.Time, 1)}
	results := make(chan Result, 2)
	m, err := newService(db, badger.DefaultOptions("test"), Options{FlattenOnStartup: true, ValueLogGCInterval: time.Second, ValueLogGCDiscardRatio: .5, Clock: testClock{ticker}, Observer: observer{results}})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.RunStartup(t.Context()); err != nil {
		t.Fatal(err)
	}
	if db.flatten.Load() < 1 {
		t.Fatal("flatten not called")
	}
	if err = m.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err = m.Start(t.Context()); err == nil {
		t.Fatal("duplicate start")
	}
	ticker.c <- time.Now()
	<-db.entered
	done := make(chan struct{})
	go func() { m.Stop(); close(done) }()
	select {
	case <-done:
		t.Fatal("stop returned during database call")
	case <-time.After(10 * time.Millisecond):
	}
	close(db.release)
	<-done
	m.Stop()
	if !ticker.stopped.Load() || db.calls.Load() != 1 {
		t.Fatal("worker not joined")
	}
	<-results
	r := <-results
	if r.Err != badger.ErrNoRewrite || r.Operation != OperationValueLogGC {
		t.Fatal(r)
	}
}
func TestValidationBeforeOpeningDatabase(t *testing.T) {
	for _, ratio := range []float64{-1, 0, 1, 2, math.NaN()} {
		if err := ValidateOptions(badger.DefaultOptions("test"), Options{ValueLogGCInterval: time.Second, ValueLogGCDiscardRatio: ratio}); err == nil {
			t.Fatalf("accepted ratio %v", ratio)
		}
	}
	for _, db := range []badger.Options{badger.DefaultOptions("").WithInMemory(true), badger.DefaultOptions("test").WithReadOnly(true)} {
		if err := ValidateOptions(db, Options{ValueLogGCInterval: time.Second, ValueLogGCDiscardRatio: .5}); err == nil {
			t.Fatal("accepted unsupported database")
		}
	}
	if err := ValidateOptions(badger.DefaultOptions("test"), Options{}); err != nil {
		t.Fatal(err)
	}
}
