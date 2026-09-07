package maintenance

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type passDB struct{ run func() error }

func (d passDB) Flatten(int) error           { return nil }
func (d passDB) RunValueLogGC(float64) error { return d.run() }

type passClock struct{ now time.Time }

func (c *passClock) Now() time.Time                 { return c.now }
func (c *passClock) NewTicker(time.Duration) Ticker { panic("unused") }

func TestValueLogGCPassBudgets(t *testing.T) {
	failure := errors.New("GC failed")
	for _, tc := range []struct {
		name            string
		maxRuns         int
		budget, perCall time.Duration
		stopAt          int
		result          error
		cancelAt        int
		want            int
	}{
		{name: "catch up", stopAt: 4, result: badger.ErrNoRewrite, want: 4},
		{name: "default run budget", want: 8},
		{name: "run budget", maxRuns: 3, want: 3},
		{name: "time budget", budget: 10 * time.Millisecond, perCall: 6 * time.Millisecond, want: 2},
		{name: "default time budget", perCall: time.Second, want: 1},
		{name: "failure", stopAt: 2, result: failure, want: 2},
		{name: "cancellation", cancelAt: 1, want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			clock := &passClock{now: time.Unix(100, 0)}
			calls := 0
			results := make(chan Result, 16)
			db := passDB{run: func() error {
				calls++
				clock.now = clock.now.Add(tc.perCall)
				if calls == tc.cancelAt {
					cancel()
				}
				if calls == tc.stopAt {
					return tc.result
				}
				return nil
			}}
			m, err := newService(db, badger.DefaultOptions("test"), Options{
				ValueLogGCInterval: time.Second, ValueLogGCDiscardRatio: .5,
				ValueLogGCMaxRuns: tc.maxRuns, ValueLogGCMaxDuration: tc.budget,
				Clock: clock, Observer: observer{results},
			})
			if err != nil {
				t.Fatal(err)
			}
			m.runValueLogGCPass(ctx)
			if calls != tc.want || len(results) != tc.want {
				t.Fatalf("calls=%d observations=%d want=%d", calls, len(results), tc.want)
			}
			for i := 1; i <= calls; i++ {
				r := <-results
				if r.Operation != OperationValueLogGC || r.Duration != tc.perCall {
					t.Fatalf("result=%+v", r)
				}
				if i == tc.stopAt && !errors.Is(r.Err, tc.result) {
					t.Fatalf("error=%v", r.Err)
				}
			}
			cancel()
			m.runValueLogGCPass(ctx)
			if calls != tc.want {
				t.Fatal("GC started after cancellation")
			}
		})
	}
}

func TestRejectNegativeGCBudgets(t *testing.T) {
	for _, opts := range []Options{{ValueLogGCMaxRuns: -1}, {ValueLogGCMaxDuration: -time.Nanosecond}} {
		if err := ValidateOptions(badger.DefaultOptions("test"), opts); err == nil {
			t.Fatal("accepted negative budget")
		}
	}
}
