package badgerbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

type batchDrainTestCounter struct {
	metric.Int64Counter
	onAdd func()
}

func (c batchDrainTestCounter) Add(context.Context, int64, ...metric.AddOption) {
	c.onAdd()
}

type batchDrainTestProvider struct {
	metric.MeterProvider
	hooks sync.Map
}

func (p *batchDrainTestProvider) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	return batchDrainTestMeter{Meter: p.MeterProvider.Meter(name, opts...), provider: p}
}

type batchDrainTestMeter struct {
	metric.Meter
	provider *batchDrainTestProvider
}

func (m batchDrainTestMeter) Int64Counter(name string, opts ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	c, err := m.Meter.Int64Counter(name, opts...)
	return batchDrainTestCounter{Int64Counter: c, onAdd: func() {
		if hook, ok := m.provider.hooks.Load(name); ok {
			hook.(func())()
		}
	}}, err
}

func TestBatchFinalDrainDoesNotFollowRefilledResults(t *testing.T) {
	probe := &batchDrainTestProvider{MeterProvider: noop.NewMeterProvider()}
	db, s, cleanup := openTestStoreWithOptions[string, string](t, "bounded-drain", Serde[string, string]{}, Options{Observability: ObservabilityOptions{MeterProvider: probe}})
	defer cleanup()
	for range 3 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now(), 3, time.Minute, 10)
	if err != nil || len(work) != 3 {
		t.Fatalf("claim=%v err=%v", work, err)
	}
	corruptRecordTestValue(t, db, s.keys.messageKey(work[0].Message.ID), func(data []byte) []byte {
		return changeRecordTestField(t, data, "attempt", nil, true)
	})
	pending := make(map[MessageID]claimedRecord[string, string])
	for _, record := range work {
		pending[record.Message.ID] = record
	}
	results := make(chan BatchProcessResult, 3)
	results <- BatchProcessResult{ID: work[0].Message.ID}
	results <- BatchProcessResult{ID: work[1].Message.ID}
	results <- BatchProcessResult{ID: 999}
	invalidResults := 0

	probe.hooks.Store("badgerbox_batch_result_invalid_total", func() {
		invalidResults++
		// Refill synchronously when a result is consumed so this regression is
		// deterministic, without relying on scheduling a fast producer goroutine.
		// Stop eventually so the unfixed drain can fail the test without hanging.
		if invalidResults < 16 {
			results <- BatchProcessResult{ID: 999}
		}
	})
	p, err := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, BatchProcessorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	traceCtx, endTraces := p.startMessageTraces(t.Context(), work, time.Now())
	defer endTraces()
	ctx, cancel := context.WithCancel(traceCtx)
	cancel()
	if err := p.cancelPendingBatchResults(ctx, results, pending, time.Now()); err == nil {
		t.Fatal("drain lost the corrupt record's settlement error")
	}
	if invalidResults != 1 || len(results) != 1 {
		t.Errorf("drain consumed results arriving after shutdown: invalid=%d buffered=%d", invalidResults, len(results))
	}
	if len(pending) != 0 {
		t.Errorf("unsettled pending records: %d", len(pending))
	}
	if _, err := s.Get(t.Context(), work[1].Message.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("buffered success was not acknowledged: %v", err)
	}
	if q, err := s.QueueSnapshot(t.Context()); err != nil || q.ProcessingDepth != 1 || q.ReadyDepth != 1 {
		t.Fatalf("failed lease or unresolved retry lost: snapshot=%+v err=%v", q, err)
	}
}

func TestBatchCancelsCallbackBeforeTerminalSettlement(t *testing.T) {
	for _, mode := range []string{"error", "panic", "cancel", "timeout", "closed"} {
		t.Run(mode, func(t *testing.T) {
			runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
			probe := &batchDrainTestProvider{MeterProvider: noop.NewMeterProvider()}
			_, s, cleanup := openTestStoreWithOptions(t, mode, Serde[string, string]{}, Options{Runtime: runtime, Observability: ObservabilityOptions{MeterProvider: probe}})
			defer cleanup()
			for range 2 {
				if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "payload"}); err != nil {
					t.Fatal(err)
				}
			}
			work, err := s.claimReadyBatch(t.Context(), runtime.Now(), 2, 100*time.Millisecond, 10)
			if err != nil || len(work) != 2 {
				t.Fatalf("claim=%v err=%v", work, err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			started := make(chan struct{})
			var callbackCtx context.Context
			missingCalls := 0

			probe.hooks.Store("badgerbox_batch_result_missing_total", func() {
				<-started
				missingCalls++
				if callbackCtx.Err() == nil {
					t.Error("terminal settlement began while the callback context was still active")
				}
			})
			p, err := NewBatchProcessor(s, func(processCtx context.Context, messages []Message[string, string], results chan<- BatchProcessResult) error {
				callbackCtx = processCtx
				close(started)
				results <- BatchProcessResult{ID: messages[0].ID}
				switch mode {
				case "error":
					return errors.New("callback failed")
				case "panic":
					panic("callback failed")
				case "cancel":
					cancel()
				case "closed":
					close(results)
				}
				return nil
			}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{SettlementTimeout: time.Second}})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.processBatch(ctx, work); err != nil {
				t.Fatal(err)
			}
			if missingCalls != 1 {
				t.Fatalf("missing-result settlements=%d", missingCalls)
			}
			if _, err := s.Get(t.Context(), work[0].Message.ID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("explicit success was not acknowledged: %v", err)
			}
			if q, err := s.QueueSnapshot(t.Context()); err != nil || q.ProcessingDepth != 0 || q.ReadyDepth != 1 {
				t.Fatalf("unresolved work was not retried: snapshot=%+v err=%v", q, err)
			}
		})
	}
}
