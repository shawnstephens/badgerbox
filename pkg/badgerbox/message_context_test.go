package badgerbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestSingleCancellationLeavesUninvokedMessageUntouched(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "single-cancel", Serde[string, string]{})
	defer cleanup()
	var ids []MessageID
	for range 2 {
		id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	started := make(chan MessageID, 1)
	p, err := NewProcessor(s, func(ctx context.Context, m Message[string, string]) error {
		started <- m.ID
		<-ctx.Done()
		return ctx.Err()
	}, ProcessorOptions{Concurrency: 1, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runProcessor(p)
	first := <-started
	stopProcessor(t, cancel, done)
	for _, id := range ids {
		if id == first {
			continue
		}
		m, err := s.Get(t.Context(), id)
		if err != nil || m.Attempt != 0 || m.State != MessageStateReady {
			t.Fatalf("uninvoked=%+v err=%v", m, err)
		}
	}
	q, err := s.QueueSnapshot(t.Context())
	if err != nil || q.DeadLetterDepth != 1 || q.ReadyDepth != 1 {
		t.Fatalf("snapshot=%+v err=%v", q, err)
	}
	select {
	case id := <-started:
		t.Fatalf("extra callback %v", id)
	default:
	}
}

func TestCanceledBatchBeforeInvocationRefundsClaims(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "before-invoke", Serde[string, string]{})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p, _ := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error {
		t.Error("callback invoked")
		return nil
	}, BatchProcessorOptions{})
	if err := p.processBatch(ctx, work); err != nil {
		t.Fatal(err)
	}
	m, err := s.Get(t.Context(), id)
	if err != nil || m.Attempt != 0 || m.State != MessageStateReady {
		t.Fatalf("message=%+v err=%v", m, err)
	}
}

func TestMessageContextAvailableDuringCallbacks(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		t.Run(mode, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			defer provider.Shutdown(t.Context())
			_, s, cleanup := openTestStoreWithOptions[string, string](t, "tracing", Serde[string, string]{}, Options{Observability: ObservabilityOptions{TracerProvider: provider, Propagator: propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})}})
			defer cleanup()
			for _, label := range []string{"one", "two"} {
				member, _ := baggage.NewMember("label", label)
				bag, _ := baggage.New(member)
				ctx := baggage.ContextWithBaggage(t.Context(), bag)
				if _, err := s.Enqueue(ctx, EnqueueRequest[string, string]{Payload: label}); err != nil {
					t.Fatal(err)
				}
			}
			work, err := s.claimReadyBatch(t.Context(), time.Now(), 2, time.Minute, 2)
			if err != nil {
				t.Fatal(err)
			}
			check := func(ctx context.Context, m Message[string, string]) error {
				if !trace.SpanContextFromContext(ctx).IsValid() {
					t.Error("callback has no trace")
				}
				if got := baggage.FromContext(ctx).Member("label").Value(); got != m.Payload {
					t.Errorf("baggage=%s want %s", got, m.Payload)
				}
				_, child := provider.Tracer("test").Start(ctx, "delivery")
				child.End()
				return errors.New("retry")
			}
			var p *BatchProcessor[string, string]
			if mode == "single" {
				single, _ := NewProcessor(s, check, ProcessorOptions{})
				p = single.batch
			} else {
				p, _ = NewBatchProcessor(s, func(ctx context.Context, messages []Message[string, string], results chan<- BatchProcessResult) error {
					derived, cancel := context.WithTimeout(ctx, time.Second)
					defer cancel()
					for _, m := range messages {
						mc := ContextForMessage(derived, m.ID)
						want, _ := derived.Deadline()
						got, _ := mc.Deadline()
						if !got.Equal(want) {
							t.Error("lost deadline")
						}
						results <- BatchProcessResult{ID: m.ID, Err: check(mc, m)}
					}
					cancel()
					if !errors.Is(ContextForMessage(derived, messages[0].ID).Err(), context.Canceled) {
						t.Error("lost cancellation")
					}
					return nil
				}, BatchProcessorOptions{})
			}
			if mode == "single" {
				for _, record := range work {
					if err := p.processBatch(t.Context(), []claimedRecord[string, string]{record}); err != nil {
						t.Fatal(err)
					}
				}
			} else if err := p.processBatch(t.Context(), work); err != nil {
				t.Fatal(err)
			}
			p.callbacks.Wait()
			enqueues := map[trace.SpanID]bool{}
			processes := map[trace.SpanID]bool{}
			var children []sdktrace.ReadOnlySpan
			for _, span := range recorder.Ended() {
				switch span.Name() {
				case "badgerbox.enqueue":
					enqueues[span.SpanContext().SpanID()] = true
				case "badgerbox.process":
					if processes[span.SpanContext().SpanID()] {
						t.Error("span ended twice")
					}
					processes[span.SpanContext().SpanID()] = true
				case "delivery":
					children = append(children, span)
				}
			}
			if len(processes) != 2 || len(children) != 2 {
				t.Fatalf("processes=%d children=%d", len(processes), len(children))
			}
			for _, span := range recorder.Ended() {
				if span.Name() == "badgerbox.process" && !enqueues[span.Parent().SpanID()] {
					t.Error("lost enqueue parent")
				}
			}
			for _, child := range children {
				if !processes[child.Parent().SpanID()] {
					t.Error("delivery not parented to processing span")
				}
			}
		})
	}
}
