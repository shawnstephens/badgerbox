package badgerbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRunReportsSettlementTimeoutAndJoinsCallbacks(t *testing.T) {
	for _, mode := range []string{"single", "batch"} {
		for _, stop := range []string{"cancel", "deadline"} {
			t.Run(mode+"/"+stop, func(t *testing.T) {
				_, s, cleanup := openTestStore[string, string](t, "shutdown-timeout", Serde[string, string]{})
				defer cleanup()
				id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{})
				if err != nil {
					t.Fatal(err)
				}
				started, release := make(chan struct{}), make(chan struct{})
				defer func() {
					select {
					case <-release:
					default:
						close(release)
					}
				}()
				callback := func(ctx context.Context, _ Message[string, string]) error {
					close(started)
					<-ctx.Done()
					<-release
					return nil
				}
				opts := ProcessorOptions{Concurrency: 1, SettlementTimeout: time.Nanosecond, LeaseDuration: time.Minute}
				var run func(context.Context) error
				if mode == "single" {
					p, err := NewProcessor(s, callback, opts)
					if err != nil {
						t.Fatal(err)
					}
					run = p.Run
				} else {
					p, err := NewBatchProcessor(s, func(ctx context.Context, messages []Message[string, string], results chan<- BatchProcessResult) error {
						results <- BatchProcessResult{ID: messages[0].ID, Err: callback(ctx, messages[0])}
						return nil
					}, BatchProcessorOptions{ProcessorOptions: opts, ClaimBatchSize: 1})
					if err != nil {
						t.Fatal(err)
					}
					run = p.Run
				}
				ctx, cancel := context.WithCancel(t.Context())
				if stop == "deadline" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), 250*time.Millisecond)
				}
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- run(ctx) }()
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("callback did not start")
				}
				if stop == "cancel" {
					cancel()
				}
				<-ctx.Done()
				select {
				case err := <-done:
					t.Fatalf("returned before callback joined: %v", err)
				case <-time.After(20 * time.Millisecond):
				}
				close(release)
				select {
				case err = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("Run did not finish")
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("lost settlement timeout: %v", err)
				}
				m, err := s.Get(t.Context(), id)
				if err != nil || m.State != MessageStateProcessing {
					t.Fatalf("failed settlement changed record: %+v %v", m, err)
				}
				if n, err := s.requeueExpired(t.Context(), time.Now().Add(2*time.Minute), 1); err != nil || n != 1 {
					t.Fatalf("lease not recoverable: %d %v", n, err)
				}
			})
		}
	}
}

func TestRunAggregatesWorkerSettlementFailures(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "worker-errors", Serde[string, string]{})
	defer cleanup()
	for range 2 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan MessageID, 2)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	p, err := NewProcessor(s, func(ctx context.Context, m Message[string, string]) error {
		started <- m.ID
		<-ctx.Done()
		<-release
		return nil
	}, ProcessorOptions{Concurrency: 2, LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	var ids []MessageID
	for range 2 {
		select {
		case id := <-started:
			ids = append(ids, id)
		case <-time.After(3 * time.Second):
			t.Fatal("workers did not start")
		}
	}
	for _, id := range ids {
		corruptRecordTestValue(t, db, s.keys.messageKey(id), func(data []byte) []byte { return changeRecordTestField(t, data, "created_at_unix_nano", nil, true) })
	}
	cancel()
	close(release)
	select {
	case err = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not finish")
	}
	for _, id := range ids {
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("message %s:", id)) {
			t.Fatalf("missing worker error for %s: %v", id, err)
		}
	}
}

func TestClaimReleaseTimeoutIsReportableDuringCancellation(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "release-timeout", Serde[string, string]{})
	defer cleanup()
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
		t.Fatal(err)
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 1)
	if err != nil || len(work) != 1 {
		t.Fatalf("claim: %v %v", work, err)
	}
	p, err := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error {
		t.Error("callback invoked")
		return nil
	}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{SettlementTimeout: time.Nanosecond}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = p.processBatch(ctx, work)
	if !errors.Is(err, context.DeadlineExceeded) || !reportableLoopError(ctx, err) {
		t.Fatalf("release timeout suppressed: %v", err)
	}
}

func TestQueueDrainContinuesAfterReleaseFailure(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "drain-errors", Serde[string, string]{})
	defer cleanup()
	for range 2 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
			t.Fatal(err)
		}
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now(), 2, time.Minute, 1)
	if err != nil || len(work) != 2 {
		t.Fatalf("claim: %v %v", work, err)
	}
	corruptRecordTestValue(t, db, s.keys.messageKey(work[0].Message.ID), func(data []byte) []byte { return changeRecordTestField(t, data, "attempt", nil, true) })
	p, err := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, BatchProcessorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	workCh := make(chan []claimedRecord[string, string], 2)
	slots := make(chan struct{}, 2)
	for _, record := range work {
		workCh <- []claimedRecord[string, string]{record}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.drainQueuedWork(ctx, workCh, slots); err == nil {
		t.Fatal("release failure lost")
	}
	if len(workCh) != 0 || len(slots) != 2 {
		t.Fatal("drain stopped at failed release")
	}
	m, err := s.Get(t.Context(), work[1].Message.ID)
	if err != nil || m.State != MessageStateReady || m.Attempt != 0 {
		t.Fatalf("healthy claim not refunded: %+v %v", m, err)
	}
}

func TestLoopCancellationCannotHideJoinedFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	storageErr := errors.New("storage failure")
	for _, err := range []error{
		errors.Join(context.Canceled, storageErr),
		fmt.Errorf("worker: %w", errors.Join(storageErr, context.DeadlineExceeded)),
		errors.Join(context.Canceled, markSettlementError(context.DeadlineExceeded)),
		markSettlementError(context.Canceled),
	} {
		if !reportableLoopError(ctx, err) {
			t.Errorf("suppressed failure: %v", err)
		}
	}
	if reportableLoopError(ctx, errors.Join(context.Canceled, fmt.Errorf("reaper: %w", context.DeadlineExceeded))) {
		t.Fatal("normal loop cancellation became an error")
	}
	if !reportableLoopError(t.Context(), context.Canceled) {
		t.Fatal("unexpected cancellation suppressed while running")
	}
	wrapped := markSettlementError(storageErr)
	var target settlementError
	if !errors.Is(wrapped, storageErr) || !errors.As(wrapped, &target) {
		t.Fatal("settlement wrapper lost error identity")
	}
}
