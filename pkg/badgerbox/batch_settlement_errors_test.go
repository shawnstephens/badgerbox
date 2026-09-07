package badgerbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestBatchSettlementContinuesAfterRecordErrors(t *testing.T) {
	for _, mode := range []string{"results", "callback-error", "panic", "closed", "cancel", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			db, s, cleanup := openTestStore[string, string](t, "settlement-errors", Serde[string, string]{})
			defer cleanup()
			for range 4 {
				if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "payload"}); err != nil {
					t.Fatal(err)
				}
			}
			lease := time.Second
			if mode == "timeout" {
				lease = 100 * time.Millisecond
			}
			work, err := s.claimReadyBatch(t.Context(), time.Now(), 4, lease, 10)
			if err != nil || len(work) != 4 {
				t.Fatalf("claim=%v err=%v", work, err)
			}
			original := recordTestNamespace(t, db, s.opts.Namespace)
			// Corrupt only two records after claim, creating independent settlement
			// failures without breaking the DB or the remaining claimed records.
			for _, record := range work[:2] {
				corruptRecordTestValue(t, db, s.keys.messageKey(record.Message.ID), func(data []byte) []byte {
					return changeRecordTestField(t, data, "created_at_unix_nano", nil, true)
				})
			}
			corrupted := recordTestNamespace(t, db, s.opts.Namespace)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			p, err := NewBatchProcessor(s, func(_ context.Context, messages []Message[string, string], results chan<- BatchProcessResult) error {
				results <- BatchProcessResult{ID: messages[0].ID, Err: Permanent(errors.New("failed"))}
				results <- BatchProcessResult{ID: messages[1].ID}
				results <- BatchProcessResult{ID: messages[2].ID}
				switch mode {
				case "results":
					results <- BatchProcessResult{ID: messages[3].ID, Err: errors.New("retry")}
				case "callback-error":
					return errors.New("callback failed")
				case "panic":
					panic("callback failed")
				case "closed":
					close(results)
				case "cancel":
					cancel()
				}
				return nil
			}, ProcessorOptions{BatchSettlementTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			err = p.processBatch(ctx, work)
			for _, record := range work[:2] {
				if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("message %s:", record.Message.ID)) {
					t.Fatalf("missing settlement error for %s: %v", record.Message.ID, err)
				}
			}
			if _, err := s.Get(t.Context(), work[2].Message.ID); !errors.Is(err, ErrNotFound) {
				t.Fatalf("explicit success was not acknowledged: %v", err)
			}
			if q, err := s.QueueSnapshot(t.Context()); err != nil || q.ProcessingDepth != 2 || q.ReadyDepth != 1 || q.DeadLetterDepth != 0 {
				t.Fatalf("snapshot=%+v err=%v", q, err)
			}
			after := recordTestNamespace(t, db, s.opts.Namespace)
			for _, record := range work[:2] {
				for _, key := range [][]byte{s.keys.messageKey(record.Message.ID), s.keys.processingKey(record.LeaseUntil, record.Message.ID), s.keys.processingCreatedKey(record.Message.CreatedAt, record.Message.ID)} {
					value, present := after[string(key)]
					if !present || !bytes.Equal(value, corrupted[string(key)]) {
						t.Fatalf("failed settlement changed %q", key)
					}
				}
				// Once the local corruption is repaired, the unchanged lease can be
				// recovered without replaying the already acknowledged success.
				key := s.keys.messageKey(record.Message.ID)
				corruptRecordTestValue(t, db, key, func([]byte) []byte { return original[string(key)] })
			}
			if n, err := s.requeueExpired(t.Context(), time.Now().Add(2*time.Second), 4); err != nil || n != 2 {
				t.Fatalf("recover=%d err=%v", n, err)
			}
		})
	}
}

func TestPendingBatchFailuresAreAggregated(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "pending-errors", Serde[string, string]{})
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
	pending := make(map[MessageID]claimedRecord[string, string])
	for _, record := range work {
		pending[record.Message.ID] = record
	}
	for _, record := range work[:2] {
		corruptRecordTestValue(t, db, s.keys.messageKey(record.Message.ID), func(data []byte) []byte { return changeRecordTestField(t, data, "attempt", nil, true) })
	}
	p, err := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, ProcessorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = p.failPendingBatchResults(t.Context(), pending, time.Now(), errors.New("callback failed"))
	for _, record := range work[:2] {
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("message %s:", record.Message.ID)) {
			t.Fatalf("missing error for %s: %v", record.Message.ID, err)
		}
	}
	if len(pending) != 0 {
		t.Fatalf("%d outcomes were not attempted", len(pending))
	}
	if message, err := s.Get(t.Context(), work[2].Message.ID); err != nil || message.State != MessageStateReady {
		t.Fatalf("healthy pending message was not retried: %+v %v", message, err)
	}
}

func TestBatchLargeFailureDoesNotAbandonSuccess(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueLogFileSize(1 << 20))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New[string, string](db, Serde[string, string]{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 2 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "payload"}); err != nil {
			t.Fatal(err)
		}
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now(), 2, time.Minute, 10)
	if err != nil || len(work) != 2 {
		t.Fatalf("claim=%v err=%v", work, err)
	}
	p, err := NewBatchProcessor(s, func(_ context.Context, messages []Message[string, string], results chan<- BatchProcessResult) error {
		results <- BatchProcessResult{ID: messages[0].ID, Err: Permanent(errors.New(strings.Repeat("x", 2<<20)))}
		results <- BatchProcessResult{ID: messages[1].ID}
		return nil
	}, ProcessorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.processBatch(t.Context(), work); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get(t.Context(), work[1].Message.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("success not acknowledged: %v", err)
	}
	letters, _, err := s.ListDeadLetters(t.Context(), 2, nil)
	if err != nil || len(letters) != 1 {
		t.Fatalf("letters=%d err=%v", len(letters), err)
	}
	if !letters[0].Permanent || len(letters[0].Error) > maxStoredErrorBytes || !strings.HasSuffix(letters[0].Error, "[truncated]") {
		t.Fatal("large failure did not produce a bounded permanent dead letter")
	}
	if q, err := s.QueueSnapshot(t.Context()); err != nil || q.ProcessingDepth != 0 || q.DeadLetterDepth != 1 {
		t.Fatalf("snapshot=%+v err=%v", q, err)
	}
}
