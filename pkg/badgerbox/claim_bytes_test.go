package badgerbox

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func storedItemBytes(t *testing.T, db *badger.DB, key []byte) ([]byte, int64) {
	t.Helper()
	var value []byte
	var size int64
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		size = storedValueUpperBound(item)
		value, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return value, size
}

func TestClaimByteBudgetMixedSizesAndReferencedReplay(t *testing.T) {
	codec := &quarantineCodec{}
	db, s, cleanup := openTestStore(t, "mixed-claim-bytes", Serde[string, string]{Message: codec, Destination: codec})
	defer cleanup()
	var ids []MessageID
	originals := make(map[MessageID][]byte)
	sizes := make(map[MessageID]int64)
	for _, size := range []int{80, 700, 50, 32 << 10, 400, 50} {
		id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", size), Destination: "route"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
		originals[id], sizes[id] = storedItemBytes(t, db, s.keys.messageKey(id))
	}
	limit := sizes[ids[1]] + sizes[ids[2]]
	seen := map[MessageID]bool{}
	for range len(ids) + 1 {
		work, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 10, limit, time.Minute, 3)
		if err != nil {
			t.Fatal(err)
		}
		var total int64
		for _, record := range work {
			id := record.Message.ID
			if seen[id] {
				t.Fatalf("duplicate claim %s", id)
			}
			seen[id] = true
			total += sizes[id]
			if err := s.acknowledge(t.Context(), id, record.LeaseToken); err != nil {
				t.Fatal(err)
			}
		}
		if total > limit {
			t.Fatalf("claim read %d bytes > %d", total, limit)
		}
	}
	oversize := ids[3]
	if len(seen) != len(ids)-1 || seen[oversize] {
		t.Fatalf("claims=%v", seen)
	}
	remaining, _ := storedItemBytes(t, db, s.keys.messageKey(oversize))
	if !bytes.Equal(remaining, originals[oversize]) {
		t.Fatal("quarantine rewrote oversized source")
	}
	if _, err := s.Get(t.Context(), oversize); !errors.Is(err, ErrMessageQuarantined) {
		t.Fatalf("Get error=%v", err)
	}
	q, err := s.QueueSnapshot(t.Context())
	if err != nil || q.ReadyDepth != 0 || q.ProcessingDepth != 0 || q.DeadLetterDepth != 1 || q.OldestReadyAge != 0 {
		t.Fatalf("snapshot=%+v err=%v", q, err)
	}
	calls := codec.calls.Load()
	rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 10, MaxBytes: 1024})
	if err != nil || len(rows) != 1 || rows[0].Details != nil || rows[0].QuarantinedSource == nil {
		t.Fatalf("metadata=%+v err=%v", rows, err)
	}
	if codec.calls.Load() != calls || rows[0].QuarantinedSource.StoredBytes != sizes[oversize] || !strings.Contains(rows[0].QuarantinedSource.FailureText, "claim byte limit") {
		t.Fatalf("reference=%+v", rows[0].QuarantinedSource)
	}
	audit, err := s.Audit(t.Context(), AuditOptions{})
	if err != nil || !audit.Complete || audit.LiveRows != 0 || audit.DeadLetters.Rows != 1 || audit.DeadLetters.ReferencedRows != 1 || len(audit.Samples.Anomalies) != 0 {
		t.Fatalf("audit=%+v err=%v", audit, err)
	}
	if report, err := s.Audit(t.Context(), AuditOptions{MaxScannedBytes: 1024}); !errors.Is(err, ErrAuditLimitExceeded) || report.Complete {
		t.Fatalf("budget report=%+v err=%v", report, err)
	}
	before := recordTestNamespace(t, db, s.opts.Namespace)
	budget := rows[0].StoredBytes + rows[0].QuarantinedSource.StoredBytes
	if _, _, err := s.ListDeadLettersWithOptions(t.Context(), DeadLetterListOptions{Limit: 10, MaxBytes: budget - 1}); !errors.Is(err, ErrDeadLetterTooLarge) {
		t.Fatalf("list err=%v", err)
	}
	if err := s.RequeueDeadLetterWithOptions(t.Context(), oversize, rows[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: budget - 1}); !errors.Is(err, ErrDeadLetterTooLarge) {
		t.Fatalf("requeue err=%v", err)
	}
	if codec.calls.Load() != calls {
		t.Fatal("rejected read invoked codecs")
	}
	if after := recordTestNamespace(t, db, s.opts.Namespace); !reflect.DeepEqual(before, after) {
		t.Fatal("bounded rejection changed source or quarantine")
	}
	letters, _, err := s.ListDeadLettersWithOptions(t.Context(), DeadLetterListOptions{Limit: 10, MaxBytes: budget})
	if err != nil || len(letters) != 1 || letters[0].Message.ID != oversize || letters[0].Message.Payload != strings.Repeat("x", 32<<10) {
		t.Fatalf("full letters=%d err=%v", len(letters), err)
	}
	calls = codec.calls.Load()
	if err := s.RequeueDeadLetterWithOptions(t.Context(), oversize, rows[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: budget}); err != nil {
		t.Fatal(err)
	}
	if codec.calls.Load() != calls {
		t.Fatal("requeue decoded payload")
	}
	replayed, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 10, budget, time.Minute, 3)
	if err != nil || len(replayed) != 1 || replayed[0].Message.ID != oversize || replayed[0].Message.Payload != strings.Repeat("x", 32<<10) {
		t.Fatalf("replayed=%d err=%v", len(replayed), err)
	}
	if err := s.acknowledge(t.Context(), oversize, replayed[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	if q, err := s.QueueSnapshot(t.Context()); err != nil || q != (QueueSnapshot{}) {
		t.Fatalf("remaining=%+v err=%v", q, err)
	}
	if n := countKeysWithPrefix(t, db, s.keys.quarantinePrefix); n != 0 {
		t.Fatalf("remaining markers=%d", n)
	}
}

func TestHugeMalformedHeadIsQuarantinedBeforeCopying(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "huge-head", Serde[string, string]{})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "old"})
	if err != nil {
		t.Fatal(err)
	}
	huge := bytes.Repeat([]byte("z"), 16<<20)
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.messageKey(id), huge) }); err != nil {
		t.Fatal(err)
	}
	healthy, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "healthy"})
	if err != nil {
		t.Fatal(err)
	}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	work, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 10, 1024, time.Minute, 3)
	runtime.ReadMemStats(&after)
	if err != nil || len(work) != 1 || work[0].Message.ID != healthy {
		t.Fatalf("work=%v err=%v", work, err)
	}
	// Generous allocation headroom for Badger bookkeeping still rules out even
	// one copy of the 16 MiB source. Its malformed envelope also proves no decode.
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 4<<20 {
		t.Fatalf("oversized source allocated %d bytes", allocated)
	}
	rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 1, MaxBytes: 1024})
	if err != nil || len(rows) != 1 || rows[0].QuarantinedSource == nil {
		t.Fatalf("metadata=%+v err=%v", rows, err)
	}
	if _, err := s.Get(t.Context(), id); !errors.Is(err, ErrMessageQuarantined) {
		t.Fatalf("Get err=%v", err)
	}
	state := recordTestNamespace(t, db, s.opts.Namespace)
	if err := s.RequeueDeadLetterWithOptions(t.Context(), id, rows[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: 20 << 20}); err == nil {
		t.Fatal("replayed malformed source")
	}
	if next := recordTestNamespace(t, db, s.opts.Namespace); !reflect.DeepEqual(state, next) {
		t.Fatal("invalid replay changed source")
	}
}

func TestClaimBudgetHandlesBadgerValueLogUnderestimate(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: "q"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "old"})
	if err != nil {
		t.Fatal(err)
	}
	value := bytes.Repeat([]byte("x"), 100)
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.messageKey(id), value) }); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(s.keys.messageKey(id))
		if err != nil {
			return err
		}
		if item.ValueSize() != 99 || storedValueUpperBound(item) != 100 {
			t.Fatalf("value estimate=%d bound=%d", item.ValueSize(), storedValueUpperBound(item))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if work, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 1, 99, time.Minute, 3); err != nil || len(work) != 0 {
		t.Fatalf("work=%v err=%v", work, err)
	}
	rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 1})
	if err != nil || len(rows) != 1 || rows[0].QuarantinedSource == nil {
		t.Fatalf("metadata=%v err=%v", rows, err)
	}
}

func TestByteLimitedDispatchDoesNotWaitForPoll(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "byte-dispatch", Serde[string, string]{})
	defer cleanup()
	for range 3 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", 400)}); err != nil {
			t.Fatal(err)
		}
	}
	processed := make(chan MessageID, 3)
	p, err := NewBatchProcessor(s, func(ctx context.Context, messages []Message[string, string], results chan<- BatchProcessResult) error {
		for _, m := range messages {
			processed <- m.ID
			results <- BatchProcessResult{ID: m.ID}
		}
		return nil
	}, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{Concurrency: 2, PollInterval: time.Hour, ClaimMaxBytes: 1024}, ClaimBatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	defer stopProcessor(t, cancel, done)
	for range 3 {
		select {
		case <-processed:
		case <-time.After(5 * time.Second):
			t.Fatal("byte-limited batch waited for poll")
		}
	}
}

func TestClaimByteLimitRejectsNegativeConfiguration(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "negative-claim-bytes", Serde[string, string]{})
	defer cleanup()
	_, err := NewBatchProcessor(s, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, BatchProcessorOptions{ProcessorOptions: ProcessorOptions{ClaimMaxBytes: -1}})
	if err == nil || !strings.Contains(err.Error(), "ClaimMaxBytes") {
		t.Fatalf("err=%v", err)
	}
	_, err = NewProcessor(s, func(context.Context, Message[string, string]) error { return nil }, ProcessorOptions{ClaimMaxBytes: -1})
	if err == nil || !strings.Contains(err.Error(), "ClaimMaxBytes") {
		t.Fatalf("single processor err=%v", err)
	}
}

func TestReferencedQuarantineSurvivesReopenAndDetectsSourceRewrite(t *testing.T) {
	for _, rewrite := range []bool{false, true} {
		t.Run(map[bool]string{false: "reopen", true: "same-size-rewrite"}[rewrite], func(t *testing.T) {
			dir := t.TempDir()
			opts := badger.DefaultOptions(dir).WithLogger(nil).WithValueThreshold(1)
			db, err := badger.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			s, err := New[string, string](db, Serde[string, string]{}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("a", 1000)})
			if err != nil {
				t.Fatal(err)
			}
			value, _ := storedItemBytes(t, db, s.keys.messageKey(id))
			if _, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 1, 512, time.Minute, 3); err != nil {
				t.Fatal(err)
			}
			rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 1})
			if err != nil || len(rows) != 1 {
				t.Fatalf("rows=%v err=%v", rows, err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			db, err = badger.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			s, err = New[string, string](db, Serde[string, string]{}, Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if rewrite {
				if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.messageKey(id), value) }); err != nil {
					t.Fatal(err)
				}
			}
			before := recordTestNamespace(t, db, s.opts.Namespace)
			err = s.RequeueDeadLetterWithOptions(t.Context(), id, rows[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: 16 << 10})
			if rewrite {
				if !errors.Is(err, ErrInconsistentIndex) {
					t.Fatalf("rewrite replay error=%v", err)
				}
				if after := recordTestNamespace(t, db, s.opts.Namespace); !reflect.DeepEqual(before, after) {
					t.Fatal("invalid requeue changed reference")
				}
				if _, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 1}); !errors.Is(err, ErrInconsistentIndex) {
					t.Fatalf("rewrite metadata err=%v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				message, err := s.Get(t.Context(), id)
				if err != nil || message.Payload != strings.Repeat("a", 1000) {
					t.Fatalf("payload=%q err=%v", message.Payload, err)
				}
			}
		})
	}
}

func TestReferencedQuarantineExcludesOldestAuxiliaryIndex(t *testing.T) {
	r := newFakeRuntime(time.Unix(1700000000, 0))
	_, s, cleanup := openTestStoreWithOptions(t, "quarantine-age", Serde[string, string]{}, Options{Runtime: r})
	defer cleanup()
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", 4096)}); err != nil {
		t.Fatal(err)
	}
	r.SetNow(r.Now().Add(time.Hour))
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "healthy"}); err != nil {
		t.Fatal(err)
	}
	if work, _, err := s.claimReadyBatchWithLimits(t.Context(), r.Now(), 1, 1024, time.Minute, 3); err != nil || len(work) != 0 {
		t.Fatalf("work=%v err=%v", work, err)
	}
	r.SetNow(r.Now().Add(time.Second))
	q, err := s.QueueSnapshot(t.Context())
	if err != nil || q.ReadyDepth != 1 || q.DeadLetterDepth != 1 || q.OldestReadyAge != time.Second {
		t.Fatalf("snapshot=%+v err=%v", q, err)
	}
}

func TestClaimByteBudgetRepairsDuplicateIndexWithoutReadingPendingWrite(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "duplicate-claim-index", Serde[string, string]{})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "healthy"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.keys.readyKey(time.Now().Add(time.Millisecond), id), emptyValue)
	}); err != nil {
		t.Fatal(err)
	}
	work, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now().Add(time.Second), 10, 1024, time.Minute, 3)
	if err != nil || len(work) != 1 || work[0].Message.ID != id {
		t.Fatalf("work=%v err=%v", work, err)
	}
	if n := countKeysWithPrefix(t, db, s.keys.readyPrefix); n != 0 {
		t.Fatalf("duplicate index remains: %d", n)
	}
}

func TestSingleProcessorQuarantinesOversizedHeadAndReplaysWithLargerBudget(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "single-byte-control", Serde[string, string]{})
	defer cleanup()
	payload := strings.Repeat("large", 1000)
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: payload, Destination: "route"})
	if err != nil {
		t.Fatal(err)
	}
	original, _ := storedItemBytes(t, db, s.keys.messageKey(id))
	healthy, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "healthy", Destination: "route"})
	if err != nil {
		t.Fatal(err)
	}
	processed := make(chan Message[string, string], 2)
	callback := func(ctx context.Context, message Message[string, string]) error { processed <- message; return nil }
	processor, err := NewProcessor(s, callback, ProcessorOptions{Concurrency: 1, PollInterval: time.Hour, ClaimMaxBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	runUntilSettled := func(processor *Processor[string, string], wantID MessageID, wantPayload string, retained uint64) {
		t.Helper()
		cancel, done := runProcessor(processor)
		defer stopProcessor(t, cancel, done)
		select {
		case message := <-processed:
			if message.ID != wantID || message.Payload != wantPayload || message.Destination != "route" {
				t.Fatalf("unexpected callback=%+v", message)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("single processor did not deliver the expected message")
		}
		// Callback entry precedes durable acknowledgement. Wait for that commit
		// before canceling so this test does not race shutdown's retry semantics.
		waitFor(t, func() bool {
			usage, err := s.Usage(t.Context())
			return err == nil && usage.RetainedMessages == retained
		})
	}
	runUntilSettled(processor, healthy, "healthy", 1)
	if value, _ := storedItemBytes(t, db, s.keys.messageKey(id)); !bytes.Equal(value, original) {
		t.Fatal("single processor changed quarantined source")
	}
	rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 2, MaxBytes: 1024})
	if err != nil || len(rows) != 1 || rows[0].ID != id || rows[0].QuarantinedSource == nil {
		t.Fatalf("quarantine=%+v err=%v", rows, err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil || usage.RetainedMessages != 1 {
		t.Fatalf("usage=%+v err=%v", usage, err)
	}
	if err := s.RequeueDeadLetterWithOptions(t.Context(), id, rows[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: 32 << 10}); err != nil {
		t.Fatal(err)
	}
	processor, err = NewProcessor(s, callback, ProcessorOptions{Concurrency: 1, PollInterval: time.Hour, ClaimMaxBytes: 32 << 10})
	if err != nil {
		t.Fatal(err)
	}
	runUntilSettled(processor, id, payload, 0)
	assertUsage(t, s, 0, 0)
}
