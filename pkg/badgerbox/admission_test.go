package badgerbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func assertUsage[M, D any](t *testing.T, s *Store[M, D], messages, bytes uint64) UsageSnapshot {
	t.Helper()
	usage, err := s.Usage(t.Context())
	if err != nil || usage.RetainedMessages != messages || usage.RetainedBytes != bytes {
		t.Fatalf("usage=%+v error=%v; want %d messages, %d bytes", usage, err, messages, bytes)
	}
	audit, err := s.Audit(t.Context(), AuditOptions{})
	if err != nil || !audit.Complete || !audit.Usage.Matches || audit.Usage.Persisted != usage {
		t.Fatalf("audit usage=%+v complete=%v error=%v", audit.Usage, audit.Complete, err)
	}
	return usage
}

func TestAdmissionCapacityTracksEntireLifecycle(t *testing.T) {
	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	_, s, cleanup := openTestStoreWithOptions(t, "admission-lifecycle", Serde[string, string]{}, Options{
		Runtime: runtime, AdmissionLimits: AdmissionLimits{MaxRetainedMessages: 1},
	})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "retained", Destination: "destination", AvailableAt: runtime.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil || usage.RetainedBytes == 0 {
		t.Fatalf("usage=%+v err=%v", usage, err)
	}
	assertFull := func() {
		t.Helper()
		assertUsage(t, s, 1, usage.RetainedBytes)
		_, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "blocked"})
		if !errors.Is(err, ErrAdmissionLimit) {
			t.Fatalf("admission error=%v", err)
		}
	}
	assertFull()
	runtime.SetNow(runtime.Now().Add(time.Minute))
	claim := func() claimedRecord[string, string] {
		t.Helper()
		work, err := s.claimReadyBatch(t.Context(), runtime.Now(), 1, time.Second, 10)
		if err != nil || len(work) != 1 || work[0].Message.ID != id {
			t.Fatalf("work=%+v err=%v", work, err)
		}
		assertFull()
		return work[0]
	}
	work := claim()
	if acknowledged, err := s.acknowledgeOwned(t.Context(), id, "stale-token"); err != nil || acknowledged {
		t.Fatalf("stale acknowledgement=%v err=%v", acknowledged, err)
	}
	assertFull()
	if _, err := s.failProcessing(t.Context(), id, work.LeaseToken, errors.New("retry"), time.Second, time.Second); err != nil {
		t.Fatal(err)
	}
	assertFull()
	runtime.SetNow(runtime.Now().Add(time.Second))
	work = claim()
	if released, err := s.releaseClaimed(t.Context(), []claimedRecord[string, string]{work}); err != nil || released != 1 {
		t.Fatalf("released=%d err=%v", released, err)
	}
	assertFull()
	claim()
	runtime.SetNow(runtime.Now().Add(2 * time.Second))
	if recovered, err := s.requeueExpired(t.Context(), runtime.Now(), 1); err != nil || recovered != 1 {
		t.Fatalf("recovered=%d err=%v", recovered, err)
	}
	assertFull()
	work = claim()
	if _, err := s.failProcessing(t.Context(), id, work.LeaseToken, Permanent(errors.New("dead letter")), time.Second, time.Second); err != nil {
		t.Fatal(err)
	}
	assertFull()
	// Tightening below current usage must preserve the existing record and permit
	// exact requeue: retained work does not require a second admission.
	if err := s.CompareAndSwapAdmissionLimits(t.Context(), usage.Limits, AdmissionLimits{MaxRetainedMessages: 1, MaxRetainedBytes: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.RequeueDeadLetter(t.Context(), id, runtime.Now(), runtime.Now()); err != nil {
		t.Fatal(err)
	}
	assertFull()
	work = claim()
	if acknowledged, err := s.acknowledgeOwned(t.Context(), id, work.LeaseToken); err != nil || !acknowledged {
		t.Fatalf("acknowledged=%v err=%v", acknowledged, err)
	}
	assertUsage(t, s, 0, 0)
	if err := s.acknowledge(t.Context(), id, work.LeaseToken); err != nil {
		t.Fatal(err)
	}
	assertUsage(t, s, 0, 0)
}

func TestAdmissionBytesBoundaryAndPrewriteRejection(t *testing.T) {
	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	db, s, cleanup := openTestStoreWithOptions(t, "admission-bytes", Serde[[]byte, []byte]{Message: binaryCodec{}, Destination: binaryCodec{}}, Options{Runtime: runtime})
	defer cleanup()
	req := EnqueueRequest[[]byte, []byte]{Payload: bytes.Repeat([]byte{0, 255, '"'}, 101), Destination: []byte("route"), AvailableAt: runtime.Now().Add(24 * time.Hour)}
	if _, err := s.Enqueue(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil || usage.RetainedBytes <= uint64(len(req.Payload)+len(req.Destination)) {
		t.Fatalf("logical bytes omit record/base64 overhead: usage=%+v err=%v", usage, err)
	}
	limit := AdmissionLimits{MaxRetainedBytes: usage.RetainedBytes * 2}
	if err := s.CompareAndSwapAdmissionLimits(t.Context(), AdmissionLimits{}, limit); err != nil {
		t.Fatal(err)
	}
	// IDs 0 and 1 have the same encoded width. Scheduling cannot change the
	// immutable accounting charge.
	req.AvailableAt = time.Unix(0, math.MinInt64)
	if _, err := s.Enqueue(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	assertUsage(t, s, 2, limit.MaxRetainedBytes)
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("business"), []byte("safe")); err != nil {
			return err
		}
		_, err := s.EnqueueTx(t.Context(), txn, req)
		var rejection *AdmissionLimitError
		if !errors.As(err, &rejection) || rejection.Resource != "bytes" || rejection.Limit != limit.MaxRetainedBytes || rejection.Used != limit.MaxRetainedBytes || rejection.Requested == 0 {
			t.Fatalf("rejection=%+v err=%v", rejection, err)
		}
		// This known pre-write failure must not corrupt a transaction even if a
		// caller ignores the general recommendation to discard on any error.
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	assertUsage(t, s, 2, limit.MaxRetainedBytes)
}

func TestAdmissionTransactionRollbackAndConflicts(t *testing.T) {
	db, s, cleanup := openTestStoreWithOptions(t, "admission-transactions", Serde[string, string]{}, Options{AdmissionLimits: AdmissionLimits{MaxRetainedMessages: 1}})
	defer cleanup()
	req := EnqueueRequest[string, string]{Payload: "one"}
	first, second := db.NewTransaction(true), db.NewTransaction(true)
	defer first.Discard()
	defer second.Discard()
	for _, txn := range []*badger.Txn{first, second} {
		if _, err := s.EnqueueTx(t.Context(), txn, req); err != nil {
			t.Fatal(err)
		}
	}
	assertUsage(t, s, 0, 0)
	if err := first.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := second.Commit(); !errors.Is(err, badger.ErrConflict) {
		t.Fatalf("concurrent overcommit err=%v", err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertUsage(t, s, 1, usage.RetainedBytes)
	if err := s.CompareAndSwapAdmissionLimits(t.Context(), usage.Limits, AdmissionLimits{}); err != nil {
		t.Fatal(err)
	}
	rollback := db.NewTransaction(true)
	if _, err := s.EnqueueTx(t.Context(), rollback, req); err != nil {
		t.Fatal(err)
	}
	rollback.Discard()
	assertUsage(t, s, 1, usage.RetainedBytes)
	if err := db.Update(func(txn *badger.Txn) error {
		for range 4 {
			if _, err := s.EnqueueTx(t.Context(), txn, req); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	audit, err := s.Audit(t.Context(), AuditOptions{})
	if err != nil || !audit.Usage.Matches || audit.Usage.RetainedMessages != 5 {
		t.Fatalf("audit=%+v err=%v", audit.Usage, err)
	}
}

func TestAdmissionLimitsPersistAcrossRestart(t *testing.T) {
	options := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	limits := AdmissionLimits{MaxRetainedMessages: 1, MaxRetainedBytes: 10_000}
	s, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: "restart", AdmissionLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "persistent"}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = badger.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	keysBefore := recordTestNamespace(t, db, "restart")
	for _, mismatched := range []AdmissionLimits{{}, {MaxRetainedMessages: 1}, {MaxRetainedMessages: 2, MaxRetainedBytes: 10_000}} {
		_, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: "restart", AdmissionLimits: mismatched})
		var mismatch *AdmissionLimitsMismatchError
		if !errors.As(err, &mismatch) || mismatch.Actual != limits || mismatch.Expected != mismatched {
			t.Fatalf("mismatch=%+v err=%v", mismatch, err)
		}
		if !reflect.DeepEqual(keysBefore, recordTestNamespace(t, db, "restart")) {
			t.Fatal("mismatched reopen changed storage")
		}
	}
	s, err = New[string, string](db, Serde[string, string]{}, Options{Namespace: "restart", AdmissionLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if got := assertUsage(t, s, before.RetainedMessages, before.RetainedBytes); got != before {
		t.Fatalf("reopened usage=%+v want=%+v", got, before)
	}
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); !errors.Is(err, ErrAdmissionLimit) {
		t.Fatalf("restart overcommit err=%v", err)
	}
}

func TestAdmissionSharedStoresCannotOvercommit(t *testing.T) {
	limits := AdmissionLimits{MaxRetainedMessages: 50}
	db, first, cleanup := openTestStoreWithOptions(t, "admission-concurrency", Serde[string, string]{}, Options{AdmissionLimits: limits})
	defer cleanup()
	second, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: first.opts.Namespace, AdmissionLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	var admitted atomic.Int64
	var failures atomic.Int64
	var wg sync.WaitGroup
	for worker := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := []*Store[string, string]{first, second}[worker%2]
			for i := range 30 {
				_, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: fmt.Sprintf("%d/%d", worker, i)})
				if err == nil {
					admitted.Add(1)
				} else if !errors.Is(err, ErrAdmissionLimit) {
					failures.Add(1)
					t.Errorf("enqueue: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	if failures.Load() != 0 || admitted.Load() != 50 {
		t.Fatalf("admitted=%d unexpected failures=%d", admitted.Load(), failures.Load())
	}
	usage, err := first.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	assertUsage(t, first, 50, usage.RetainedBytes)
	assertUsage(t, second, 50, usage.RetainedBytes)
}

func TestAdmissionLiveLimitChangeIsAuthoritative(t *testing.T) {
	db, first, cleanup := openTestStore[string, string](t, "admission-cas", Serde[string, string]{})
	defer cleanup()
	second, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: first.opts.Namespace})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	txn := db.NewTransaction(true)
	defer txn.Discard()
	if _, err := second.EnqueueTx(t.Context(), txn, EnqueueRequest[string, string]{}); err != nil {
		t.Fatal(err)
	}
	limit := AdmissionLimits{MaxRetainedMessages: 1}
	if err := first.CompareAndSwapAdmissionLimits(t.Context(), AdmissionLimits{}, limit); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); !errors.Is(err, badger.ErrConflict) {
		t.Fatalf("prepared enqueue ignored changed limits: %v", err)
	}
	if err := second.CompareAndSwapAdmissionLimits(t.Context(), AdmissionLimits{}, AdmissionLimits{MaxRetainedMessages: 2}); !errors.Is(err, ErrAdmissionLimitsMismatch) {
		t.Fatalf("stale CAS=%v", err)
	}
	for _, s := range []*Store[string, string]{first, second} {
		usage, err := s.Usage(t.Context())
		if err != nil || usage.Limits != limit {
			t.Fatalf("usage=%+v err=%v", usage, err)
		}
	}
	if _, err := second.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Enqueue(t.Context(), EnqueueRequest[string, string]{}); !errors.Is(err, ErrAdmissionLimit) {
		t.Fatalf("old Store bypassed changed limit: %v", err)
	}
	if err := first.CompareAndSwapAdmissionLimits(t.Context(), limit, AdmissionLimits{}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
		t.Fatalf("explicitly disabling limit: %v", err)
	}
}

func TestAdmissionRejectsForeignTransactionsAndUnsafeBadger(t *testing.T) {
	_, first, cleanup := openTestStore[string, string](t, "foreign", Serde[string, string]{})
	defer cleanup()
	otherDB, second, cleanupOther := openTestStore[string, string](t, "foreign", Serde[string, string]{})
	defer cleanupOther()
	txn := otherDB.NewTransaction(true)
	defer txn.Discard()
	if _, err := first.EnqueueTx(t.Context(), txn, EnqueueRequest[string, string]{}); !errors.Is(err, ErrForeignTransaction) {
		t.Fatalf("foreign enqueue err=%v", err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	assertUsage(t, first, 0, 0)
	assertUsage(t, second, 0, 0)
	unsafeDB, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil).WithDetectConflicts(false))
	if err != nil {
		t.Fatal(err)
	}
	defer unsafeDB.Close()
	if _, err := New[string, string](unsafeDB, Serde[string, string]{}, Options{}); !errors.Is(err, ErrConflictDetectionRequired) {
		t.Fatalf("unsafe Badger err=%v", err)
	}
	if got := countKeysWithPrefix(t, unsafeDB, []byte("ob/")); got != 0 {
		t.Fatalf("unsafe DB initialization wrote %d keys", got)
	}
}

func TestAdmissionCounterCorruptionFailsClosedAndAuditsMismatch(t *testing.T) {
	for _, corruption := range []string{"count-mismatch", "byte-mismatch", "count-overflow", "byte-overflow", "underflow", "missing", "huge"} {
		t.Run(corruption, func(t *testing.T) {
			db, s, cleanup := openTestStore[string, string](t, "corrupt-accounting", Serde[string, string]{})
			defer cleanup()
			id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "record"})
			if err != nil {
				t.Fatal(err)
			}
			work, err := s.claimReadyBatch(t.Context(), time.Now().Add(time.Second), 1, time.Minute, 3)
			if err != nil || len(work) != 1 {
				t.Fatalf("claim=%v err=%v", work, err)
			}
			if err := db.Update(func(txn *badger.Txn) error {
				state, err := s.loadAdmissionState(txn)
				if err != nil {
					return err
				}
				switch corruption {
				case "count-mismatch":
					state.RetainedMessages++
				case "byte-mismatch":
					state.RetainedBytes++
				case "count-overflow":
					state.RetainedMessages = math.MaxUint64
				case "byte-overflow":
					state.RetainedBytes = math.MaxUint64
				case "underflow":
					state.RetainedBytes = 1
				case "missing":
					return txn.Delete(s.keys.admissionKey)
				case "huge":
					return txn.Set(s.keys.admissionKey, bytes.Repeat([]byte{1}, 1<<20))
				}
				return s.storeAdmissionState(txn, state)
			}); err != nil {
				t.Fatal(err)
			}
			audit, auditErr := s.Audit(t.Context(), AuditOptions{})
			if audit.Usage.Matches || (auditErr == nil && !audit.Complete) {
				t.Fatalf("corruption reported healthy: %+v err=%v", audit.Usage, auditErr)
			}
			if corruption == "count-overflow" || corruption == "byte-overflow" {
				if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); !errors.Is(err, ErrAdmissionOverflow) {
					t.Fatalf("overflow admission err=%v", err)
				}
			}
			if corruption == "count-mismatch" || corruption == "byte-mismatch" || corruption == "underflow" || corruption == "missing" || corruption == "huge" {
				if _, err := s.acknowledgeOwned(t.Context(), id, work[0].LeaseToken); !errors.Is(err, ErrAdmissionState) {
					t.Fatalf("unsafe acknowledgement err=%v", err)
				}
				if _, err := s.Get(t.Context(), id); err != nil {
					t.Fatalf("failed settlement lost message: %v", err)
				}
			}
			if corruption == "missing" || corruption == "huge" {
				if _, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: s.opts.Namespace}); !errors.Is(err, ErrAdmissionState) {
					t.Fatalf("corrupt accounting reinitialized: %v", err)
				}
			}
		})
	}
}

func TestAdmissionPublicContextAndCloseChecks(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "admission-context", Serde[string, string]{})
	defer cleanup()
	if _, err := s.Usage(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil usage=%v", err)
	}
	if err := s.CompareAndSwapAdmissionLimits(nil, AdmissionLimits{}, AdmissionLimits{}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil CAS=%v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Usage(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled usage=%v", err)
	}
	if err := s.CompareAndSwapAdmissionLimits(ctx, AdmissionLimits{}, AdmissionLimits{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled CAS=%v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Usage(t.Context()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed usage=%v", err)
	}
	if err := s.CompareAndSwapAdmissionLimits(t.Context(), AdmissionLimits{}, AdmissionLimits{}); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed CAS=%v", err)
	}
}

func TestAdmissionConcurrentConstructorsCannotOverwriteConfiguration(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	start := make(chan struct{})
	type result struct {
		store *Store[string, string]
		limit AdmissionLimits
		err   error
	}
	results := make(chan result, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			limit := AdmissionLimits{MaxRetainedMessages: uint64(i%2 + 1)}
			s, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: "initialize", AdmissionLimits: limit})
			results <- result{store: s, limit: limit, err: err}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var actual AdmissionLimits
	accepted := 0
	for result := range results {
		if result.err != nil {
			var mismatch *AdmissionLimitsMismatchError
			if !errors.As(result.err, &mismatch) {
				t.Fatalf("constructor error=%v", result.err)
			}
			continue
		}
		t.Cleanup(func() { _ = result.store.Close() })
		accepted++
		usage := assertUsage(t, result.store, 0, 0)
		if actual == (AdmissionLimits{}) {
			actual = usage.Limits
		}
		if usage.Limits != actual || result.limit != actual {
			t.Fatalf("constructor overwrote limits: usage=%+v expected=%+v", usage, actual)
		}
	}
	if accepted != 8 {
		t.Fatalf("accepted=%d constructors, want exactly one configuration (8)", accepted)
	}
}

func TestAdmissionSmallValueThresholdAndMultipleTransactionalWrites(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueThreshold(1))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	limits := AdmissionLimits{MaxRetainedMessages: 5}
	s, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: "metadata-in-value-log", AdmissionLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		for range 5 {
			if _, err := s.EnqueueTx(t.Context(), txn, EnqueueRequest[string, string]{}); err != nil {
				return err
			}
		}
		if _, err := s.EnqueueTx(t.Context(), txn, EnqueueRequest[string, string]{}); !errors.Is(err, ErrAdmissionLimit) {
			t.Fatalf("pending accounting did not enforce limit: %v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := s.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New[string, string](db, Serde[string, string]{}, Options{Namespace: "metadata-in-value-log", AdmissionLimits: limits})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertUsage(t, s, 5, before.RetainedBytes)
	work, err := s.claimReadyBatch(t.Context(), time.Now().Add(time.Second), 5, time.Minute, 3)
	if err != nil || len(work) != 5 {
		t.Fatalf("claim=%v err=%v", work, err)
	}
	for _, record := range work {
		if err := s.acknowledge(t.Context(), record.Message.ID, record.LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
	assertUsage(t, s, 0, 0)
}

func TestAdmissionEmptyMessagesConsumeBytes(t *testing.T) {
	_, s, cleanup := openTestStoreWithOptions(t, "empty-byte-limit", Serde[[]byte, []byte]{Message: binaryCodec{}, Destination: binaryCodec{}}, Options{AdmissionLimits: AdmissionLimits{MaxRetainedBytes: 1}})
	defer cleanup()
	for _, value := range [][]byte{nil, {}} {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[[]byte, []byte]{Payload: value, Destination: value}); !errors.Is(err, ErrAdmissionLimit) {
			t.Fatalf("empty record bypassed byte budget: %v", err)
		}
	}
	assertUsage(t, s, 0, 0)
}

func TestAdmissionFailedSettlementRollbackKeepsCapacityReserved(t *testing.T) {
	_, s, cleanup := openTestStoreWithOptions(t, "ack-rollback", Serde[string, string]{}, Options{AdmissionLimits: AdmissionLimits{MaxRetainedMessages: 1}})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "settlement must commit"})
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now().Add(time.Second), 1, time.Minute, 3)
	if err != nil || len(work) != 1 {
		t.Fatalf("claim=%v err=%v", work, err)
	}
	injected := errors.New("injected failure after callback")
	acknowledged, err := s.acknowledgeUsingUpdate(t.Context(), id, work[0].LeaseToken, func(fn func(*badger.Txn) error) error {
		txn := s.db.NewTransaction(true)
		defer txn.Discard()
		if err := fn(txn); err != nil {
			return err
		}
		return injected
	})
	if acknowledged || !errors.Is(err, injected) {
		t.Fatalf("acknowledged=%v error=%v", acknowledged, err)
	}
	assertUsage(t, s, 1, usage.RetainedBytes)
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); !errors.Is(err, ErrAdmissionLimit) {
		t.Fatalf("rolled back settlement released capacity: %v", err)
	}
}

func TestAdmissionTraceContextHasStableCanonicalCharge(t *testing.T) {
	for name, carrier := range map[string]map[string]string{
		"unicode":       {"héader": "<>&\u2028😀", "trace": "\\\"\n"},
		"invalid-key":   {"\xff": "one", "\xfe": "two"},
		"invalid-value": {"trace": "\xff"},
	} {
		t.Run(name, func(t *testing.T) {
			db, s, cleanup := openTestStore[string, string](t, "trace-charge", Serde[string, string]{})
			defer cleanup()
			var id MessageID
			err := db.Update(func(txn *badger.Txn) error {
				result, err := s.enqueueTx(t.Context(), txn, EnqueueRequest[string, string]{}, 3, carrier)
				id = result.id
				return err
			})
			if name != "unicode" {
				if err == nil {
					t.Fatal("non-roundtrippable trace carrier accepted")
				}
				assertUsage(t, s, 0, 0)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			usage, err := s.Usage(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			assertUsage(t, s, 1, usage.RetainedBytes)
			work, err := s.claimReadyBatch(t.Context(), time.Now().Add(time.Second), 1, time.Minute, 3)
			if err != nil || len(work) != 1 {
				t.Fatalf("claim=%v err=%v", work, err)
			}
			if err := s.acknowledge(t.Context(), id, work[0].LeaseToken); err != nil {
				t.Fatal(err)
			}
			assertUsage(t, s, 0, 0)
		})
	}
}
