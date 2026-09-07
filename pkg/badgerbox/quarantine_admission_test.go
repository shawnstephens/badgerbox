package badgerbox

import (
	"context"
	"errors"
	"reflect"

	"github.com/dgraph-io/badger/v4"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestQuarantinePreservesAdmissionUntilSuccessfulAcknowledgement(t *testing.T) {
	for _, mode := range []string{"codec-error", "codec-panic", "claim-bytes"} {
		t.Run(mode, func(t *testing.T) {
			codec := &quarantineCodec{panicDecode: mode == "codec-panic"}
			codec.broken.Store(mode != "claim-bytes")
			pressure := errors.New("disk pressure")
			var reject atomic.Bool
			var probes atomic.Int64
			opts := Options{AdmissionLimits: AdmissionLimits{MaxRetainedMessages: 1}, EnqueueGuard: func(ctx context.Context) error {
				probes.Add(1)
				if reject.Load() {
					return pressure
				}
				return ctx.Err()
			}}
			_, s, cleanup := openTestStoreWithOptions(t, "retained-quarantine", Serde[string, string]{Message: codec}, opts)
			defer cleanup()
			payload := "poison"
			claimBytes := int64(4096)
			if mode == "claim-bytes" {
				payload = strings.Repeat("x", 4096)
				claimBytes = 512
			}
			id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: payload, Destination: "route"})
			if err != nil {
				t.Fatal(err)
			}
			initial, err := s.Usage(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			assertUsage(t, s, 1, initial.RetainedBytes)
			work, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 10, claimBytes, time.Minute, 3)
			if err != nil || len(work) != 0 {
				t.Fatalf("work=%v err=%v", work, err)
			}
			assertUsage(t, s, 1, initial.RetainedBytes)
			if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "another"}); !errors.Is(err, ErrAdmissionLimit) {
				t.Fatalf("quarantine released admission: %v", err)
			}
			letters, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 2, MaxBytes: 16 << 10})
			if err != nil || len(letters) != 1 || letters[0].ID != id {
				t.Fatalf("letters=%v err=%v", letters, err)
			}
			if mode == "claim-bytes" && letters[0].QuarantinedSource == nil {
				t.Fatal("missing referenced quarantine")
			}
			ack, err := s.acknowledgeOwned(t.Context(), id, "never-owned")
			if ack || (err != nil && !errors.Is(err, ErrMessageQuarantined)) {
				t.Fatalf("stale ack=%v err=%v", ack, err)
			}
			assertUsage(t, s, 1, initial.RetainedBytes)
			// Replay and settlement must work even while both intake guards reject
			// new work. Moving an existing charge must never require readmission.
			if err := s.CompareAndSwapAdmissionLimits(t.Context(), initial.Limits, AdmissionLimits{MaxRetainedMessages: 1, MaxRetainedBytes: 1}); err != nil {
				t.Fatal(err)
			}
			reject.Store(true)
			codec.broken.Store(false)
			beforeProbes := probes.Load()
			if err := s.RequeueDeadLetterWithOptions(t.Context(), id, letters[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: 32 << 10}); err != nil {
				t.Fatal(err)
			}
			assertUsage(t, s, 1, initial.RetainedBytes)
			work, _, err = s.claimReadyBatchWithLimits(t.Context(), time.Now(), 1, 32<<10, time.Minute, 3)
			if err != nil || len(work) != 1 || work[0].Message.ID != id || work[0].Message.Payload != payload {
				t.Fatalf("replayed=%v err=%v", work, err)
			}
			assertUsage(t, s, 1, initial.RetainedBytes)
			ack, err = s.acknowledgeOwned(t.Context(), id, work[0].LeaseToken)
			if err != nil || !ack {
				t.Fatalf("ack=%v err=%v", ack, err)
			}
			assertUsage(t, s, 0, 0)
			if err := s.acknowledge(t.Context(), id, work[0].LeaseToken); err != nil {
				t.Fatal(err)
			}
			assertUsage(t, s, 0, 0)
			if probes.Load() != beforeProbes {
				t.Fatal("replay or settlement invoked intake guard")
			}
		})
	}
}

func TestMixedQuarantineAuditCountsEveryRetainedSourceOnce(t *testing.T) {
	codec := &quarantineCodec{}
	codec.broken.Store(true)
	_, s, cleanup := openTestStoreWithOptions(t, "mixed-quarantine-usage", Serde[string, string]{Message: codec}, Options{AdmissionLimits: AdmissionLimits{MaxRetainedMessages: 3}})
	defer cleanup()
	var charges []uint64
	var total uint64
	for _, payload := range []string{"poison", strings.Repeat("x", 4096), "healthy"} {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: payload}); err != nil {
			t.Fatal(err)
		}
		usage, err := s.Usage(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		charges = append(charges, usage.RetainedBytes-total)
		total = usage.RetainedBytes
	}
	work, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 10, 1024, time.Minute, 3)
	if err != nil || len(work) != 1 || work[0].Message.Payload != "healthy" {
		t.Fatalf("work=%v err=%v", work, err)
	}
	assertUsage(t, s, 3, total)
	if err := s.acknowledge(t.Context(), work[0].Message.ID, work[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	assertUsage(t, s, 2, total-charges[2])
	audit, err := s.Audit(t.Context(), AuditOptions{})
	if err != nil || !audit.Complete || !audit.Usage.Matches || audit.LiveRows != 0 || audit.DeadLetters.Rows != 2 || audit.DeadLetters.ReferencedRows != 1 || len(audit.Samples.Anomalies) != 0 {
		t.Fatalf("audit=%+v err=%v", audit, err)
	}
	if q, err := s.QueueSnapshot(t.Context()); err != nil || q.ReadyDepth != 0 || q.ProcessingDepth != 0 || q.DeadLetterDepth != 2 {
		t.Fatalf("snapshot=%+v err=%v", q, err)
	}
}

func TestRejectedReferencedReplayPreservesQuotaAndSource(t *testing.T) {
	db, s, cleanup := openTestStoreWithOptions(t, "quarantine-replay-quota", Serde[string, string]{}, Options{AdmissionLimits: AdmissionLimits{MaxRetainedMessages: 1}})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", 4096)})
	if err != nil {
		t.Fatal(err)
	}
	usage, err := s.Usage(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.claimReadyBatchWithLimits(t.Context(), time.Now(), 1, 512, time.Minute, 3); err != nil {
		t.Fatal(err)
	}
	rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	if err := s.RequeueDeadLetterWithOptions(t.Context(), id, rows[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: 512}); !errors.Is(err, ErrDeadLetterTooLarge) {
		t.Fatalf("replay budget error=%v", err)
	}
	assertUsage(t, s, 1, usage.RetainedBytes)
	// Rewriting even identical source bytes gives a different committed version;
	// an existing reference must not silently attach to that replacement.
	value, _ := storedItemBytes(t, db, s.keys.messageKey(id))
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.messageKey(id), value) }); err != nil {
		t.Fatal(err)
	}
	before := recordTestNamespace(t, db, s.opts.Namespace)
	if err := s.RequeueDeadLetterWithOptions(t.Context(), id, rows[0].FailedAt, DeadLetterRequeueOptions{MaxBytes: 32 << 10}); !errors.Is(err, ErrInconsistentIndex) {
		t.Fatalf("wrong source error=%v", err)
	}
	if after := recordTestNamespace(t, db, s.opts.Namespace); !reflect.DeepEqual(before, after) {
		t.Fatal("rejected replay changed records or quota metadata")
	}
	if after, err := s.Usage(t.Context()); err != nil || after != usage {
		t.Fatalf("quota after rejected replay=%+v err=%v", after, err)
	}
	if report, err := s.Audit(t.Context(), AuditOptions{}); !errors.Is(err, ErrInconsistentIndex) || report.Complete {
		t.Fatalf("invalid source audit=%+v err=%v", report, err)
	}
}
