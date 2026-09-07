package badgerbox

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestExpiredOwnerCannotSettleReferencedQuarantine(t *testing.T) {
	for _, settlement := range []string{"acknowledge", "acknowledge-owned", "retry", "dead-letter", "release"} {
		t.Run(settlement, func(t *testing.T) {
			r := newFakeRuntime(time.Unix(1_700_000_000, 0))
			db, s, cleanup := openTestStoreWithOptions(t, "stale-quarantine-owner", Serde[string, string]{}, Options{
				Runtime: r, AdmissionLimits: AdmissionLimits{MaxRetainedMessages: 1},
			})
			defer cleanup()
			id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", 4096)})
			if err != nil {
				t.Fatal(err)
			}
			work, err := s.claimReadyBatch(t.Context(), r.Now(), 1, time.Second, 3)
			if err != nil || len(work) != 1 {
				t.Fatalf("claim=%v err=%v", work, err)
			}
			r.SetNow(r.Now().Add(2 * time.Second))
			if recovered, err := s.requeueExpired(t.Context(), r.Now(), 1); err != nil || recovered != 1 {
				t.Fatalf("recovered=%d err=%v", recovered, err)
			}
			// Another processor with a smaller budget quarantines the recovered
			// row before the original callback completes.
			if claimed, _, err := s.claimReadyBatchWithLimits(t.Context(), r.Now(), 1, 512, time.Second, 3); err != nil || len(claimed) != 0 {
				t.Fatalf("quarantine claim=%v err=%v", claimed, err)
			}
			before := recordTestNamespace(t, db, s.opts.Namespace)
			switch settlement {
			case "acknowledge":
				err = s.acknowledge(t.Context(), id, work[0].LeaseToken)
			case "acknowledge-owned":
				var acknowledged bool
				acknowledged, err = s.acknowledgeOwned(t.Context(), id, work[0].LeaseToken)
				if acknowledged {
					t.Fatal("stale owner acknowledged quarantine")
				}
			case "retry", "dead-letter":
				cause := errors.New("late callback failure")
				if settlement == "dead-letter" {
					cause = Permanent(cause)
				}
				var result failProcessingResult
				result, err = s.failProcessing(t.Context(), id, work[0].LeaseToken, cause, time.Second, time.Second)
				if result != (failProcessingResult{}) {
					t.Fatalf("stale owner changed lifecycle: %+v", result)
				}
			case "release":
				var released int
				released, err = s.releaseClaimed(t.Context(), work)
				if released != 0 {
					t.Fatalf("stale owner released %d messages", released)
				}
			}
			if err != nil {
				t.Fatalf("lost ownership caused settlement failure: %v", err)
			}
			if after := recordTestNamespace(t, db, s.opts.Namespace); !reflect.DeepEqual(before, after) {
				t.Fatal("stale settlement changed quarantine or admission usage")
			}
			if _, err := s.Get(t.Context(), id); !errors.Is(err, ErrMessageQuarantined) {
				t.Fatalf("Get no longer reports quarantine: %v", err)
			}
			report, err := s.Audit(t.Context(), AuditOptions{})
			if err != nil || !report.Complete || !report.Usage.Matches || report.DeadLetters.ReferencedRows != 1 {
				t.Fatalf("audit=%+v err=%v", report, err)
			}
		})
	}
}
