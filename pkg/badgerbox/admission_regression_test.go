package badgerbox

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestClaimRejectsInvalidUTF8TokenAtomically(t *testing.T) {
	for _, invalid := range []string{"\xffnonce", "partial\xe2\x82"} {
		t.Run(invalid, func(t *testing.T) {
			runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
			calls := 0
			runtime.tokenFunc = func() (string, error) {
				calls++
				if calls == 2 {
					return invalid, nil
				}
				return "valid-first", nil
			}
			db, s, cleanup := openTestStoreWithOptions(t, "utf8-token", Serde[string, string]{}, Options{Runtime: runtime})
			defer cleanup()
			for range 2 {
				if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "test"}); err != nil {
					t.Fatal(err)
				}
			}
			before := recordTestNamespace(t, db, s.opts.Namespace)
			work, err := s.claimReadyBatch(t.Context(), runtime.Now(), 2, time.Second, 3)
			if err == nil || len(work) != 0 {
				t.Fatalf("invalid token: claimed=%d err=%v", len(work), err)
			}
			if after := recordTestNamespace(t, db, s.opts.Namespace); !reflect.DeepEqual(before, after) {
				t.Fatal("invalid second token committed part of the claimed batch")
			}
			// Valid UTF-8 that JSON escapes must still round-trip exactly. Recover
			// the first lease, then acknowledge the replacement attempt.
			for i := range 2 {
				token := strings.Repeat("é", 120) + "\x00\"\\" + string(rune('a'+i))
				runtime.tokenFunc = func() (string, error) { return token, nil }
				work, err = s.claimReadyBatch(t.Context(), runtime.Now(), 1, time.Second, 3)
				if err != nil || len(work) != 1 || work[0].LeaseToken != token {
					t.Fatalf("valid token: work=%v err=%v", work, err)
				}
				runtime.SetNow(runtime.Now().Add(2 * time.Second))
				if n, err := s.requeueExpired(t.Context(), runtime.Now(), 1); err != nil || n != 1 {
					t.Fatalf("recover valid token: n=%d err=%v", n, err)
				}
			}
			runtime.tokenFunc = func() (string, error) { return "replacement", nil }
			work, err = s.claimReadyBatch(t.Context(), runtime.Now(), 2, time.Second, 3)
			if err != nil || len(work) != 2 {
				t.Fatalf("claim after recovery: work=%v err=%v", work, err)
			}
			for _, record := range work {
				if err := s.acknowledge(t.Context(), record.Message.ID, record.LeaseToken); err != nil {
					t.Fatal(err)
				}
			}
			if q, err := s.QueueSnapshot(t.Context()); err != nil || q != (QueueSnapshot{}) {
				t.Fatalf("residual snapshot=%+v err=%v", q, err)
			}
		})
	}
}

func TestAcceptedBoundaryRecordCanBeRequeuedAfterMetadataChanges(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithValueLogFileSize(1 << 20))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
	s, err := New[string, string](db, Serde[string, string]{}, Options{Runtime: runtime})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 10 {
		if _, err := s.seq.Next(); err != nil {
			t.Fatal(err)
		}
	}
	// The original admission calculation accepted this exact 1 MiB boundary
	// record but rejected its requeue after MaxAttempts grew from 10 to 100.
	payload := strings.Repeat("x", 761692)
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: payload})
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{100, math.MaxInt} {
		work, err := s.claimReadyBatch(t.Context(), runtime.Now(), 1, time.Minute, limit)
		if err != nil || len(work) != 1 || work[0].Message.ID != id {
			t.Fatalf("claim limit=%d: work=%v err=%v", limit, work, err)
		}
		if _, err := s.failProcessing(t.Context(), id, work[0].LeaseToken, Permanent(errors.New("failed")), time.Second, time.Second); err != nil {
			t.Fatal(err)
		}
		if err := s.RequeueDeadLetter(t.Context(), id, runtime.Now(), time.Unix(0, math.MinInt64+1)); err != nil {
			t.Fatalf("requeue unchanged payload with limit=%d: %v", limit, err)
		}
		got, err := s.Get(t.Context(), id)
		if err != nil || got.Payload != payload || got.Attempt != 0 || got.State != MessageStateReady {
			t.Fatalf("requeue did not preserve payload and reset lifecycle: err=%v", err)
		}
	}
	work, err := s.claimReadyBatch(t.Context(), runtime.Now(), 1, time.Minute, 3)
	if err != nil || len(work) != 1 {
		t.Fatalf("final claim: work=%v err=%v", work, err)
	}
	if err := s.acknowledge(t.Context(), id, work[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
}
