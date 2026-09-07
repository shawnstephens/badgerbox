package badgerbox

import (
	"github.com/dgraph-io/badger/v4"
	"strings"
	"testing"
	"time"
)

func TestAdaptiveClaimsAndPagedRecovery(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithMemTableSize(1 << 20).WithValueThreshold(128 << 10))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New[string, string](db, Serde[string, string]{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 32 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", 8<<10), Destination: "test"}); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	claimed, limit, err := s.claimReadyBatchWithEffectiveLimit(t.Context(), now, 32, time.Second, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) == 0 || limit >= 32 || len(claimed) > limit {
		t.Fatalf("claimed=%d effective=%d", len(claimed), limit)
	}
	n, err := s.requeueExpired(t.Context(), now.Add(2*time.Second), 1)
	if err != nil || n != 1 {
		t.Fatalf("requeue=%d err=%v", n, err)
	}
	q, err := s.QueueSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if q.ProcessingDepth != int64(len(claimed)-1) {
		t.Fatalf("unbounded recovery: %+v", q)
	}
}
