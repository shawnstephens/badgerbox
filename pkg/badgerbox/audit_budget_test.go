package badgerbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"strings"
	"testing"
	"time"
)

func TestAuditDeadLetterOnlyBudgets(t *testing.T) {
	db, s, cleanup := openTestStore[[]byte, []byte](t, "audit-history", Serde[[]byte, []byte]{Message: rejectDecodeCodec{}, Destination: rejectDecodeCodec{}})
	defer cleanup()
	var rowBytes int64
	if err := db.Update(func(txn *badger.Txn) error {
		for i := 1; i <= 1000; i++ {
			id := MessageID(i)
			at := time.Unix(0, int64(i))
			record := storedRecord{ID: id, Status: MessageStateProcessing, Attempt: 1, MaxAttempts: 1, LeaseToken: "lease", LeaseUntilUnix: 1, PayloadBytes: []byte{255}, DestinationBytes: []byte{128}}
			value, err := json.Marshal(storedDeadLetter{Record: record, FailedAt: at.UnixNano()})
			if err != nil {
				return err
			}
			key := s.keys.deadLetterKey(at, id)
			rowBytes = int64(len(key) + len(value))
			if err := txn.Set(key, value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, opts := range []AuditOptions{{SampleLimit: 1, MaxScannedKeys: 7}, {SampleLimit: 1, MaxScannedBytes: rowBytes * 3}} {
		report, err := s.Audit(t.Context(), opts)
		if !errors.Is(err, ErrAuditLimitExceeded) || report.Complete || report.LiveRows != 0 {
			t.Fatalf("report=%+v err=%v", report, err)
		}
		if opts.MaxScannedKeys > 0 && report.ScannedKeys != 7 {
			t.Fatalf("keys=%d", report.ScannedKeys)
		}
		if opts.MaxScannedBytes > 0 && report.ScannedBytes > opts.MaxScannedBytes {
			t.Fatal("byte budget exceeded")
		}
	}
	report, err := s.Audit(t.Context(), AuditOptions{})
	if err != nil || !report.Complete || report.DeadLetters.Rows != 1000 || report.ScannedKeys != 1001 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestAuditRejectsHugeValueBeforeDecode(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "audit-huge", Serde[string, string]{})
	defer cleanup()
	if err := db.Update(func(txn *badger.Txn) error {
		return txn.Set(s.keys.deadLetterKey(time.Now(), 1), bytes.Repeat([]byte("invalid JSON"), 10000))
	}); err != nil {
		t.Fatal(err)
	}
	report, err := s.Audit(t.Context(), AuditOptions{MaxScannedBytes: 1024})
	if !errors.Is(err, ErrAuditLimitExceeded) || report.ScannedKeys != 0 || report.Complete {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestAuditIncompleteIndexDoesNotInventMissingRows(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "audit-partial-index", Serde[string, string]{})
	defer cleanup()
	for range 2 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{}); err != nil {
			t.Fatal(err)
		}
	}
	report, err := s.Audit(t.Context(), AuditOptions{MaxScannedKeys: 3})
	if !errors.Is(err, ErrAuditLimitExceeded) || report.Complete || report.States.Ready.Lifecycle.Missing != 0 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	for _, opts := range []AuditOptions{{MaxScannedKeys: -1}, {MaxScannedBytes: -1}} {
		if _, err := s.Audit(t.Context(), opts); err == nil {
			t.Fatal("negative budget accepted")
		}
	}
}

func TestAuditAndLiveReadsRejectSameLifecycleCorruption(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "audit-corruption", Serde[string, string]{})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(s.keys.messageKey(id))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(value, &fields); err != nil {
			return err
		}
		delete(fields, "created_at_unix_nano")
		value, err = json.Marshal(fields)
		if err != nil {
			return err
		}
		return txn.Set(s.keys.messageKey(id), value)
	}); err != nil {
		t.Fatal(err)
	}
	_, liveErr := s.Get(t.Context(), id)
	report, auditErr := s.Audit(t.Context(), AuditOptions{})
	if liveErr == nil || auditErr == nil || report.Complete || !strings.Contains(liveErr.Error(), auditErr.Error()) {
		t.Fatalf("live=%v audit=%v complete=%v", liveErr, auditErr, report.Complete)
	}
}
