package badgerbox

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestAuditReportsIndexAnomaliesWithoutDecodingOrMutation(t *testing.T) {
	db, s, close := openTestStore[[]byte, []byte](t, "audit", Serde[[]byte, []byte]{Message: rejectDecodeCodec{}, Destination: rejectDecodeCodec{}})
	defer close()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[[]byte, []byte]{Payload: []byte{255}, Destination: []byte{128}})
	if err != nil {
		t.Fatal(err)
	}
	healthy, err := s.Audit(t.Context(), AuditOptions{})
	if err != nil || healthy.LiveRows != 1 || healthy.States.Ready.Lifecycle.Keys != 1 {
		t.Fatalf("report=%+v err=%v", healthy, err)
	}
	var rec storedRecord
	err = db.Update(func(txn *badger.Txn) error {
		r, e := s.loadRecord(txn, id)
		if e != nil {
			return e
		}
		rec = r
		if e = txn.Delete(s.keys.readyCreatedKey(time.Unix(0, r.CreatedAtUnix), id)); e != nil {
			return e
		}
		if e = txn.Set(s.keys.readyKey(time.Unix(0, r.AvailableAtUnix).Add(time.Second), id), []byte("wrong")); e != nil {
			return e
		}
		if e = txn.Set(s.keys.readyKey(time.Now(), 999), nil); e != nil {
			return e
		}
		dl, _ := json.Marshal(storedDeadLetter{Record: r, FailedAt: time.Now().UnixNano()})
		return txn.Set(s.keys.deadLetterKey(time.Now(), id), dl)
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := func() []byte {
		var b []byte
		err := db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			for it.Rewind(); it.Valid(); it.Next() {
				b = append(b, it.Item().Key()...)
				v, e := it.Item().ValueCopy(nil)
				if e != nil {
					return e
				}
				b = append(b, v...)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	before := snapshot()
	report, err := s.Audit(t.Context(), AuditOptions{SampleLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	r := report.States.Ready
	if r.Created.Missing != 1 || r.Lifecycle.DuplicateKeys != 1 || r.Lifecycle.TimestampMismatches != 1 || r.Lifecycle.ValueMismatches != 1 || r.Lifecycle.Orphaned != 1 {
		t.Fatalf("ready=%+v", r)
	}
	if report.DeadLetters.LiveRowCollisions != 1 || report.DeadLetters.UnexpectedRecordStates != 1 || report.DeadLetters.FailedAtMismatches != 1 {
		t.Fatalf("dead letters=%+v", report.DeadLetters)
	}
	if !bytes.Equal(before, snapshot()) {
		t.Fatal("audit mutated storage")
	}
	if rec.ID != id {
		t.Fatal("record mismatch")
	}
}

func TestAuditRejectsMalformedRowsAndBoundsSamples(t *testing.T) {
	db, s, close := openTestStore[string, string](t, "audit-bounds", Serde[string, string]{})
	defer close()
	for range 10 {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "secret", Destination: "private"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(s.keys.readyPrefix); it.ValidForPrefix(s.keys.readyPrefix); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	report, err := s.Audit(t.Context(), AuditOptions{SampleLimit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if report.States.Ready.Lifecycle.Missing != 10 || len(report.Samples.Anomalies) != 2 {
		t.Fatalf("report=%+v", report)
	}
	encoded, _ := json.Marshal(report)
	if bytes.Contains(encoded, []byte("secret")) || bytes.Contains(encoded, []byte("private")) {
		t.Fatal("contents exposed")
	}
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.messageKey(0), []byte(`{"status":"ready"}`)) }); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Audit(t.Context(), AuditOptions{}); err == nil {
		t.Fatal("accepted missing record id")
	}
	if _, err = s.Audit(nil, AuditOptions{}); err != ErrNilContext {
		t.Fatalf("nil context: %v", err)
	}
}
