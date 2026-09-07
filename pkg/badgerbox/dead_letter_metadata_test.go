package badgerbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func TestMetadataPagesAdvancePastOversizedRows(t *testing.T) {
	db, s, cleanup := openTestStore[[]byte, []byte](t, "metadata-pages", Serde[[]byte, []byte]{Message: rejectDecodeCodec{}, Destination: rejectDecodeCodec{}})
	defer cleanup()
	at := time.Unix(1, 0)
	if err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 5; i++ {
			id := MessageID(i)
			value := bytes.Repeat([]byte("not JSON"), 1000)
			if i%2 == 1 {
				record := validRecordForValidation(MessageStateProcessing)
				record.ID = id
				record.PayloadBytes = []byte{255}
				var err error
				value, err = json.Marshal(storedDeadLetter{Record: record, FailedAt: at.UnixNano()})
				if err != nil {
					return err
				}
			}
			if err := txn.Set(s.keys.deadLetterKey(at, id), value); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{1, 2, 5} {
		var cursor []byte
		var all []DeadLetterMetadata
		for page := 0; page < 6; page++ {
			rows, next, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: limit, Cursor: cursor, MaxBytes: 1024})
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, rows...)
			if next == nil {
				break
			}
			if bytes.Equal(next, cursor) {
				t.Fatal("cursor did not advance")
			}
			cursor = next
		}
		if len(all) != 5 {
			t.Fatalf("limit=%d rows=%d", limit, len(all))
		}
		for i, row := range all {
			if row.ID != MessageID(i) || row.Oversized != (i%2 == 0) || (row.Details == nil) != (i%2 == 0) || !row.FailedAt.Equal(at) || len(row.Cursor) == 0 {
				t.Fatalf("row=%+v", row)
			}
		}
		tail, next, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 10, Cursor: all[3].Cursor, MaxBytes: 1024})
		if err != nil || len(tail) != 1 || tail[0].ID != 4 || next != nil {
			t.Fatalf("resume rows=%+v next=%x err=%v", tail, next, err)
		}
	}
	other, err := New[[]byte, []byte](db, Serde[[]byte, []byte]{}, Options{Namespace: "other"})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, _, err := other.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 1, Cursor: s.keys.deadLetterKey(at, 1)}); err == nil {
		t.Fatal("cross-namespace cursor accepted")
	}
	if err := s.RequeueDeadLetterWithOptions(t.Context(), 0, at, DeadLetterRequeueOptions{MaxBytes: 1024}); !errors.Is(err, ErrDeadLetterTooLarge) {
		t.Fatalf("oversize requeue=%v", err)
	}
	if err := s.RequeueDeadLetterWithOptions(t.Context(), 1, at, DeadLetterRequeueOptions{MaxBytes: 1024}); err != nil {
		t.Fatal(err)
	}
	// Requeue must not decode application bytes, and an existing live ID must survive.
	value, _ := json.Marshal(storedDeadLetter{Record: func() storedRecord { r := validRecordForValidation(MessageStateProcessing); r.ID = 1; return r }(), FailedAt: at.UnixNano()})
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.deadLetterKey(at, 1), value) }); err != nil {
		t.Fatal(err)
	}
	if err := s.RequeueDeadLetterWithOptions(t.Context(), 1, at, DeadLetterRequeueOptions{MaxBytes: 1024}); !errors.Is(err, ErrLiveMessageExists) {
		t.Fatalf("collision=%v", err)
	}
}

func TestMetadataFailureTextIsBounded(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "metadata-text", Serde[string, string]{})
	defer cleanup()
	at := time.Unix(1, 0)
	record := validRecordForValidation(MessageStateProcessing)
	value, err := json.Marshal(storedDeadLetter{Record: record, FailedAt: at.UnixNano(), Error: strings.Repeat("界", 1000)})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.deadLetterKey(at, record.ID), value) }); err != nil {
		t.Fatal(err)
	}
	rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 1, MaxBytes: 8192})
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	if d := rows[0].Details; d == nil || !d.FailureTextTruncated || len(d.FailureText) > 1024 || strings.Contains(d.FailureText, "�") {
		t.Fatalf("details=%+v", d)
	}
}
