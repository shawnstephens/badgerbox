package badgerbox

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"testing"
	"time"
)

func TestDeadLetterBytePagesAndExactRequeue(t *testing.T) {
	db, s, close := openTestStore[[]byte, []byte](t, "dlq-pages", Serde[[]byte, []byte]{Message: binaryCodec{}, Destination: binaryCodec{}})
	defer close()
	failed := time.Now().UTC()
	var size int64
	for i := range 3 {
		id := MessageID(100 + i)
		rec := storedRecord{ID: id, PayloadBytes: []byte{255, 0}, DestinationBytes: []byte{128}, Status: recordStatusProcessing, CreatedAtUnix: failed.UnixNano()}
		b, _ := json.Marshal(storedDeadLetter{Record: rec, FailedAt: failed.UnixNano()})
		size = int64(len(b))
		if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.deadLetterKey(failed, id), b) }); err != nil {
			t.Fatal(err)
		}
	}
	s.serde.Message = rejectDecodeCodec{}
	if _, _, err := s.ListDeadLettersWithOptions(t.Context(), DeadLetterListOptions{Limit: 10, MaxBytes: 1}); !errors.Is(err, ErrDeadLetterTooLarge) {
		t.Fatalf("oversize: %v", err)
	}
	s.serde.Message = binaryCodec{}
	var cursor []byte
	for i := range 3 {
		rows, next, err := s.ListDeadLettersWithOptions(t.Context(), DeadLetterListOptions{Limit: 10, MaxBytes: size, Cursor: cursor})
		if err != nil || len(rows) != 1 || rows[0].Message.ID != MessageID(100+i) {
			t.Fatalf("page=%+v err=%v", rows, err)
		}
		if (next == nil) != (i == 2) {
			t.Fatalf("cursor at page %d: %x", i, next)
		}
		cursor = next
	}
	if err := s.RequeueDeadLetter(t.Context(), 100, failed.Add(time.Nanosecond), time.Time{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("wrong time: %v", err)
	}
	if err := s.RequeueDeadLetter(t.Context(), 100, failed, time.Time{}); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(t.Context(), 100)
	if err != nil || !bytes.Equal(got.Payload, []byte{255, 0}) || got.Attempt != 0 || got.State != MessageStateReady {
		t.Fatalf("message=%+v err=%v", got, err)
	}
	rows, next, err := s.ListDeadLetters(t.Context(), 2, nil)
	if err != nil || len(rows) != 2 || next != nil {
		t.Fatalf("final count page: %d %x %v", len(rows), next, err)
	}
}
