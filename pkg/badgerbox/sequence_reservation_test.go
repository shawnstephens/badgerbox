package badgerbox

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func openSequenceTestDB(t *testing.T) *badger.DB {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestFailedReservationCannotExposeUncommittedRange(t *testing.T) {
	db := openSequenceTestDB(t)
	seq, err := newMessageSequence(db, []byte("seq"), 2)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := seq.Next(); err != nil {
			t.Fatal(err)
		}
	}
	beforeNext, beforeEnd := seq.next, seq.leased
	err = seq.reserve(func(fn func(*badger.Txn) error) error {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		if err := fn(txn); err != nil {
			return err
		}
		return badger.ErrConflict
	})
	if !errors.Is(err, badger.ErrConflict) {
		t.Fatal(err)
	}
	if seq.next != beforeNext || seq.leased != beforeEnd {
		t.Fatal("failed commit changed in-memory reservation")
	}
	other, err := newMessageSequence(db, []byte("seq"), 2)
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := other.Next()
	if err != nil {
		t.Fatal(err)
	}
	id, err := seq.Next()
	if err != nil {
		t.Fatal(err)
	}
	if id <= otherID {
		t.Fatalf("reused failed range: id=%d other=%d", id, otherID)
	}
}

func TestSequenceRejectsMalformedAndOverflowWithoutMutation(t *testing.T) {
	for _, input := range [][]byte{{1, 2, 3}, {255, 255, 255, 255, 255, 255, 255, 254}} {
		db := openSequenceTestDB(t)
		key := []byte("seq")
		if err := db.Update(func(txn *badger.Txn) error { return txn.Set(key, input) }); err != nil {
			t.Fatal(err)
		}
		if _, err := newMessageSequence(db, key, 2); err == nil {
			t.Fatal("invalid reservation accepted")
		}
		if err := db.View(func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if err != nil {
				return err
			}
			return item.Value(func(got []byte) error {
				if string(got) != string(input) {
					t.Fatal("rejected reservation mutated disk")
				}
				return nil
			})
		}); err != nil {
			t.Fatal(err)
		}
	}
	db := openSequenceTestDB(t)
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], math.MaxUint64-1)
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set([]byte("seq"), value[:]) }); err != nil {
		t.Fatal(err)
	}
	seq, err := newMessageSequence(db, []byte("seq"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := seq.Next(); err != nil || id != math.MaxUint64-1 {
		t.Fatalf("last ID=%d err=%v", id, err)
	}
	if _, err := seq.Next(); !errors.Is(err, ErrMessageIDExhausted) {
		t.Fatalf("overflow=%v", err)
	}
}

func TestReleasePreservesOtherReservationsAndEncoding(t *testing.T) {
	db := openSequenceTestDB(t)
	key := []byte("seq")
	old, err := db.GetSequence(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := old.Next(); err != nil || id != 0 {
		t.Fatalf("old=%d %v", id, err)
	}
	seq, err := newMessageSequence(db, key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := old.Release(); err != nil {
		t.Fatal(err)
	}
	if id, err := seq.Next(); err != nil || id != 2 {
		t.Fatalf("new=%d %v", id, err)
	}
	if err := seq.Release(); err != nil {
		t.Fatal(err)
	}
	compat, err := db.GetSequence(key, 2)
	if err != nil {
		t.Fatal(err)
	}
	if id, err := compat.Next(); err != nil || id != 3 {
		t.Fatalf("compatible=%d %v", id, err)
	}
}
