package badgerbox

import (
	"bytes"
	"context"
	"errors"
	"github.com/dgraph-io/badger/v4"
	"testing"
)

type binaryCodec struct{}

func (binaryCodec) Marshal(v []byte) ([]byte, error)   { return bytes.Clone(v), nil }
func (binaryCodec) Unmarshal(v []byte) ([]byte, error) { return bytes.Clone(v), nil }

func TestBinaryCodecAndIndependentDefaults(t *testing.T) {
	_, s, cleanup := openTestStore[[]byte, testDestination](t, "binary", Serde[[]byte, testDestination]{Message: binaryCodec{}})
	defer cleanup()
	for _, value := range [][]byte{nil, {}, {0, 255, 128, 1}} {
		id, err := s.Enqueue(t.Context(), EnqueueRequest[[]byte, testDestination]{Payload: value, Destination: testDestination{Route: "test"}})
		if err != nil {
			t.Fatal(err)
		}
		got, err := s.Get(t.Context(), id)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Payload, value) || got.Destination.Route != "test" || got.State != MessageStateReady {
			t.Fatalf("round trip: %#v", got)
		}
	}
}
func TestRejectIncompatibleFormatWithoutMutation(t *testing.T) {
	for _, version := range [][]byte{nil, {1}, {255}} {
		t.Run(string(version), func(t *testing.T) {
			db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			keys := newKeyspace("old")
			original := []byte("preserve me")
			if err := db.Update(func(txn *badger.Txn) error {
				if err := txn.Set(keys.messageKey(1), original); err != nil {
					return err
				}
				if version != nil {
					return txn.Set(keys.queueStateVersionKey, version)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := New[string, string](db, Serde[string, string]{}, Options{Namespace: "old"}); !errors.Is(err, ErrIncompatibleFormat) {
				t.Fatalf("error = %v", err)
			}
			if err := db.View(func(txn *badger.Txn) error {
				if _, err := txn.Get(keys.sequenceKey); !errors.Is(err, badger.ErrKeyNotFound) {
					t.Fatalf("sequence created: %v", err)
				}
				item, err := txn.Get(keys.messageKey(1))
				if err != nil {
					return err
				}
				b, err := item.ValueCopy(nil)
				if !bytes.Equal(b, original) {
					t.Fatal("record changed")
				}
				return err
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestPublicStoreRejectsNilContext(t *testing.T) {
	_, s, cleanup := openTestStore[string, string](t, "nil-context", Serde[string, string]{})
	defer cleanup()
	if _, err := s.Enqueue(nil, EnqueueRequest[string, string]{}); !errors.Is(err, ErrNilContext) {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := s.Get(nil, 0); !errors.Is(err, ErrNilContext) {
		t.Fatalf("get: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Enqueue(ctx, EnqueueRequest[string, string]{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("enqueue canceled: %v", err)
	}
}
