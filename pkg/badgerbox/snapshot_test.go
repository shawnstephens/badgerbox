package badgerbox

import (
	"context"
	"github.com/dgraph-io/badger/v4"
	"strings"
	"testing"
)

type rejectDecodeCodec struct{ binaryCodec }

func (rejectDecodeCodec) Unmarshal([]byte) ([]byte, error) { panic("snapshot must not decode") }
func TestSnapshotDoesNotDecodePayload(t *testing.T) {
	_, s, close := openTestStore[[]byte, []byte](t, "snapshot-opaque", Serde[[]byte, []byte]{Message: rejectDecodeCodec{}, Destination: rejectDecodeCodec{}})
	defer close()
	if _, err := s.Enqueue(t.Context(), EnqueueRequest[[]byte, []byte]{Payload: []byte{255}, Destination: []byte{0}}); err != nil {
		t.Fatal(err)
	}
	q, err := s.QueueSnapshot(t.Context())
	if err != nil || q.ReadyDepth != 1 {
		t.Fatalf("snapshot=%+v, err=%v", q, err)
	}
}
func BenchmarkQueueSnapshot(b *testing.B) {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	s, err := New[string, string](db, Serde[string, string]{}, Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	for range 1000 {
		if _, err := s.Enqueue(b.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", 8192), Destination: "test"}); err != nil {
			b.Fatal(err)
		}
	}
	for _, test := range []struct {
		name string
		fn   func(context.Context) (QueueSnapshot, error)
	}{{"indexes", s.QueueSnapshot}, {"rows", s.queueSnapshotLegacy}} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := test.fn(b.Context()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
