package badgerbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
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

func TestSnapshotRejectsMissingCreationIndex(t *testing.T) {
	for _, processing := range []bool{false, true} {
		name := "ready"
		if processing {
			name = "processing"
		}
		t.Run(name, func(t *testing.T) {
			runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
			provider := sdkmetric.NewMeterProvider()
			defer provider.Shutdown(t.Context())
			db, s, close := openTestStoreWithOptions(t, "missing-created", Serde[string, string]{}, Options{
				Runtime: runtime, Observability: ObservabilityOptions{MeterProvider: provider},
			})
			defer close()
			id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "payload", Destination: "destination"})
			if err != nil {
				t.Fatal(err)
			}
			key := s.keys.readyCreatedKey(runtime.Now(), id)
			if processing {
				work, err := s.claimReadyBatch(t.Context(), runtime.Now(), 1, time.Minute, defaultMaxAttempts)
				if err != nil || len(work) != 1 {
					t.Fatalf("claim=%v err=%v", work, err)
				}
				key = s.keys.processingCreatedKey(runtime.Now(), id)
			}
			// A real zero age is valid while the creation index still exists.
			if _, err := s.QueueSnapshot(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(txn *badger.Txn) error { return txn.Delete(key) }); err != nil {
				t.Fatal(err)
			}
			if q, err := s.QueueSnapshot(t.Context()); !errors.Is(err, ErrInconsistentIndex) || q != (QueueSnapshot{}) {
				t.Fatalf("snapshot=%+v err=%v, want consistency error and no partial snapshot", q, err)
			}
			if err := s.RecordObservabilitySnapshot(t.Context()); !errors.Is(err, ErrInconsistentIndex) {
				t.Fatalf("observability snapshot error=%v", err)
			}
		})
	}
}

func TestEmptySnapshotHasZeroAges(t *testing.T) {
	_, s, close := openTestStore[string, string](t, "empty-snapshot", Serde[string, string]{})
	defer close()
	if q, err := s.QueueSnapshot(t.Context()); err != nil || q != (QueueSnapshot{}) {
		t.Fatalf("snapshot=%+v err=%v", q, err)
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
