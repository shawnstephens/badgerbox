package badgerbox

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type quarantineCodec struct {
	broken      atomic.Bool
	panicDecode bool
	calls       atomic.Int64
}

func (c *quarantineCodec) Marshal(v string) ([]byte, error) { return []byte(v), nil }
func (c *quarantineCodec) Unmarshal(v []byte) (string, error) {
	c.calls.Add(1)
	if c.broken.Load() && string(v) == "poison" {
		copy(v, "MUTATE")
		if c.panicDecode {
			panic("codec cannot read this schema")
		}
		return "", errors.New("codec cannot read this schema")
	}
	return string(v), nil
}

func TestCodecFailureQuarantinesOnlyAffectedRecordAndPreservesOpaqueBytes(t *testing.T) {
	for _, field := range []string{"payload", "destination"} {
		for _, panicDecode := range []bool{false, true} {
			t.Run(field+map[bool]string{false: "-error", true: "-panic"}[panicDecode], func(t *testing.T) {
				codec := &quarantineCodec{panicDecode: panicDecode}
				codec.broken.Store(true)
				serde := Serde[string, string]{}
				if field == "payload" {
					serde.Message = codec
				} else {
					serde.Destination = codec
				}
				db, s, cleanup := openTestStore(t, "codec-quarantine", serde)
				defer cleanup()
				req := EnqueueRequest[string, string]{Payload: "good", Destination: "good"}
				if field == "payload" {
					req.Payload = "poison"
				} else {
					req.Destination = "poison"
				}
				poison, err := s.Enqueue(t.Context(), req)
				if err != nil {
					t.Fatal(err)
				}
				var original storedRecord
				if err := db.View(func(txn *badger.Txn) error { original, err = s.loadRecord(txn, poison); return err }); err != nil {
					t.Fatal(err)
				}
				healthy, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "good", Destination: "good"})
				if err != nil {
					t.Fatal(err)
				}
				work, err := s.claimReadyBatch(t.Context(), time.Now(), 2, time.Minute, 3)
				if err != nil || len(work) != 1 || work[0].Message.ID != healthy {
					t.Fatalf("work=%v err=%v", work, err)
				}
				rows, _, err := s.ListDeadLetterMetadata(t.Context(), DeadLetterListOptions{Limit: 2})
				if err != nil || len(rows) != 1 || rows[0].ID != poison || rows[0].Details == nil {
					t.Fatalf("rows=%+v err=%v", rows, err)
				}
				if !rows[0].Details.Permanent || rows[0].Details.Attempt != 1 || !strings.Contains(rows[0].Details.FailureText, field) || !strings.Contains(rows[0].Details.FailureText, "codec") {
					t.Fatalf("details=%+v", rows[0].Details)
				}
				var dlq storedDeadLetter
				if err := db.View(func(txn *badger.Txn) error {
					item, err := txn.Get(rows[0].Cursor)
					if err != nil {
						return err
					}
					value, err := item.ValueCopy(nil)
					if err != nil {
						return err
					}
					dlq, err = decodeStoredDeadLetter(value)
					return err
				}); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(original.PayloadBytes, dlq.Record.PayloadBytes) || !bytes.Equal(original.DestinationBytes, dlq.Record.DestinationBytes) {
					t.Fatal("codec mutation corrupted quarantined bytes")
				}
				audit, err := s.Audit(t.Context(), AuditOptions{})
				if err != nil || !audit.Complete || audit.DeadLetters.UnexpectedRecordStates != 0 {
					t.Fatalf("audit=%+v err=%v", audit, err)
				}
				// Exact requeue and metadata inspection must not invoke the broken codec.
				calls := codec.calls.Load()
				if err := s.RequeueDeadLetter(t.Context(), poison, rows[0].FailedAt, time.Now()); err != nil {
					t.Fatal(err)
				}
				if codec.calls.Load() != calls {
					t.Fatal("requeue invoked codec")
				}
				codec.broken.Store(false)
				replayed, err := s.claimReadyBatch(t.Context(), time.Now(), 2, time.Minute, 3)
				if err != nil || len(replayed) != 1 || replayed[0].Message.ID != poison {
					t.Fatalf("replayed=%v err=%v", replayed, err)
				}
				if replayed[0].Message.Payload != req.Payload || replayed[0].Message.Destination != req.Destination {
					t.Fatal("replay changed opaque bytes")
				}
			})
		}
	}
}

func TestPoisonHeadDoesNotWaitForPollInterval(t *testing.T) {
	codec := &quarantineCodec{}
	codec.broken.Store(true)
	_, s, cleanup := openTestStore(t, "poison-head", Serde[string, string]{Message: codec})
	defer cleanup()
	for _, payload := range []string{"poison", "poison", "good"} {
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: payload}); err != nil {
			t.Fatal(err)
		}
	}
	processed := make(chan struct{}, 1)
	p, err := NewProcessor(s, func(ctx context.Context, m Message[string, string]) error {
		if m.Payload != "good" {
			t.Errorf("poison callback %q", m.Payload)
		}
		processed <- struct{}{}
		return nil
	}, ProcessorOptions{Concurrency: 1, PollInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runProcessor(p)
	defer stopProcessor(t, cancel, done)
	select {
	case <-processed:
	case err := <-done:
		t.Fatalf("processor stopped: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("healthy message behind poison head was not delivered")
	}
}

func TestCorruptEnvelopeStillFailsClosed(t *testing.T) {
	db, s, cleanup := openTestStore[string, string](t, "corrupt-envelope", Serde[string, string]{})
	defer cleanup()
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "good"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(s.keys.messageKey(id), []byte(`{"id":0}`)) }); err != nil {
		t.Fatal(err)
	}
	before := recordTestNamespace(t, db, s.opts.Namespace)
	if _, err := s.claimReadyBatch(t.Context(), time.Now(), 10, time.Minute, 3); err == nil {
		t.Fatal("invalid storage envelope was accepted")
	}
	if after := recordTestNamespace(t, db, s.opts.Namespace); !reflect.DeepEqual(before, after) {
		t.Fatal("corrupt envelope changed queue rows")
	}
}
