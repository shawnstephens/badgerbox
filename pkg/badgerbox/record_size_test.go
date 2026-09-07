package badgerbox

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
)

func TestEnqueueRejectsUnclaimableRecordBeforeQueueWrites(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithMemTableSize(1 << 20).WithValueThreshold(157286))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	s, err := New[string, string](db, Serde[string, string]{}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	req := EnqueueRequest[string, string]{Payload: strings.Repeat("x", 117500), Destination: "test"}
	if _, err := s.Enqueue(t.Context(), req); !errors.Is(err, ErrMessageTooLarge) || !errors.Is(err, badger.ErrTxnTooBig) {
		t.Fatalf("enqueue err=%v, want lifecycle size rejection", err)
	}
	// A caller can still commit its unrelated writes after admission rejects a
	// record: EnqueueTx must not leave any partial queue entries in that txn.
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("business"), []byte("kept")); err != nil {
			return err
		}
		if _, err := s.EnqueueTx(t.Context(), txn, req); !errors.Is(err, ErrMessageTooLarge) {
			t.Fatalf("EnqueueTx err=%v", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range [][]byte{s.keys.messagePrefix, s.keys.readyPrefix, s.keys.processingPrefix, s.keys.readyCreatedPrefix, s.keys.processingCreatedPrefix, s.keys.deadLetterPrefix} {
		if n := countKeysWithPrefix(t, db, prefix); n != 0 {
			t.Fatalf("rejection left %d keys under %q", n, prefix)
		}
	}
	id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "small", Destination: "test"})
	if err != nil {
		t.Fatal(err)
	}
	work, err := s.claimReadyBatch(t.Context(), time.Now(), 1, time.Minute, 3)
	if err != nil || len(work) != 1 || work[0].Message.ID != id {
		t.Fatalf("claim=%v err=%v", work, err)
	}
	if err := s.acknowledge(t.Context(), id, work[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedRecordsCompleteLifecycleNearBadgerLimits(t *testing.T) {
	for _, config := range []struct {
		name      string
		threshold int64
		dynamic   bool
		memory    bool
	}{
		{"inline-limit", 157286, false, false},
		{"value-log", 128 << 10, false, false},
		{"adaptive-threshold", 1024, true, false},
		{"memory", 157286, false, true},
	} {
		t.Run(config.name, func(t *testing.T) {
			opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil).WithMemTableSize(1 << 20).WithValueThreshold(config.threshold).WithValueLogFileSize(1 << 20)
			if config.dynamic {
				opts = opts.WithVLogPercentile(0.99)
			}
			if config.memory {
				opts = opts.WithDir("").WithValueDir("").WithInMemory(true)
			}
			db, err := badger.Open(opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
			runtime.tokenFunc = func() (string, error) { return strings.Repeat("\x00", maxLeaseTokenBytes), nil }
			s, err := New[string, string](db, Serde[string, string]{}, Options{Runtime: runtime})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			acceptedLarge := false
			for _, size := range []int{1, 80 << 10, 100 << 10, 117500, 160 << 10, 720 << 10, 770 << 10} {
				id, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: strings.Repeat("x", size), Destination: "test"})
				if errors.Is(err, ErrMessageTooLarge) {
					continue
				}
				if err != nil {
					t.Fatalf("size=%d: enqueue: %v", size, err)
				}
				if size >= 160<<10 {
					acceptedLarge = true
				}
				claim := func() claimedRecord[string, string] {
					t.Helper()
					work, err := s.claimReadyBatch(t.Context(), runtime.Now(), 1, time.Second, math.MaxInt)
					if err != nil || len(work) != 1 || work[0].Message.ID != id {
						t.Fatalf("size=%d: claim=%v err=%v", size, work, err)
					}
					return work[0]
				}
				work := claim()
				if _, err := s.failProcessing(t.Context(), id, work.LeaseToken, errors.New("retry"), time.Second, time.Second); err != nil {
					t.Fatalf("size=%d: retry: %v", size, err)
				}
				runtime.SetNow(runtime.Now().Add(2 * time.Second))
				claim()
				runtime.SetNow(runtime.Now().Add(2 * time.Second))
				if n, err := s.requeueExpired(t.Context(), runtime.Now(), 1); err != nil || n != 1 {
					t.Fatalf("size=%d: recovery=%d err=%v", size, n, err)
				}
				work = claim()
				if _, err := s.failProcessing(t.Context(), id, work.LeaseToken, Permanent(errors.New(strings.Repeat("\x00", 2<<20))), time.Second, time.Second); err != nil {
					t.Fatalf("size=%d: dead-letter: %v", size, err)
				}
				letters, _, err := s.ListDeadLetters(t.Context(), 1, nil)
				if err != nil || len(letters) != 1 {
					t.Fatalf("size=%d: letters=%d err=%v", size, len(letters), err)
				}
				if len(letters[0].Error) > maxStoredErrorBytes || !strings.HasSuffix(letters[0].Error, "[truncated]") {
					t.Fatal("failure text was not bounded")
				}
				if err := s.RequeueDeadLetter(t.Context(), id, letters[0].FailedAt, runtime.Now()); err != nil {
					t.Fatalf("size=%d: requeue dead letter: %v", size, err)
				}
				work = claim()
				if err := s.acknowledge(t.Context(), id, work.LeaseToken); err != nil {
					t.Fatal(err)
				}
				if q, err := s.QueueSnapshot(t.Context()); err != nil || q != (QueueSnapshot{}) {
					t.Fatalf("size=%d: residual snapshot=%+v err=%v", size, q, err)
				}
			}
			if !config.memory && !acceptedLarge {
				t.Fatal("safe value-log records were all rejected")
			}
		})
	}
}

func TestStoredFailureTextPreservesBoundedUTF8(t *testing.T) {
	for _, text := range []string{"short", strings.Repeat("€", maxStoredErrorBytes), strings.Repeat("\xff", maxStoredErrorBytes*2)} {
		got := storedFailureText(errors.New(text))
		if len(got) > maxStoredErrorBytes || !utf8.ValidString(got) {
			t.Fatalf("invalid stored failure text: len=%d valid=%v", len(got), utf8.ValidString(got))
		}
		if text == "short" && got != text {
			t.Fatalf("short error changed: %q", got)
		}
	}
}

func TestInvalidRuntimeTokenDoesNotChangeRecord(t *testing.T) {
	for _, token := range []string{"", strings.Repeat("x", maxLeaseTokenBytes+1)} {
		runtime := newFakeRuntime(time.Unix(1_700_000_000, 0))
		runtime.tokenFunc = func() (string, error) { return token, nil }
		db, s, close := openTestStoreWithOptions(t, "invalid-token", Serde[string, string]{}, Options{Runtime: runtime})
		if _, err := s.Enqueue(t.Context(), EnqueueRequest[string, string]{Payload: "test"}); err != nil {
			t.Fatal(err)
		}
		before := recordTestNamespace(t, db, s.opts.Namespace)
		if _, err := s.claimReadyBatch(t.Context(), runtime.Now(), 1, time.Minute, 3); err == nil {
			t.Fatal("invalid runtime token accepted")
		}
		after := recordTestNamespace(t, db, s.opts.Namespace)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("invalid token changed rows or indexes")
		}
		close()
	}
}
