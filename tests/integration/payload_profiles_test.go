//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

type profileCodec struct{}

func (profileCodec) Marshal(v []byte) ([]byte, error)   { return bytes.Clone(v), nil }
func (profileCodec) Unmarshal(v []byte) ([]byte, error) { return bytes.Clone(v), nil }

// Exercise the whole on-disk lifecycle under a small Badger memory profile.
// Random binary data prevents compression from hiding large-value pressure.
func TestDurablePayloadProfiles(t *testing.T) {
	for _, profile := range []struct{ bytes, count, batch int }{
		{1024, 1200, 64}, {64 << 10, 256, 32}, {512 << 10, 48, 16},
	} {
		t.Run(fmt.Sprintf("payload_%d", profile.bytes), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			path := t.TempDir()
			var db *badger.DB
			var store *badgerbox.Store[[]byte, string]
			open := func() {
				t.Helper()
				var err error
				db, err = badger.Open(badger.DefaultOptions(path).WithLogger(nil).
					WithSyncWrites(true).WithMemTableSize(8 << 20).WithNumMemtables(3).
					WithBlockCacheSize(8 << 20).WithIndexCacheSize(8 << 20).
					WithValueThreshold(1024).WithValueLogFileSize(8 << 20))
				if err != nil {
					t.Fatal(err)
				}
				store, err = badgerbox.New[[]byte, string](db,
					badgerbox.Serde[[]byte, string]{Message: profileCodec{}}, badgerbox.Options{Namespace: "profiles"})
				if err != nil {
					_ = db.Close()
					t.Fatal(err)
				}
			}
			closeDB := func() {
				t.Helper()
				if err := errors.Join(store.Close(), db.Close()); err != nil {
					t.Fatal(err)
				}
			}
			open()
			t.Cleanup(func() { _ = store.Close(); _ = db.Close() })
			ids := make([]badgerbox.MessageID, profile.count)
			hashes := make([][32]byte, profile.count)
			random := rand.NewChaCha8([32]byte{1})
			for i := range profile.count {
				payload := make([]byte, profile.bytes)
				_, _ = random.Read(payload)
				binary.BigEndian.PutUint64(payload, uint64(i))
				hashes[i] = sha256.Sum256(payload)
				// The application state and outbox entry must survive together.
				if err := db.Update(func(txn *badger.Txn) error {
					var err error
					ids[i], err = store.EnqueueTx(ctx, txn, badgerbox.EnqueueRequest[[]byte, string]{Payload: payload, Destination: "verified"})
					if err != nil {
						return err
					}
					return txn.Set([]byte(fmt.Sprintf("app/%d", i)), hashes[i][:])
				}); err != nil {
					t.Fatal(err)
				}
			}
			closeDB()
			open()
			for i, id := range ids {
				msg, err := store.Get(ctx, id)
				if err != nil || sha256.Sum256(msg.Payload) != hashes[i] || msg.Destination != "verified" {
					t.Fatalf("reopened message %d corrupted: %v", i, err)
				}
				if err := db.View(func(txn *badger.Txn) error {
					item, err := txn.Get([]byte(fmt.Sprintf("app/%d", i)))
					if err != nil {
						return err
					}
					return item.Value(func(v []byte) error {
						if !bytes.Equal(v, hashes[i][:]) {
							return errors.New("application state differs from committed payload")
						}
						return nil
					})
				}); err != nil {
					t.Fatal(err)
				}
			}
			var mu sync.Mutex
			succeeded := make([]bool, profile.count)
			retries := 0
			run := func(injectFailures bool, deadLetters int64) {
				t.Helper()
				processor, err := badgerbox.NewBatchProcessor(store, func(_ context.Context, messages []badgerbox.Message[[]byte, string], results chan<- badgerbox.BatchProcessResult) error {
					mu.Lock()
					defer mu.Unlock()
					for _, msg := range messages {
						if len(msg.Payload) < 8 {
							return errors.New("short payload")
						}
						i := binary.BigEndian.Uint64(msg.Payload)
						if i >= uint64(len(hashes)) || sha256.Sum256(msg.Payload) != hashes[i] || msg.ID != ids[i] || msg.Destination != "verified" {
							return errors.New("delivery identity or payload corrupted")
						}
						var resultErr error
						switch {
						case injectFailures && i%20 == 0:
							resultErr = badgerbox.Permanent(errors.New("injected rejection"))
						case injectFailures && i%10 == 1 && msg.Attempt == 1:
							retries++
							resultErr = errors.New("injected transient failure")
						default:
							if succeeded[i] {
								return errors.New("duplicate delivery without expired lease")
							}
							succeeded[i] = true
						}
						results <- badgerbox.BatchProcessResult{ID: msg.ID, Err: resultErr}
					}
					return nil
				}, badgerbox.BatchProcessorOptions{ClaimBatchSize: profile.batch, ProcessorOptions: badgerbox.ProcessorOptions{
					Concurrency: 4, PollInterval: time.Millisecond, LeaseDuration: 30 * time.Second,
					RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond,
				}})
				if err != nil {
					t.Fatal(err)
				}
				runCtx, stop := context.WithCancel(ctx)
				done := make(chan error, 1)
				go func() { done <- processor.Run(runCtx) }()
				defer func() {
					stop()
					if err := <-done; err != nil {
						t.Error(err)
					}
				}()
				for {
					snapshot, err := store.QueueSnapshot(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if snapshot.ReadyDepth == 0 && snapshot.ProcessingDepth == 0 {
						if snapshot.DeadLetterDepth != deadLetters {
							t.Fatalf("dead letters=%d, want %d", snapshot.DeadLetterDepth, deadLetters)
						}
						return
					}
					select {
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					case <-time.After(5 * time.Millisecond):
					}
				}
			}
			deadCount := int64((profile.count + 19) / 20)
			run(true, deadCount)
			if retries != (profile.count+8)/10 {
				t.Fatalf("retry count=%d", retries)
			}
			closeDB()
			open()
			letters, next, err := store.ListDeadLetterMetadata(ctx, badgerbox.DeadLetterListOptions{Limit: 1000, MaxBytes: 128})
			if err != nil || next != nil || int64(len(letters)) != deadCount {
				t.Fatalf("metadata page count=%d, next=%x, err=%v", len(letters), next, err)
			}
			for _, letter := range letters {
				if !letter.Oversized || letter.Details != nil {
					t.Fatal("expected key-only metadata under 128-byte page budget")
				}
				if err := store.RequeueDeadLetter(ctx, letter.ID, letter.FailedAt, time.Time{}); err != nil {
					t.Fatal(err)
				}
			}
			run(false, 0)
			for i, seen := range succeeded {
				if !seen {
					t.Fatalf("message %d never succeeded", i)
				}
			}
			closeDB()
			open()
			report, err := store.Audit(ctx, badgerbox.AuditOptions{})
			if err != nil || !report.Complete || report.LiveRows != 0 || report.DeadLetters.Rows != 0 || len(report.Samples.Anomalies) != 0 {
				t.Fatalf("final audit=%+v err=%v", report, err)
			}
			for _, id := range ids {
				if _, err := store.Get(ctx, id); !errors.Is(err, badgerbox.ErrNotFound) {
					t.Fatalf("acknowledged message reappeared: %v", err)
				}
			}
		})
	}
}
