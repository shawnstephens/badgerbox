//go:build resourcefault && (darwin || linux)

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/internal/diskspace"
	"github.com/shawnstephens/badgerbox/pkg/admission"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
)

// This opt-in test fills only an explicitly marked, bounded disposable mount.
// See docs/RESOURCE_FAULTS.md. It must never run on the workspace filesystem.
func TestFullFilesystemRejectsIntakeAndRecoversRetainedMessages(t *testing.T) {
	volume := os.Getenv("BADGERBOX_TEST_VOLUME")
	if volume == "" || !filepath.IsAbs(volume) {
		t.Fatal("BADGERBOX_TEST_VOLUME must name an absolute disposable mount point")
	}
	marker, err := os.ReadFile(filepath.Join(volume, ".badgerbox-disposable-volume"))
	if err != nil || string(marker) != "badgerbox resource-fault fixture v1\n" {
		t.Fatal("missing disposable-volume marker")
	}
	root, err := os.Stat(volume)
	if err != nil {
		t.Fatal(err)
	}
	parent, err := os.Stat(filepath.Dir(filepath.Clean(volume)))
	if err != nil {
		t.Fatal(err)
	}
	if root.Sys().(*syscall.Stat_t).Dev == parent.Sys().(*syscall.Stat_t).Dev {
		t.Fatal("test volume must be a separate mounted filesystem")
	}
	workspace, err := os.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	if root.Sys().(*syscall.Stat_t).Dev == workspace.Sys().(*syscall.Stat_t).Dev {
		t.Fatal("test volume must not contain the working directory")
	}
	total, _, err := diskspace.Read(volume)
	if err != nil || total < 128<<20 || total > 1<<30 {
		t.Fatalf("fixture must be between 128 MiB and 1 GiB: total=%d err=%v", total, err)
	}
	path, err := os.MkdirTemp(volume, "badgerbox-pressure-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(path)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	dbPath := filepath.Join(path, "db")
	opts := badger.DefaultOptions(dbPath).WithLogger(nil).WithSyncWrites(true).
		WithMemTableSize(4 << 20).WithNumMemtables(2).WithBlockCacheSize(4 << 20).
		WithIndexCacheSize(4 << 20).WithValueThreshold(1024).WithValueLogFileSize(8 << 20)
	db, err := badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	guard, err := admission.NewDiskGuard(admission.DiskGuardOptions{Paths: []string{dbPath}, MinFreeBytes: 16 << 20})
	if err != nil {
		t.Fatal(err)
	}
	storeOpts := badgerbox.Options{Namespace: "pressure", EnqueueGuard: guard.Check, AdmissionLimits: badgerbox.AdmissionLimits{MaxRetainedMessages: 64, MaxRetainedBytes: 1 << 20}}
	store, err := badgerbox.New[string, string](db, badgerbox.Serde[string, string]{}, storeOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if store != nil {
			_ = store.Close()
		}
	}()
	expected := make(map[badgerbox.MessageID]string)
	for i := range 32 {
		payload := fmt.Sprintf("%d:%s", i, strings.Repeat("retained", 512))
		id, err := store.Enqueue(ctx, badgerbox.EnqueueRequest[string, string]{Payload: payload, Destination: "verified"})
		if err != nil {
			t.Fatal(err)
		}
		expected[id] = payload
	}
	before, err := store.Usage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fillerPath := filepath.Join(path, "filler")
	filler, err := os.Create(fillerPath)
	if err != nil {
		t.Fatal(err)
	}
	// Removing filler precedes database cleanup on every exit, so cleanup has
	// workspace even if a test assertion fails while the volume is exhausted.
	defer os.Remove(fillerPath)
	defer filler.Close()
	buffer := []byte(strings.Repeat("f", 1<<20))
	var written int64
	for written <= total {
		if err := ctx.Err(); err != nil {
			t.Fatal(err)
		}
		n, writeErr := filler.Write(buffer)
		written += int64(n)
		if writeErr != nil {
			err = writeErr
			break
		}
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("expected ENOSPC within bounded fixture, wrote=%d err=%v", written, err)
	}
	_ = filler.Close()
	_, available, err := diskspace.Read(volume)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("fixture_capacity=%d filler_bytes=%d free_bytes_after_ENOSPC=%d", total, written, available)
	if _, err := store.Enqueue(ctx, badgerbox.EnqueueRequest[string, string]{Payload: "must reject"}); !errors.Is(err, admission.ErrDiskPressure) {
		t.Fatalf("enqueue at full volume: %v", err)
	}
	if err := db.Update(func(txn *badger.Txn) error {
		if err := txn.Set([]byte("app/must-rollback"), []byte("pending")); err != nil {
			return err
		}
		_, err := store.EnqueueTx(ctx, txn, badgerbox.EnqueueRequest[string, string]{Payload: "must also reject"})
		return err
	}); !errors.Is(err, admission.ErrDiskPressure) {
		t.Fatalf("transaction at full volume: %v", err)
	}
	after, err := store.Usage(ctx)
	if err != nil || after != before {
		t.Fatalf("rejection changed retained usage: before=%+v after=%+v err=%v", before, after, err)
	}
	if err := os.Remove(fillerPath); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = badger.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	store, err = badgerbox.New[string, string](db, badgerbox.Serde[string, string]{}, storeOpts)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(txn *badger.Txn) error { _, err := txn.Get([]byte("app/must-rollback")); return err }); !errors.Is(err, badger.ErrKeyNotFound) {
		t.Fatalf("rejected application state committed: %v", err)
	}
	for id, payload := range expected {
		msg, err := store.Get(ctx, id)
		if err != nil || msg.Payload != payload || msg.Destination != "verified" {
			t.Fatalf("retained payload %s: %v", id, err)
		}
	}
	var mu sync.Mutex
	seen := make(map[badgerbox.MessageID]bool)
	processor, err := badgerbox.NewBatchProcessor(store, func(_ context.Context, messages []badgerbox.Message[string, string], results chan<- badgerbox.BatchProcessResult) error {
		mu.Lock()
		defer mu.Unlock()
		for _, msg := range messages {
			if seen[msg.ID] || expected[msg.ID] != msg.Payload {
				return errors.New("duplicate or invalid retained delivery")
			}
			seen[msg.ID] = true
			results <- badgerbox.BatchProcessResult{ID: msg.ID}
		}
		return nil
	}, badgerbox.BatchProcessorOptions{ClaimBatchSize: 4, ProcessorOptions: badgerbox.ProcessorOptions{ClaimMaxBytes: 64 << 10, Concurrency: 2, PollInterval: time.Millisecond}})
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
		usage, err := store.Usage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if usage.RetainedMessages == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(5 * time.Millisecond):
		}
	}
	report, err := store.Audit(ctx, badgerbox.AuditOptions{})
	if err != nil || !report.Complete || !report.Usage.Matches || report.LiveRows != 0 || report.DeadLetters.Rows != 0 || len(report.Samples.Anomalies) != 0 ||
		report.States.Ready.Lifecycle.Keys != 0 || report.States.Ready.Created.Keys != 0 ||
		report.States.Processing.Lifecycle.Keys != 0 || report.States.Processing.Created.Keys != 0 {
		t.Fatalf("final audit=%+v err=%v", report, err)
	}
	mu.Lock()
	count := len(seen)
	mu.Unlock()
	if count != len(expected) {
		t.Fatalf("delivered %d of %d retained messages", count, len(expected))
	}
	t.Logf("retained_messages_verified=%d rejected_transactions=2 final_retained_messages=0", count)
}
