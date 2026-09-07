//go:build integration && (linux || darwin)

package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/runner"
)

type recoveryRuntime struct {
	badgerbox.SystemRuntime
	offset time.Duration
}

func (r recoveryRuntime) Now() time.Time { return r.SystemRuntime.Now().Add(r.offset) }

func runAcknowledgedRecoveryHelper(t *testing.T, ctx context.Context, mode string) {
	t.Helper()
	const total = 12
	runtime := recoveryRuntime{}
	if mode == "ack-drain" {
		runtime.offset = 2 * time.Minute
	}
	service, err := runner.Open(ctx, runner.Options{Badger: badger.DefaultOptions(os.Getenv("BADGERBOX_RECOVERY_PATH")).WithSyncWrites(true).WithLogger(nil)})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown(context.Background())
	batchDelivered := make(chan struct{}, 1)
	store, err := runner.Register(service, badgerbox.Serde[[]byte, []byte]{Message: binaryCodec{}, Destination: binaryCodec{}}, runner.QueueOptions{
		Store:     badgerbox.Options{Namespace: "ack-recovery", Runtime: runtime},
		Processor: badgerbox.BatchProcessorOptions{ClaimBatchSize: 8, ProcessorOptions: badgerbox.ProcessorOptions{Concurrency: 1, LeaseDuration: time.Minute, PollInterval: time.Millisecond}},
	}, func(ctx context.Context, m []badgerbox.Message[[]byte, []byte], results chan<- badgerbox.BatchProcessResult) error {
		for _, message := range m {
			if len(message.Payload) != 3 || message.Payload[0] != 255 || message.Payload[1] != 0 || int(message.Payload[2]) >= total || !bytes.Equal(message.Destination, []byte{255, 128}) {
				return errors.New("binary data corrupted")
			}
			ordinal := int(message.Payload[2])
			fmt.Printf("delivered:%d\n", ordinal)
			if mode == "ack-drain" || ordinal < 4 {
				results <- badgerbox.BatchProcessResult{ID: message.ID}
			}
		}
		if mode == "ack-crash" {
			batchDelivered <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]badgerbox.MessageID, total)
	if mode == "ack-crash" {
		availableAt := runtime.Now()
		for i := range total {
			ids[i], err = store.Enqueue(ctx, badgerbox.EnqueueRequest[[]byte, []byte]{Payload: []byte{255, 0, byte(i)}, Destination: []byte{255, 128}, AvailableAt: availableAt})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if mode == "ack-crash" {
		select {
		case <-batchDelivered:
		case <-time.After(5 * time.Second):
			t.Fatal("delivery checkpoint timed out")
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		snapshot, e := store.QueueSnapshot(ctx)
		if e != nil {
			t.Fatal(e)
		}
		complete := snapshot.ReadyDepth == 0 && snapshot.ProcessingDepth == 0 && snapshot.DeadLetterDepth == 0
		if mode == "ack-crash" {
			complete = snapshot.ReadyDepth == 4 && snapshot.ProcessingDepth == 4 && snapshot.DeadLetterDepth == 0
			for i, id := range ids {
				_, e = store.Get(ctx, id)
				if i < 4 {
					complete = complete && errors.Is(e, badgerbox.ErrNotFound)
				} else if e != nil {
					t.Fatalf("remaining record %d: %v", i, e)
				}
			}
		}
		if complete {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("checkpoint timed out: %+v", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	report, e := store.Audit(ctx, badgerbox.AuditOptions{})
	if e != nil || !report.Complete || len(report.Samples.Anomalies) != 0 {
		t.Fatalf("audit=%+v err=%v", report, e)
	}
	if mode == "ack-crash" {
		for i := range 4 {
			fmt.Printf("acknowledged:%d\n", i)
		}
		fmt.Println("ready-to-stop")
		<-ctx.Done()
		t.Fatal("expected SIGKILL at committed acknowledgement checkpoint")
	}
	if err = service.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("stopped")
}

func TestAcknowledgedMessagesStayDeletedAfterSIGKILL(t *testing.T) {
	dir := t.TempDir()
	before := runHelper(t, dir, "ack-crash")
	after := runHelper(t, dir, "ack-drain")
	parse := func(lines []string, prefix string) map[int]int {
		t.Helper()
		counts := map[int]int{}
		for _, line := range lines {
			if strings.HasPrefix(line, prefix) {
				id, err := strconv.Atoi(strings.TrimPrefix(line, prefix))
				if err != nil {
					t.Fatal(line)
				}
				counts[id]++
			}
		}
		return counts
	}
	acknowledged := parse(before, "acknowledged:")
	deliveredBefore := parse(before, "delivered:")
	replayed := parse(after, "delivered:")
	if len(acknowledged) != 4 || len(deliveredBefore) != 8 || len(replayed) != 8 {
		t.Fatalf("ack=%v before=%v replay=%v", acknowledged, deliveredBefore, replayed)
	}
	for i := range 12 {
		if i < 4 {
			if acknowledged[i] != 1 || deliveredBefore[i] != 1 || replayed[i] != 0 {
				t.Fatalf("acknowledged %d resurrected or checkpoint invalid", i)
			}
		} else {
			if replayed[i] != 1 {
				t.Fatalf("record %d replay count=%d", i, replayed[i])
			}
			if (i < 8 && deliveredBefore[i] != 1) || (i >= 8 && deliveredBefore[i] != 0) {
				t.Fatalf("record %d checkpoint delivery count=%d", i, deliveredBefore[i])
			}
		}
	}
}
