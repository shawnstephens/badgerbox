//go:build integration && (linux || darwin)

package integration_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/runner"
)

const messageCount = 128

type binaryCodec struct{}

func (binaryCodec) Marshal(v []byte) ([]byte, error)   { return bytes.Clone(v), nil }
func (binaryCodec) Unmarshal(v []byte) ([]byte, error) { return bytes.Clone(v), nil }

// Each helper is a separate OS process and uses public APIs only. No parent
// process opens the database while a helper owns it.
func TestRecoveryProcessHelper(t *testing.T) {
	mode := os.Getenv("BADGERBOX_RECOVERY_MODE")
	if mode == "" {
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()
	if mode == "ack-crash" || mode == "ack-drain" {
		runAcknowledgedRecoveryHelper(t, ctx, mode)
		return
	}
	service, err := runner.Open(ctx, runner.Options{Badger: badger.DefaultOptions(os.Getenv("BADGERBOX_RECOVERY_PATH")).WithSyncWrites(true).WithLogger(nil)})
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	store, err := runner.Register(service, badgerbox.Serde[[]byte, []byte]{Message: binaryCodec{}, Destination: binaryCodec{}}, runner.QueueOptions{Store: badgerbox.Options{Namespace: "recovery"}, Processor: badgerbox.BatchProcessorOptions{ClaimBatchSize: 8, ProcessorOptions: badgerbox.ProcessorOptions{Concurrency: 2, LeaseDuration: 100 * time.Millisecond, PollInterval: 5 * time.Millisecond, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}}}, func(ctx context.Context, m []badgerbox.Message[[]byte, []byte], results chan<- badgerbox.BatchProcessResult) error {
		for _, message := range m {
			if len(message.Payload) != 3 || !bytes.Equal(message.Destination, []byte{255, 128}) {
				return errors.New("binary data corrupted")
			}
			if message.Payload[0] == 0 {
				if mode == "drain" {
					results <- badgerbox.BatchProcessResult{ID: message.ID, Err: badgerbox.Permanent(errors.New("invalid message"))}
				}
				continue
			}
			fmt.Printf("delivered:%d\n", int(message.Payload[1])<<8|int(message.Payload[2]))
			if mode == "drain" {
				results <- badgerbox.BatchProcessResult{ID: message.ID}
			}
		}
		if mode != "drain" {
			once.Do(func() { fmt.Println("ready-to-stop") })
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if mode != "drain" {
		for i := range messageCount + 1 {
			payload := []byte{255, byte(i >> 8), byte(i)}
			if i == messageCount {
				payload[0] = 0
			}
			if _, err = store.Enqueue(ctx, badgerbox.EnqueueRequest[[]byte, []byte]{Payload: payload, Destination: []byte{255, 128}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err = service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if mode == "drain" {
		deadline := time.Now().Add(10 * time.Second)
		for {
			snapshot, e := store.QueueSnapshot(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if snapshot.ReadyDepth == 0 && snapshot.ProcessingDepth == 0 && snapshot.DeadLetterDepth == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("drain timed out: %+v", snapshot)
			}
			time.Sleep(5 * time.Millisecond)
		}
		report, e := store.Audit(ctx, badgerbox.AuditOptions{})
		if e != nil || !report.Complete || len(report.Samples.Anomalies) != 0 {
			t.Fatalf("audit=%+v err=%v", report, e)
		}
		letters, next, e := store.ListDeadLetters(ctx, 1, nil)
		if e != nil || len(letters) != 1 || next != nil || letters[0].Message.Payload[0] != 0 {
			t.Fatalf("dead letters=%+v err=%v", letters, e)
		}
	} else {
		<-ctx.Done()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = service.Shutdown(shutdownCtx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("stopped")
}

func TestRecoveryAcrossGracefulAndAbruptProcessExit(t *testing.T) {
	for _, mode := range []string{"graceful", "crash"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			before := runHelper(t, dir, mode)
			after := runHelper(t, dir, "drain")
			delivered := make(map[int]bool)
			for _, line := range after {
				if strings.HasPrefix(line, "delivered:") {
					id, err := strconv.Atoi(strings.TrimPrefix(line, "delivered:"))
					if err != nil || id < 0 || id >= messageCount {
						t.Fatalf("unexpected delivery %s", line)
					}
					delivered[id] = true
				}
			}
			if len(delivered) != messageCount {
				t.Fatalf("delivered %d unique messages, want %d", len(delivered), messageCount)
			}
			sawDuplicate := false
			for _, line := range before {
				if strings.HasPrefix(line, "delivered:") {
					id, _ := strconv.Atoi(strings.TrimPrefix(line, "delivered:"))
					sawDuplicate = sawDuplicate || delivered[id]
				}
			}
			if !sawDuplicate {
				t.Fatal("did not exercise delivery-before-ack replay")
			}
		})
	}
}

func runHelper(t *testing.T, path, mode string) []string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(executable, "-test.run=^TestRecoveryProcessHelper$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), "BADGERBOX_RECOVERY_MODE="+mode, "BADGERBOX_RECOVERY_PATH="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	finished := false
	defer func() {
		if !finished {
			_ = cmd.Process.Kill()
			<-done
		}
	}()
	lines := make(chan string, 1024)
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
		done <- cmd.Wait()
	}()

	var output []string
	signaled := false
	timer := time.NewTimer(25 * time.Second)
	defer timer.Stop()
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				lines = nil
				continue
			}
			output = append(output, line)
			if line == "ready-to-stop" && !signaled {
				signaled = true
				sig := syscall.SIGTERM
				if mode == "crash" || mode == "ack-crash" {
					sig = syscall.SIGKILL
				}
				if err = cmd.Process.Signal(sig); err != nil {
					t.Fatal(err)
				}
			}
		case err = <-done:
			finished = true
			// Drain every line scanned before process exit.
			if lines != nil {
				for line := range lines {
					output = append(output, line)
				}
			}
			if mode == "crash" || mode == "ack-crash" {
				var exit *exec.ExitError
				if !signaled || !errors.As(err, &exit) {
					t.Fatalf("expected abrupt exit: %v %s %v", err, stderr.String(), output)
				}
			} else if err != nil {
				t.Fatalf("helper %s failed: %v %s %v", mode, err, stderr.String(), output)
			}
			return output
		case <-timer.C:
			_ = cmd.Process.Kill()
			<-done
			finished = true
			t.Fatalf("helper %s timed out: %s %v", mode, stderr.String(), output)
		}
	}
}
