package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cli "github.com/urfave/cli/v3"
)

func runBenchmarkTest(t *testing.T, extra ...string) (benchmarkReport, error) {
	t.Helper()
	t.Setenv("BADGERBOX_DEMO_BROKERS", "")
	path := filepath.Join(t.TempDir(), "report.json")
	args := []string{"badgerbox-demo", "benchmark", "--messages", "160", "--payload-bytes", "1024", "--enqueue-parallelism", "2", "--processor-concurrency", "2", "--processor-claim-batch-size", "16", "--poll-interval", "2ms", "--retry-base-delay", "2ms", "--retry-max-delay", "10ms", "--sample-interval", "10ms", "--badger-sync-writes", "--badger-memtable-size", "8MiB", "--badger-block-cache-size", "4MiB", "--badger-value-log-file-size", "8MiB", "--db-path", t.TempDir(), "--output", path}
	args = append(args, extra...)
	err := newRootCommand().Run(context.Background(), args)
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatalf("read report: %v (run error: %v)", readErr, err)
	}
	var report benchmarkReport
	if decodeErr := json.Unmarshal(data, &report); decodeErr != nil {
		t.Fatal(decodeErr)
	}
	return report, err
}

func TestBenchmarkChecksConservationAndMetricsWithRetries(t *testing.T) {
	report, err := runBenchmarkTest(t, "--fail-every", "5")
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed || report.Accepted != 160 || report.UniqueDelivered != 160 || report.InvalidDeliveries != 0 || report.DuplicateDeliveries != 0 {
		t.Fatalf("conservation: %+v", report)
	}
	if report.PublishAttempts != 192 {
		t.Fatalf("expected exactly 32 first-attempt failures; attempts=%d", report.PublishAttempts)
	}
	if report.Queue.ReadyDepth+report.Queue.ProcessingDepth+report.Queue.DeadLetterDepth != 0 || report.Audit == nil || !report.Audit.Complete || report.Audit.LiveRows != 0 {
		t.Fatalf("drain not verified: queue=%+v audit=%+v", report.Queue, report.Audit)
	}
	if report.Metrics["badgerbox_enqueue_total"] != 160 || report.Metrics["badgerbox_process_attempt_total"] != 192 {
		t.Fatalf("metrics=%v", report.Metrics)
	}
	if report.EnqueueLatency.Count != 160 || report.DeliveryLatency.Count != 160 || report.DeliveryLatency.P99 <= 0 || report.DeliveryPerSecond <= 0 || report.Resources.Samples < 2 || report.Resources.PeakHeap == 0 || report.Resources.FinalDisk == 0 {
		t.Fatalf("missing benchmark measurements: %+v", report)
	}
}

func TestBenchmarkTimeoutWritesFailedReportAndRetainsQueue(t *testing.T) {
	report, err := runBenchmarkTest(t, "--timeout", "150ms", "--outage", "10s")
	if err == nil || report.Passed || !strings.Contains(report.Error, "deadline") {
		t.Fatalf("timeout err=%v report=%+v", err, report)
	}
	if report.Accepted == 0 || report.UniqueDelivered != 0 || report.Queue.ReadyDepth+report.Queue.ProcessingDepth != report.Accepted {
		t.Fatalf("failure conservation: %+v", report)
	}
	if report.Metrics["badgerbox_enqueue_total"] != float64(report.Accepted) {
		t.Fatalf("missing failure metrics: %v", report.Metrics)
	}
}

func TestBenchmarkVerifierDetectsCorruptionForeignAndDuplicates(t *testing.T) {
	now := time.Now()
	v := newBenchmarkVerifier(2, 128)
	first := benchmarkPayload(0, 128, now)
	v.receive(first, now.Add(time.Millisecond))
	v.receive(first, now.Add(2*time.Millisecond))
	corrupt := bytes.Clone(first)
	corrupt[100] ^= 1
	v.receive(corrupt, now)
	timestampCorrupt := bytes.Clone(first)
	timestampCorrupt[9] ^= 1
	v.receive(timestampCorrupt, now)
	v.receive(benchmarkPayload(2, 128, now), now)
	v.receive([]byte("short"), now)
	v.receive(benchmarkPayload(1, 128, now), now.Add(4*time.Millisecond))
	if v.unique != 2 || v.duplicates != 1 || v.invalid != 4 || v.delivery.count != 2 {
		t.Fatalf("verifier: %+v", v)
	}
}

func TestBenchmarkLatencyHistogramBounds(t *testing.T) {
	var h latencyHistogram
	for i := 1; i <= 1000; i++ {
		h.add(time.Duration(i) * time.Microsecond)
	}
	result := h.summary()
	for _, test := range []struct{ actual, want float64 }{{result.P50, .0005}, {result.P95, .00095}, {result.P99, .00099}} {
		if test.actual < test.want || test.actual > test.want*1.02 {
			t.Fatalf("quantile=%f want [%f,%f]", test.actual, test.want, test.want*1.02)
		}
	}
}

func TestBenchmarkValidationPrecedesDatabaseCreation(t *testing.T) {
	for _, args := range [][]string{{"--messages", "0"}, {"--payload-bytes", "19"}, {"--rate", "NaN"}, {"--rate", "1e-100"}, {"--brokers", ", ,"}, {"--timeout", "0s"}, {"--brokers", "localhost:9092", "--outage", "1s"}} {
		command := newRootCommand()
		if err := command.Run(context.Background(), append([]string{"badgerbox-demo", "benchmark"}, args...)); err == nil {
			t.Fatalf("accepted invalid flags %v", args)
		}
	}
}

func TestBenchmarkKafkaConsumerConservation(t *testing.T) {
	brokers := os.Getenv("BADGERBOX_BENCHMARK_TEST_BROKERS")
	if brokers == "" {
		t.Skip("set BADGERBOX_BENCHMARK_TEST_BROKERS to exercise a real Kafka broker")
	}
	report, err := runBenchmarkTest(t, "--brokers", brokers, "--messages", "100", "--payload-bytes", "65536")
	if err != nil {
		t.Fatal(err)
	}
	if report.Config.Transport != "kafka-consumer-verified" || report.Accepted != 100 || report.UniqueDelivered != 100 || report.DuplicateDeliveries != 0 || report.InvalidDeliveries != 0 {
		t.Fatalf("Kafka conservation: %+v", report)
	}
}

func TestBenchmarkMinimumPayloadIntegrity(t *testing.T) {
	v := newBenchmarkVerifier(1, 20)
	now := time.Now()
	payload := benchmarkPayload(0, 20, now)
	corrupted := bytes.Clone(payload)
	corrupted[8] ^= 1
	v.receive(corrupted, now)
	v.receive(payload, now)
	if v.unique != 1 || v.invalid != 1 {
		t.Fatalf("unique=%d invalid=%d", v.unique, v.invalid)
	}
}

func TestCommandsRejectUnsupportedCompactorsBeforeDatabaseCreation(t *testing.T) {
	t.Setenv("BADGERBOX_DEMO_BROKERS", "")
	for _, tc := range []struct{ subcommand, compactors string }{
		{"benchmark", "-1"},
		{"producer", "-1"},
		{"benchmark", "0"},
	} {
		t.Run(tc.subcommand+"/compactors="+tc.compactors, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "database")
			args := []string{"badgerbox-demo", tc.subcommand, "--badger-num-compactors=" + tc.compactors, "--db-path", path}
			if tc.subcommand == "producer" {
				args = append(args, "--logging-producer")
			}
			err := newRootCommand().Run(t.Context(), args)
			if err == nil || !strings.Contains(err.Error(), "badger-num-compactors") {
				t.Fatalf("invalid compactor error = %v", err)
			}
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid options created a database: %v", err)
			}
		})
	}
}

func TestBenchmarkReportRequiresVerificationAndSuccessfulCleanup(t *testing.T) {
	cleanupErr := errors.New("cleanup failed")
	panicValue := errors.New("injected panic")
	for _, tc := range []struct {
		name     string
		verified bool
		err      error
		panics   bool
		passed   bool
	}{
		{name: "verified", verified: true, passed: true},
		{name: "unverified"},
		{name: "cleanup error", verified: true, err: cleanupErr},
		{name: "startup panic", panics: true},
		{name: "cleanup panic", verified: true, panics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			cmd := &cli.Command{
				Writer: &output,
				Flags:  []cli.Flag{&cli.StringFlag{Name: "output", Value: "-"}},
				Action: func(_ context.Context, cmd *cli.Command) (resultErr error) {
					report := benchmarkReport{}
					verified := tc.verified
					defer finalizeBenchmarkReport(cmd, &report, &verified, &resultErr)
					defer func() {
						if tc.panics {
							panic(panicValue)
						}
					}()
					return tc.err
				},
			}
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				if err := cmd.Run(t.Context(), []string{"report-test"}); !errors.Is(err, tc.err) {
					t.Fatalf("returned error = %v, want %v", err, tc.err)
				}
			}()
			if tc.panics && recovered != panicValue || !tc.panics && recovered != nil {
				t.Fatalf("panic changed or swallowed: %v", recovered)
			}
			var report benchmarkReport
			if err := json.Unmarshal(output.Bytes(), &report); err != nil {
				t.Fatal(err)
			}
			if report.Passed != tc.passed {
				t.Fatalf("passed = %t, want %t", report.Passed, tc.passed)
			}
			if tc.panics && !strings.Contains(report.Error, panicValue.Error()) {
				t.Fatalf("report omitted panic: %q", report.Error)
			}
		})
	}
}
