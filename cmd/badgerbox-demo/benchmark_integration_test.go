//go:build integration

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/cmd/badgerbox-demo/internal/demo"
)

// This suite deliberately requires a working container runtime. A missing or
// unhealthy runtime fails the test instead of silently removing Kafka evidence.
func TestBenchmarkKafkaTestcontainersConservation(t *testing.T) {
	// Bound Kafka setup and benchmark work together, while reserving time for
	// the benchmark's own shutdown contexts and our container termination.
	deadline := time.Now().Add(3 * time.Minute)
	if testDeadline, ok := t.Deadline(); ok {
		workDeadline := testDeadline.Add(-90 * time.Second)
		if workDeadline.Before(deadline) {
			deadline = workDeadline
		}
	}
	if !time.Now().Before(deadline) {
		t.Fatal("test timeout must leave at least 90 seconds for integration cleanup")
	}
	ctx, cancel := context.WithDeadline(t.Context(), deadline)
	defer cancel()

	container, brokers, err := demo.StartKafka(ctx, demo.DefaultKafkaImage, demo.DefaultClusterID)
	if err != nil {
		t.Fatalf("start required Kafka broker: %v", err)
	}
	t.Cleanup(func() {
		// Cleanup remains usable when startup or the benchmark exhausts its
		// deadline. The enclosing go test timeout must allow this extra window.
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := container.Terminate(cleanupCtx); err != nil {
			t.Errorf("terminate Kafka broker: %v", err)
		}
	})

	remaining := time.Until(deadline)
	if remaining <= 0 {
		t.Fatal("Kafka startup exhausted the integration test deadline")
	}
	report, err := runBenchmarkTest(t,
		"--brokers", strings.Join(brokers, ","),
		"--messages", "100",
		"--payload-bytes", "65536",
		"--timeout", remaining.String(),
	)
	if err != nil {
		t.Fatalf("Kafka benchmark failed: %v; report error: %s", err, report.Error)
	}
	if !report.Passed || report.Config.Transport != "kafka-consumer-verified" ||
		report.Accepted != 100 || report.UniqueDelivered != 100 ||
		report.DuplicateDeliveries != 0 || report.InvalidDeliveries != 0 {
		t.Fatalf("Kafka conservation failed: passed=%t transport=%s accepted=%d unique=%d duplicates=%d invalid=%d",
			report.Passed, report.Config.Transport, report.Accepted, report.UniqueDelivered,
			report.DuplicateDeliveries, report.InvalidDeliveries)
	}
	if report.Queue.ReadyDepth+report.Queue.ProcessingDepth+report.Queue.DeadLetterDepth != 0 ||
		report.Audit == nil || !report.Audit.Complete || report.Audit.LiveRows != 0 ||
		report.Metrics["badgerbox_enqueue_total"] != 100 {
		t.Fatalf("Kafka benchmark did not prove settled delivery: queue=%+v audit=%+v metrics=%v",
			report.Queue, report.Audit, report.Metrics)
	}
}
