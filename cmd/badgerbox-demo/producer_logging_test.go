package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/shawnstephens/badgerbox/internal/demo"
	"github.com/shawnstephens/badgerbox/pkg/badgerbox"
	"github.com/shawnstephens/badgerbox/pkg/kafkaoutbox"
)

func TestLogProcessFailureRetryable(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := demo.NewLogger(&out, string(demo.ColorNever))
	now := time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)

	logProcessFailure(logger, now, badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		ID:          101,
		Attempt:     2,
		MaxAttempts: 10,
		Payload: kafkaoutbox.KafkaMessage{
			Key: []byte("demo-key"),
		},
		Destination: kafkaoutbox.KafkaDestination{Topic: "demo-topic"},
	}, errors.New("broker unavailable"), 1*time.Second, 5*time.Second)

	logs := out.String()
	if !strings.Contains(logs, "event=retry_scheduled") {
		t.Fatalf("expected retry_scheduled log, got %q", logs)
	}
	if !strings.Contains(logs, "attempt=2") || !strings.Contains(logs, "next_attempt=3") {
		t.Fatalf("expected attempt metadata, got %q", logs)
	}
	if !strings.Contains(logs, "delay=2s") {
		t.Fatalf("expected retry delay, got %q", logs)
	}
	if !strings.Contains(logs, "available_at=2026-04-09T12:00:02Z") {
		t.Fatalf("expected available_at timestamp, got %q", logs)
	}
	if strings.Contains(logs, "event=dlq_pending") {
		t.Fatalf("did not expect dlq_pending log, got %q", logs)
	}
}

func TestLogProcessFailureTerminal(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := demo.NewLogger(&out, string(demo.ColorNever))

	logProcessFailure(logger, time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC), badgerbox.Message[kafkaoutbox.KafkaMessage, kafkaoutbox.KafkaDestination]{
		ID:          202,
		Attempt:     5,
		MaxAttempts: 5,
		Payload: kafkaoutbox.KafkaMessage{
			Key: []byte("demo-key"),
		},
		Destination: kafkaoutbox.KafkaDestination{Topic: "demo-topic"},
	}, errors.New("broker unavailable"), 1*time.Second, 5*time.Second)

	logs := out.String()
	if !strings.Contains(logs, "event=dlq_pending") {
		t.Fatalf("expected dlq_pending log, got %q", logs)
	}
	if strings.Contains(logs, "event=retry_scheduled") {
		t.Fatalf("did not expect retry_scheduled log, got %q", logs)
	}
	if !strings.Contains(logs, "attempt=5") || !strings.Contains(logs, "max_attempts=5") {
		t.Fatalf("expected attempt metadata, got %q", logs)
	}
}
