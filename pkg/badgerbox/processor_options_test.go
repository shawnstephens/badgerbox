package badgerbox

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestProcessorConstructorsRejectInvalidTuning(t *testing.T) {
	_, store, cleanup := openTestStore[string, string](t, "invalid-tuning", Serde[string, string]{})
	defer cleanup()
	for _, tc := range []struct {
		field   string
		options ProcessorOptions
	}{
		{"Concurrency", ProcessorOptions{Concurrency: -1}},
		{"PollInterval", ProcessorOptions{PollInterval: -time.Second}},
		{"LeaseDuration", ProcessorOptions{LeaseDuration: -time.Second}},
		{"RetryBaseDelay", ProcessorOptions{RetryBaseDelay: -time.Second}},
		{"RetryMaxDelay", ProcessorOptions{RetryMaxDelay: -time.Second}},
		{"MaxAttempts", ProcessorOptions{MaxAttempts: -1}},
		{"RequeuePageSize", ProcessorOptions{RequeuePageSize: -1}},
		{"SettlementTimeout", ProcessorOptions{SettlementTimeout: -time.Second}},
		{"RetryMaxDelay", ProcessorOptions{RetryBaseDelay: time.Minute, RetryMaxDelay: time.Second}},
		{"RetryMaxDelay", ProcessorOptions{RetryMaxDelay: time.Millisecond}},
	} {
		t.Run(tc.field, func(t *testing.T) {
			_, err := NewProcessor(store, func(context.Context, Message[string, string]) error { return nil }, tc.options)
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("single constructor error=%v, want invalid %s", err, tc.field)
			}
			_, err = NewBatchProcessor(store, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, BatchProcessorOptions{ProcessorOptions: tc.options})
			if err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("batch constructor error=%v, want invalid %s", err, tc.field)
			}
		})
	}
	_, err := NewBatchProcessor(store, func(context.Context, []Message[string, string], chan<- BatchProcessResult) error { return nil }, BatchProcessorOptions{ClaimBatchSize: -1})
	if err == nil || !strings.Contains(err.Error(), "ClaimBatchSize") {
		t.Fatalf("batch size error=%v", err)
	}
	for _, options := range []ProcessorOptions{{}, {RetryBaseDelay: 2 * time.Hour}, {RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Millisecond}} {
		if _, err := NewProcessor(store, func(context.Context, Message[string, string]) error { return nil }, options); err != nil {
			t.Fatalf("valid defaults rejected: options=%+v err=%v", options, err)
		}
	}
}
