package main

import (
	"context"
	"testing"
	"time"

	"github.com/urfave/cli/v3"
)

func TestProducerCircuitFlags(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		cmd := newProducerCommand()
		called := false
		cmd.Action = func(_ context.Context, c *cli.Command) error {
			called = true
			if c.Bool("circuit-breaker") == disabled {
				t.Fatal("enabled default/override")
			}
			if c.Int("circuit-failure-threshold") != 3 || c.Duration("circuit-initial-cooldown") != 5*time.Second || c.Duration("circuit-max-cooldown") != 0 || c.Bool("circuit-disable-jitter") || c.Duration("circuit-message-error-cooldown") != 0 || c.Duration("record-delivery-timeout") != 0 {
				t.Fatal("unexpected breaker defaults")
			}
			return nil
		}
		args := []string{"producer"}
		if disabled {
			args = append(args, "--circuit-breaker=false")
		}
		if err := cmd.Run(context.Background(), args); err != nil {
			t.Fatal(err)
		}
		if !called {
			t.Fatal("action not called")
		}
	}
}

func TestProducerCircuitEnvironment(t *testing.T) {
	t.Setenv("BADGERBOX_DEMO_CIRCUIT_BREAKER", "false")
	t.Setenv("BADGERBOX_DEMO_CIRCUIT_FAILURE_THRESHOLD", "7")
	t.Setenv("BADGERBOX_DEMO_CIRCUIT_INITIAL_COOLDOWN", "3s")
	t.Setenv("BADGERBOX_DEMO_CIRCUIT_MAX_COOLDOWN", "12s")
	t.Setenv("BADGERBOX_DEMO_CIRCUIT_DISABLE_JITTER", "true")
	t.Setenv("BADGERBOX_DEMO_CIRCUIT_MESSAGE_ERROR_COOLDOWN", "750ms")
	t.Setenv("BADGERBOX_DEMO_RECORD_DELIVERY_TIMEOUT", "1500ms")
	cmd := newProducerCommand()
	cmd.Action = func(_ context.Context, c *cli.Command) error {
		if !c.Bool("circuit-disable-jitter") || c.Duration("circuit-message-error-cooldown") != 750*time.Millisecond || c.Duration("record-delivery-timeout") != 1500*time.Millisecond {
			t.Fatal("recovery environment not applied")
		}
		if c.Bool("circuit-breaker") || c.Int("circuit-failure-threshold") != 7 || c.Duration("circuit-initial-cooldown") != 3*time.Second || c.Duration("circuit-max-cooldown") != 12*time.Second {
			t.Fatal("environment not applied")
		}
		return nil
	}
	if err := cmd.Run(context.Background(), []string{"producer"}); err != nil {
		t.Fatal(err)
	}
}

func TestResolveRecordDeliveryTimeout(t *testing.T) {
	for _, tc := range []struct{ record, publish, want time.Duration }{
		{0, 2 * time.Second, time.Second}, {time.Second, 3 * time.Second, time.Second},
	} {
		got, err := resolveRecordDeliveryTimeout(tc.record, tc.publish)
		if err != nil || got != tc.want {
			t.Fatalf("%+v: got=%v err=%v", tc, got, err)
		}
	}
	for _, tc := range []struct{ record, publish time.Duration }{
		{-1, time.Second}, {0, 0}, {0, time.Nanosecond}, {time.Second, time.Second}, {2 * time.Second, time.Second}, {0, 1500 * time.Millisecond}, {500 * time.Millisecond, 2 * time.Second},
	} {
		if _, err := resolveRecordDeliveryTimeout(tc.record, tc.publish); err == nil {
			t.Fatalf("accepted %+v", tc)
		}
	}
}
