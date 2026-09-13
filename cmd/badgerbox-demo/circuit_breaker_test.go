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
			if c.Int("circuit-failure-threshold") != 3 || c.Duration("circuit-initial-cooldown") != 5*time.Second || c.Duration("circuit-max-cooldown") != time.Minute {
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
	cmd := newProducerCommand()
	cmd.Action = func(_ context.Context, c *cli.Command) error {
		if c.Bool("circuit-breaker") || c.Int("circuit-failure-threshold") != 7 || c.Duration("circuit-initial-cooldown") != 3*time.Second || c.Duration("circuit-max-cooldown") != 12*time.Second {
			t.Fatal("environment not applied")
		}
		return nil
	}
	if err := cmd.Run(context.Background(), []string{"producer"}); err != nil {
		t.Fatal(err)
	}
}
