package main

import (
	"path/filepath"
	"testing"

	"github.com/shawnstephens/badgerbox/internal/demo"
)

func TestResolveKafkaTargetUsesStateFileWhenFlagsAreAbsent(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	err := demo.WriteState(stateFile, demo.State{
		Version: 1,
		Brokers: []string{"127.0.0.1:19093"},
		Topic:   "from-state",
	})
	if err != nil {
		t.Fatalf("WriteState() error = %v", err)
	}

	target, err := resolveKafkaTarget("", "", stateFile)
	if err != nil {
		t.Fatalf("resolveKafkaTarget() error = %v", err)
	}
	if len(target.Brokers) != 1 || target.Brokers[0] != "127.0.0.1:19093" {
		t.Fatalf("unexpected brokers: %#v", target.Brokers)
	}
	if target.Topic != "from-state" {
		t.Fatalf("unexpected topic %q", target.Topic)
	}
	if target.BrokersSource != "state-file" {
		t.Fatalf("unexpected brokers source %q", target.BrokersSource)
	}
	if target.TopicSource != "state-file" {
		t.Fatalf("unexpected topic source %q", target.TopicSource)
	}
}

func TestResolveKafkaTargetPrefersExplicitFlagsOverState(t *testing.T) {
	t.Parallel()

	stateFile := filepath.Join(t.TempDir(), "state.json")
	err := demo.WriteState(stateFile, demo.State{
		Version: 1,
		Brokers: []string{"127.0.0.1:19093"},
		Topic:   "from-state",
	})
	if err != nil {
		t.Fatalf("WriteState() error = %v", err)
	}

	target, err := resolveKafkaTarget("127.0.0.1:29093", "from-flags", stateFile)
	if err != nil {
		t.Fatalf("resolveKafkaTarget() error = %v", err)
	}
	if len(target.Brokers) != 1 || target.Brokers[0] != "127.0.0.1:29093" {
		t.Fatalf("unexpected brokers: %#v", target.Brokers)
	}
	if target.Topic != "from-flags" {
		t.Fatalf("unexpected topic %q", target.Topic)
	}
	if target.BrokersSource != "flags-env" {
		t.Fatalf("unexpected brokers source %q", target.BrokersSource)
	}
	if target.TopicSource != "flags-env" {
		t.Fatalf("unexpected topic source %q", target.TopicSource)
	}
}
