package main

import (
	"path/filepath"
	"testing"

	"github.com/shawnstephens/badgerbox/demo/internal/demo"
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

func TestResolveProducerTargetLoggingModeTopicFallback(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "state.json")
	err := demo.WriteState(stateFile, demo.State{
		Version: 1,
		Topic:   "from-state",
	})
	if err != nil {
		t.Fatalf("WriteState() error = %v", err)
	}

	testCases := []struct {
		name              string
		topicValue        string
		stateFile         string
		wantTopic         string
		wantTopicSource   string
		wantBrokersSource string
	}{
		{
			name:              "explicit topic wins",
			topicValue:        "from-flags",
			stateFile:         stateFile,
			wantTopic:         "from-flags",
			wantTopicSource:   "flags-env",
			wantBrokersSource: "disabled",
		},
		{
			name:              "state topic used when flag absent",
			stateFile:         stateFile,
			wantTopic:         "from-state",
			wantTopicSource:   "state-file",
			wantBrokersSource: "disabled",
		},
		{
			name:              "default topic used when state missing",
			stateFile:         filepath.Join(tempDir, "missing.json"),
			wantTopic:         demo.DefaultTopic,
			wantTopicSource:   "default",
			wantBrokersSource: "disabled",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, err := resolveProducerTarget("", tc.topicValue, tc.stateFile, true)
			if err != nil {
				t.Fatalf("resolveProducerTarget() error = %v", err)
			}
			if len(target.Brokers) != 0 {
				t.Fatalf("expected no brokers, got %#v", target.Brokers)
			}
			if target.BrokersSource != tc.wantBrokersSource {
				t.Fatalf("unexpected brokers source %q", target.BrokersSource)
			}
			if target.Topic != tc.wantTopic {
				t.Fatalf("unexpected topic %q", target.Topic)
			}
			if target.TopicSource != tc.wantTopicSource {
				t.Fatalf("unexpected topic source %q", target.TopicSource)
			}
		})
	}
}

func TestNewProducerCommandIncludesLoggingProducerFlag(t *testing.T) {
	t.Parallel()

	cmd := newProducerCommand()
	for _, flag := range cmd.Flags {
		for _, name := range flag.Names() {
			if name == "logging-producer" {
				return
			}
		}
	}

	t.Fatal("producer flag \"logging-producer\" not found")
}

func TestNewProducerCommandIncludesBadgerMemoryFlags(t *testing.T) {
	t.Parallel()

	cmd := newProducerCommand()
	flagNames := make(map[string]struct{})
	for _, flag := range cmd.Flags {
		for _, name := range flag.Names() {
			flagNames[name] = struct{}{}
		}
	}

	wantFlags := []string{
		"badger-sync-writes",
		"badger-compact-on-startup",
		"badger-memtable-size",
		"badger-num-memtables",
		"badger-num-level-zero-tables",
		"badger-num-level-zero-tables-stall",
		"badger-num-compactors",
		"badger-base-table-size",
		"badger-value-log-file-size",
		"badger-block-cache-size",
		"badger-index-cache-size",
		"badger-value-threshold",
		"processor-claim-batch-size",
	}

	for _, name := range wantFlags {
		if _, ok := flagNames[name]; !ok {
			t.Fatalf("producer flag %q not found", name)
		}
	}
}
