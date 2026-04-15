package demo

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type State struct {
	Version     int       `json:"version"`
	Brokers     []string  `json:"brokers"`
	Topic       string    `json:"topic"`
	KafkaImage  string    `json:"kafka_image"`
	StartedAt   time.Time `json:"started_at"`
	ContainerID string    `json:"container_id"`
}

func (s State) Validate() error {
	if len(s.Brokers) == 0 {
		return errors.New("demo state missing brokers")
	}
	if s.Topic == "" {
		return errors.New("demo state missing topic")
	}
	return nil
}

func WriteState(path string, state State) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("create state directory: %w", err)
	}

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal state: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write state file: %w", err)
	}
	return nil
}

func ReadState(path string) (State, error) {
	var state State

	data, err := os.ReadFile(path)
	if err != nil {
		return state, fmt.Errorf("read state file: %w", err)
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, fmt.Errorf("decode state file: %w", err)
	}
	if err := state.Validate(); err != nil {
		return state, err
	}
	return state, nil
}

func RemoveState(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove state file: %w", err)
	}
	return nil
}
