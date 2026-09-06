package demo

import (
	"testing"
	"time"
)

func TestRetryDelayUsesBaseForFirstAttempt(t *testing.T) {
	t.Parallel()

	got := RetryDelay(1*time.Second, 5*time.Second, 1)
	if got != 1*time.Second {
		t.Fatalf("RetryDelay() = %v, want %v", got, 1*time.Second)
	}
}

func TestRetryDelayGrowsExponentially(t *testing.T) {
	t.Parallel()

	got := RetryDelay(1*time.Second, 30*time.Second, 4)
	if got != 8*time.Second {
		t.Fatalf("RetryDelay() = %v, want %v", got, 8*time.Second)
	}
}

func TestRetryDelayCapsAtMax(t *testing.T) {
	t.Parallel()

	got := RetryDelay(1*time.Second, 5*time.Second, 10)
	if got != 5*time.Second {
		t.Fatalf("RetryDelay() = %v, want %v", got, 5*time.Second)
	}
}
