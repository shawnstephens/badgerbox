package badgerbox

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSystemRuntimeHelpers(t *testing.T) {
	t.Parallel()

	runtime := SystemRuntime{}
	if now := runtime.Now(); now.IsZero() {
		t.Fatal("SystemRuntime.Now returned zero time")
	}

	if err := runtime.Sleep(nil, 0); err != nil {
		t.Fatalf("Sleep(nil, 0) = %v, want nil", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Sleep(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("Sleep(canceled) = %v, want %v", err, context.Canceled)
	}

	token, err := runtime.NewLeaseToken()
	if err != nil {
		t.Fatalf("NewLeaseToken: %v", err)
	}
	if len(token) != 32 {
		t.Fatalf("lease token len = %d, want 32", len(token))
	}
}
