package badgerbox

import (
	"errors"
	"testing"
)

func TestPermanentErrorHelpers(t *testing.T) {
	t.Parallel()

	root := errors.New("boom")
	if got := Permanent(nil); got != nil {
		t.Fatalf("Permanent(nil) = %v, want nil", got)
	}

	err := Permanent(root)
	if !IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = false, want true", err)
	}
	if IsPermanent(root) {
		t.Fatalf("IsPermanent(%v) = true, want false", root)
	}
	if !errors.Is(err, root) {
		t.Fatalf("errors.Is(%v, %v) = false, want true", err, root)
	}
	if errors.Unwrap(err) != root {
		t.Fatalf("errors.Unwrap(%v) = %v, want %v", err, errors.Unwrap(err), root)
	}

	var target permanentError
	if !errors.As(err, &target) {
		t.Fatalf("errors.As(%v, permanentError) = false, want true", err)
	}
	if target.Unwrap() != root {
		t.Fatalf("target.Unwrap() = %v, want %v", target.Unwrap(), root)
	}
}
