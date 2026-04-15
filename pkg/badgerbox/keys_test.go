package badgerbox

import (
	"testing"
	"time"
)

func TestParseMessageKey(t *testing.T) {
	t.Parallel()

	keys := newKeyspace("parse")
	id := MessageID(42)

	got, err := parseMessageKey(keys.messagePrefix, keys.messageKey(id))
	if err != nil {
		t.Fatalf("parseMessageKey: %v", err)
	}
	if got != id {
		t.Fatalf("message id = %d, want %d", got, id)
	}

	if _, err := parseMessageKey(keys.messagePrefix, keys.messagePrefix); err == nil {
		t.Fatal("expected invalid message key length error")
	}

	otherKeys := newKeyspace("other")
	if _, err := parseMessageKey(keys.messagePrefix, otherKeys.messageKey(id)); err == nil {
		t.Fatal("expected invalid message key prefix error")
	}
}

func TestParseTimeAndIDKey(t *testing.T) {
	t.Parallel()

	keys := newKeyspace("parse")
	id := MessageID(7)
	at := time.Unix(100, 99).UTC()

	gotAt, gotID, err := parseTimeAndIDKey(keys.readyPrefix, keys.readyKey(at, id))
	if err != nil {
		t.Fatalf("parseTimeAndIDKey: %v", err)
	}
	if !gotAt.Equal(at) || gotID != id {
		t.Fatalf("parsed = (%v, %d), want (%v, %d)", gotAt, gotID, at, id)
	}

	invalidSeparator := append([]byte(nil), keys.readyKey(at, id)...)
	invalidSeparator[len(keys.readyPrefix)+8] = '.'
	if _, _, err := parseTimeAndIDKey(keys.readyPrefix, invalidSeparator); err == nil {
		t.Fatal("expected invalid indexed key separator error")
	}
}
