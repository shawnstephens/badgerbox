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

func TestQueueStateKeys(t *testing.T) {
	t.Parallel()

	keys := newKeyspace("queue-state")
	id := MessageID(11)
	createdAt := time.Unix(200, 33).UTC()

	if gotAt, gotID, err := parseTimeAndIDKey(keys.readyCreatedPrefix, keys.readyCreatedKey(createdAt, id)); err != nil {
		t.Fatalf("parse ready created key: %v", err)
	} else if !gotAt.Equal(createdAt) || gotID != id {
		t.Fatalf("ready created key parsed as (%v, %d), want (%v, %d)", gotAt, gotID, createdAt, id)
	}

	if gotAt, gotID, err := parseTimeAndIDKey(keys.processingCreatedPrefix, keys.processingCreatedKey(createdAt, id)); err != nil {
		t.Fatalf("parse processing created key: %v", err)
	} else if !gotAt.Equal(createdAt) || gotID != id {
		t.Fatalf("processing created key parsed as (%v, %d), want (%v, %d)", gotAt, gotID, createdAt, id)
	}

	if got := keys.readyCountShardKey(7); len(got) != len(keys.readyCountPrefix)+1 || got[len(got)-1] != 7 {
		t.Fatalf("ready count shard key = %v, want shard suffix 7", got)
	}
	if got := keys.processingCountShardKey(8); len(got) != len(keys.processingCountPrefix)+1 || got[len(got)-1] != 8 {
		t.Fatalf("processing count shard key = %v, want shard suffix 8", got)
	}
	if got := keys.deadLetterCountShardKey(9); len(got) != len(keys.deadLetterCountPrefix)+1 || got[len(got)-1] != 9 {
		t.Fatalf("dead-letter count shard key = %v, want shard suffix 9", got)
	}
}
