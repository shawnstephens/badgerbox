package demo

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

type fakeValueLogGCDB struct {
	errs  []error
	calls int
}

func (db *fakeValueLogGCDB) RunValueLogGC(float64) error {
	db.calls++
	if len(db.errs) == 0 {
		return badger.ErrNoRewrite
	}
	err := db.errs[0]
	db.errs = db.errs[1:]
	return err
}

func TestRunValueLogGCPassContinuesUntilNoRewrite(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := NewLogger(&out, string(ColorNever))
	db := &fakeValueLogGCDB{
		errs: []error{nil, nil, badger.ErrNoRewrite},
	}

	runValueLogGCPass(db, DefaultBadgerGCDiscardRatio, logger)

	if db.calls != 3 {
		t.Fatalf("RunValueLogGC calls = %d, want 3", db.calls)
	}
	logged := out.String()
	if !strings.Contains(logged, "event=value_log_gc") {
		t.Fatalf("expected value_log_gc log, got %q", logged)
	}
	if !strings.Contains(logged, "rewrites=2") {
		t.Fatalf("expected rewrite count, got %q", logged)
	}
	if !strings.Contains(logged, fmt.Sprintf("discard_ratio=%.2f", DefaultBadgerGCDiscardRatio)) {
		t.Fatalf("expected discard ratio, got %q", logged)
	}
}

func TestRunValueLogGCPassLogsFailures(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := NewLogger(&out, string(ColorNever))
	db := &fakeValueLogGCDB{
		errs: []error{errors.New("disk busy")},
	}

	runValueLogGCPass(db, DefaultBadgerGCDiscardRatio, logger)

	logged := out.String()
	if !strings.Contains(logged, "event=value_log_gc_failed") {
		t.Fatalf("expected failure log, got %q", logged)
	}
	if !strings.Contains(logged, "disk busy") {
		t.Fatalf("expected error text, got %q", logged)
	}
}
