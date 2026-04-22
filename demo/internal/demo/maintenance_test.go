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

type fakeStartupCompactionDB struct {
	flattenErr     error
	valueLogGCErrs []error
	sizes          [][2]int64

	flattenCalls   int
	flattenWorkers []int
	gcCalls        int
	gcRatios       []float64
	callOrder      []string
}

func (db *fakeStartupCompactionDB) Flatten(workers int) error {
	db.flattenCalls++
	db.flattenWorkers = append(db.flattenWorkers, workers)
	db.callOrder = append(db.callOrder, "flatten")
	return db.flattenErr
}

func (db *fakeStartupCompactionDB) RunValueLogGC(discardRatio float64) error {
	db.gcCalls++
	db.gcRatios = append(db.gcRatios, discardRatio)
	db.callOrder = append(db.callOrder, "gc")
	if len(db.valueLogGCErrs) == 0 {
		return badger.ErrNoRewrite
	}
	err := db.valueLogGCErrs[0]
	db.valueLogGCErrs = db.valueLogGCErrs[1:]
	return err
}

func (db *fakeStartupCompactionDB) Size() (int64, int64) {
	db.callOrder = append(db.callOrder, "size")
	if len(db.sizes) == 0 {
		return 0, 0
	}
	size := db.sizes[0]
	if len(db.sizes) > 1 {
		db.sizes = db.sizes[1:]
	}
	return size[0], size[1]
}

func TestRunStartupCompactionFlattensThenLoopsValueLogGC(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	logger := NewLogger(&out, string(ColorNever))
	db := &fakeStartupCompactionDB{
		sizes:          [][2]int64{{10 << 20, 20 << 20}, {3 << 20, 5 << 20}},
		valueLogGCErrs: []error{nil, nil, badger.ErrNoRewrite},
	}

	if err := RunStartupCompaction(db, "/tmp/demo-badger", 4, logger); err != nil {
		t.Fatalf("RunStartupCompaction() error = %v", err)
	}

	if db.flattenCalls != 1 {
		t.Fatalf("Flatten calls = %d, want 1", db.flattenCalls)
	}
	if len(db.flattenWorkers) != 1 || db.flattenWorkers[0] != 4 {
		t.Fatalf("Flatten workers = %#v, want [4]", db.flattenWorkers)
	}
	if got := strings.Join(db.callOrder, ","); got != "size,flatten,gc,gc,gc,size" {
		t.Fatalf("call order = %q, want %q", got, "size,flatten,gc,gc,gc,size")
	}
	if db.gcCalls != 3 {
		t.Fatalf("RunValueLogGC calls = %d, want 3", db.gcCalls)
	}
	for i, ratio := range db.gcRatios {
		if ratio != startupBadgerGCDiscardRatio {
			t.Fatalf("RunValueLogGC ratio[%d] = %v, want %v", i, ratio, startupBadgerGCDiscardRatio)
		}
	}

	logged := out.String()
	if !strings.Contains(logged, "event=startup_compaction_start") {
		t.Fatalf("expected startup_compaction_start log, got %q", logged)
	}
	if !strings.Contains(logged, "event=startup_compaction_complete") {
		t.Fatalf("expected startup_compaction_complete log, got %q", logged)
	}
	if !strings.Contains(logged, "rewrites=2") {
		t.Fatalf("expected rewrite count, got %q", logged)
	}
}

func TestRunStartupCompactionUsesAtLeastOneFlattenWorker(t *testing.T) {
	t.Parallel()

	db := &fakeStartupCompactionDB{}

	if err := RunStartupCompaction(db, "/tmp/demo-badger", 0, nil); err != nil {
		t.Fatalf("RunStartupCompaction() error = %v", err)
	}
	if len(db.flattenWorkers) != 1 || db.flattenWorkers[0] != 1 {
		t.Fatalf("Flatten workers = %#v, want [1]", db.flattenWorkers)
	}
}

func TestRunStartupCompactionReturnsFlattenError(t *testing.T) {
	t.Parallel()

	expected := errors.New("flatten failed")
	var out bytes.Buffer
	logger := NewLogger(&out, string(ColorNever))
	db := &fakeStartupCompactionDB{
		flattenErr: expected,
	}

	err := RunStartupCompaction(db, "/tmp/demo-badger", 2, logger)
	if !errors.Is(err, expected) {
		t.Fatalf("RunStartupCompaction() error = %v, want %v", err, expected)
	}
	if db.gcCalls != 0 {
		t.Fatalf("RunValueLogGC calls = %d, want 0", db.gcCalls)
	}
	if !strings.Contains(out.String(), "stage=flatten") {
		t.Fatalf("expected flatten failure log, got %q", out.String())
	}
}

func TestRunStartupCompactionReturnsValueLogGCError(t *testing.T) {
	t.Parallel()

	expected := badger.ErrRejected
	var out bytes.Buffer
	logger := NewLogger(&out, string(ColorNever))
	db := &fakeStartupCompactionDB{
		valueLogGCErrs: []error{expected},
	}

	err := RunStartupCompaction(db, "/tmp/demo-badger", 2, logger)
	if !errors.Is(err, expected) {
		t.Fatalf("RunStartupCompaction() error = %v, want %v", err, expected)
	}
	if db.flattenCalls != 1 {
		t.Fatalf("Flatten calls = %d, want 1", db.flattenCalls)
	}
	if db.gcCalls != 1 {
		t.Fatalf("RunValueLogGC calls = %d, want 1", db.gcCalls)
	}
	if !strings.Contains(out.String(), "stage=value_log_gc") {
		t.Fatalf("expected value_log_gc failure log, got %q", out.String())
	}
}
