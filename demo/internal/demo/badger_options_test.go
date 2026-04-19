package demo

import (
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
)

func TestBuildBadgerOptionsDefaultsMatchCurrentBehavior(t *testing.T) {
	t.Parallel()

	path := t.TempDir()

	got, err := BuildBadgerOptions(path, BadgerOptionsOverrides{})
	if err != nil {
		t.Fatalf("BuildBadgerOptions() error = %v", err)
	}

	want := DefaultBadgerOptions(path)

	assertBadgerOptionsEqual(t, got, want)
}

func TestBuildBadgerOptionsAppliesEachOverride(t *testing.T) {
	t.Parallel()

	path := t.TempDir()

	tests := []struct {
		name      string
		overrides BadgerOptionsOverrides
		check     func(t *testing.T, got badger.Options)
	}{
		{
			name: "memtable size",
			overrides: BadgerOptionsOverrides{
				MemTableSizeSet: true,
				MemTableSize:    "32MiB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.MemTableSize != 32<<20 {
					t.Fatalf("MemTableSize = %d, want %d", got.MemTableSize, 32<<20)
				}
			},
		},
		{
			name: "num memtables",
			overrides: BadgerOptionsOverrides{
				NumMemtablesSet: true,
				NumMemtables:    3,
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.NumMemtables != 3 {
					t.Fatalf("NumMemtables = %d, want 3", got.NumMemtables)
				}
			},
		},
		{
			name: "num level zero tables",
			overrides: BadgerOptionsOverrides{
				NumLevelZeroTablesSet: true,
				NumLevelZeroTables:    4,
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.NumLevelZeroTables != 4 {
					t.Fatalf("NumLevelZeroTables = %d, want 4", got.NumLevelZeroTables)
				}
			},
		},
		{
			name: "num level zero tables stall",
			overrides: BadgerOptionsOverrides{
				NumLevelZeroTablesStallSet: true,
				NumLevelZeroTablesStall:    20,
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.NumLevelZeroTablesStall != 20 {
					t.Fatalf("NumLevelZeroTablesStall = %d, want 20", got.NumLevelZeroTablesStall)
				}
			},
		},
		{
			name: "num compactors",
			overrides: BadgerOptionsOverrides{
				NumCompactorsSet: true,
				NumCompactors:    2,
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.NumCompactors != 2 {
					t.Fatalf("NumCompactors = %d, want 2", got.NumCompactors)
				}
			},
		},
		{
			name: "base table size",
			overrides: BadgerOptionsOverrides{
				BaseTableSizeSet: true,
				BaseTableSize:    "4MiB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.BaseTableSize != 4<<20 {
					t.Fatalf("BaseTableSize = %d, want %d", got.BaseTableSize, 4<<20)
				}
			},
		},
		{
			name: "value log file size",
			overrides: BadgerOptionsOverrides{
				ValueLogFileSizeSet: true,
				ValueLogFileSize:    "256MiB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.ValueLogFileSize != 256<<20 {
					t.Fatalf("ValueLogFileSize = %d, want %d", got.ValueLogFileSize, 256<<20)
				}
			},
		},
		{
			name: "block cache size",
			overrides: BadgerOptionsOverrides{
				BlockCacheSizeSet: true,
				BlockCacheSize:    "64MiB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.BlockCacheSize != 64<<20 {
					t.Fatalf("BlockCacheSize = %d, want %d", got.BlockCacheSize, 64<<20)
				}
			},
		},
		{
			name: "index cache size",
			overrides: BadgerOptionsOverrides{
				IndexCacheSizeSet: true,
				IndexCacheSize:    "128MiB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.IndexCacheSize != 128<<20 {
					t.Fatalf("IndexCacheSize = %d, want %d", got.IndexCacheSize, 128<<20)
				}
			},
		},
		{
			name: "value threshold",
			overrides: BadgerOptionsOverrides{
				ValueThresholdSet: true,
				ValueThreshold:    "64KiB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.ValueThreshold != 64<<10 {
					t.Fatalf("ValueThreshold = %d, want %d", got.ValueThreshold, 64<<10)
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := BuildBadgerOptions(path, tc.overrides)
			if err != nil {
				t.Fatalf("BuildBadgerOptions() error = %v", err)
			}

			tc.check(t, got)
		})
	}
}

func TestBuildBadgerOptionsParsesSizeFormats(t *testing.T) {
	t.Parallel()

	path := t.TempDir()

	tests := []struct {
		name      string
		overrides BadgerOptionsOverrides
		check     func(t *testing.T, got badger.Options)
	}{
		{
			name: "binary units",
			overrides: BadgerOptionsOverrides{
				MemTableSizeSet: true,
				MemTableSize:    "64MiB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.MemTableSize != 64<<20 {
					t.Fatalf("MemTableSize = %d, want %d", got.MemTableSize, 64<<20)
				}
			},
		},
		{
			name: "decimal units",
			overrides: BadgerOptionsOverrides{
				BlockCacheSizeSet: true,
				BlockCacheSize:    "128MB",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.BlockCacheSize != 128000000 {
					t.Fatalf("BlockCacheSize = %d, want %d", got.BlockCacheSize, 128000000)
				}
			},
		},
		{
			name: "zero value",
			overrides: BadgerOptionsOverrides{
				IndexCacheSizeSet: true,
				IndexCacheSize:    "0",
			},
			check: func(t *testing.T, got badger.Options) {
				t.Helper()
				if got.IndexCacheSize != 0 {
					t.Fatalf("IndexCacheSize = %d, want 0", got.IndexCacheSize)
				}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := BuildBadgerOptions(path, tc.overrides)
			if err != nil {
				t.Fatalf("BuildBadgerOptions() error = %v", err)
			}

			tc.check(t, got)
		})
	}
}

func TestBuildBadgerOptionsRejectsInvalidOverrides(t *testing.T) {
	t.Parallel()

	path := t.TempDir()

	tests := []struct {
		name      string
		overrides BadgerOptionsOverrides
		wantErr   string
	}{
		{
			name: "invalid size string",
			overrides: BadgerOptionsOverrides{
				MemTableSizeSet: true,
				MemTableSize:    "bogus",
			},
			wantErr: "badger-memtable-size",
		},
		{
			name: "one compactor",
			overrides: BadgerOptionsOverrides{
				NumCompactorsSet: true,
				NumCompactors:    1,
			},
			wantErr: "badger-num-compactors",
		},
		{
			name: "stall threshold not greater than level zero tables",
			overrides: BadgerOptionsOverrides{
				NumLevelZeroTablesSet:      true,
				NumLevelZeroTables:         3,
				NumLevelZeroTablesStallSet: true,
				NumLevelZeroTablesStall:    3,
			},
			wantErr: "badger-num-level-zero-tables-stall",
		},
		{
			name: "value log file size too small",
			overrides: BadgerOptionsOverrides{
				ValueLogFileSizeSet: true,
				ValueLogFileSize:    "512KiB",
			},
			wantErr: "badger-value-log-file-size",
		},
		{
			name: "value log file size too large",
			overrides: BadgerOptionsOverrides{
				ValueLogFileSizeSet: true,
				ValueLogFileSize:    "2GiB",
			},
			wantErr: "badger-value-log-file-size",
		},
		{
			name: "value threshold above badger maximum",
			overrides: BadgerOptionsOverrides{
				ValueThresholdSet: true,
				ValueThreshold:    "2MiB",
			},
			wantErr: "badger-value-threshold",
		},
		{
			name: "value threshold above memtable budget",
			overrides: BadgerOptionsOverrides{
				MemTableSizeSet:   true,
				MemTableSize:      "4MiB",
				ValueThresholdSet: true,
				ValueThreshold:    "700KiB",
			},
			wantErr: "15%",
		},
		{
			name: "zero block cache with compression enabled",
			overrides: BadgerOptionsOverrides{
				BlockCacheSizeSet: true,
				BlockCacheSize:    "0",
			},
			wantErr: "badger-block-cache-size",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := BuildBadgerOptions(path, tc.overrides)
			if err == nil {
				t.Fatal("BuildBadgerOptions() error = nil, want non-nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("BuildBadgerOptions() error = %q, want substring %q", err, tc.wantErr)
			}
		})
	}
}

func assertBadgerOptionsEqual(t *testing.T, got, want badger.Options) {
	t.Helper()

	if got.Dir != want.Dir {
		t.Fatalf("Dir = %q, want %q", got.Dir, want.Dir)
	}
	if got.ValueDir != want.ValueDir {
		t.Fatalf("ValueDir = %q, want %q", got.ValueDir, want.ValueDir)
	}
	if got.MemTableSize != want.MemTableSize {
		t.Fatalf("MemTableSize = %d, want %d", got.MemTableSize, want.MemTableSize)
	}
	if got.BaseTableSize != want.BaseTableSize {
		t.Fatalf("BaseTableSize = %d, want %d", got.BaseTableSize, want.BaseTableSize)
	}
	if got.NumMemtables != want.NumMemtables {
		t.Fatalf("NumMemtables = %d, want %d", got.NumMemtables, want.NumMemtables)
	}
	if got.NumLevelZeroTables != want.NumLevelZeroTables {
		t.Fatalf("NumLevelZeroTables = %d, want %d", got.NumLevelZeroTables, want.NumLevelZeroTables)
	}
	if got.NumLevelZeroTablesStall != want.NumLevelZeroTablesStall {
		t.Fatalf("NumLevelZeroTablesStall = %d, want %d", got.NumLevelZeroTablesStall, want.NumLevelZeroTablesStall)
	}
	if got.NumCompactors != want.NumCompactors {
		t.Fatalf("NumCompactors = %d, want %d", got.NumCompactors, want.NumCompactors)
	}
	if got.ValueLogFileSize != want.ValueLogFileSize {
		t.Fatalf("ValueLogFileSize = %d, want %d", got.ValueLogFileSize, want.ValueLogFileSize)
	}
	if got.BlockCacheSize != want.BlockCacheSize {
		t.Fatalf("BlockCacheSize = %d, want %d", got.BlockCacheSize, want.BlockCacheSize)
	}
	if got.IndexCacheSize != want.IndexCacheSize {
		t.Fatalf("IndexCacheSize = %d, want %d", got.IndexCacheSize, want.IndexCacheSize)
	}
	if got.ValueThreshold != want.ValueThreshold {
		t.Fatalf("ValueThreshold = %d, want %d", got.ValueThreshold, want.ValueThreshold)
	}
	if got.MetricsEnabled != want.MetricsEnabled {
		t.Fatalf("MetricsEnabled = %t, want %t", got.MetricsEnabled, want.MetricsEnabled)
	}
	if got.Logger != want.Logger {
		t.Fatalf("Logger = %#v, want %#v", got.Logger, want.Logger)
	}
}
