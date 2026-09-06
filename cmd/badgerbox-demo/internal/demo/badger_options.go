package demo

import (
	"fmt"
	"math"
	"strings"

	"github.com/dgraph-io/badger/v4"
	badgeroptions "github.com/dgraph-io/badger/v4/options"
	humanize "github.com/dustin/go-humanize"
)

const (
	badgerMinValueLogFileSize = 1 << 20
	badgerMaxValueLogFileSize = 2 << 30
	badgerMaxValueThreshold   = 1 << 20
)

type BadgerOptionsOverrides struct {
	SyncWritesSet bool
	SyncWrites    bool

	MemTableSizeSet bool
	MemTableSize    string

	NumMemtablesSet bool
	NumMemtables    int

	NumLevelZeroTablesSet bool
	NumLevelZeroTables    int

	NumLevelZeroTablesStallSet bool
	NumLevelZeroTablesStall    int

	NumCompactorsSet bool
	NumCompactors    int

	BaseTableSizeSet bool
	BaseTableSize    string

	ValueLogFileSizeSet bool
	ValueLogFileSize    string

	BlockCacheSizeSet bool
	BlockCacheSize    string

	IndexCacheSizeSet bool
	IndexCacheSize    string

	ValueThresholdSet bool
	ValueThreshold    string
}

func DefaultBadgerOptions(path string) badger.Options {
	return badger.DefaultOptions(path).WithLogger(nil).WithMetricsEnabled(true)
}

func FormatBytes(size int64) string {
	if size == 0 {
		return "0"
	}
	return strings.ReplaceAll(humanize.IBytes(uint64(size)), " ", "")
}

func BuildBadgerOptions(path string, overrides BadgerOptionsOverrides) (badger.Options, error) {
	opts := DefaultBadgerOptions(path)

	if overrides.SyncWritesSet {
		opts = opts.WithSyncWrites(overrides.SyncWrites)
	}

	if overrides.MemTableSizeSet {
		size, err := parseBadgerSizeFlag("badger-memtable-size", overrides.MemTableSize)
		if err != nil {
			return badger.Options{}, err
		}
		opts = opts.WithMemTableSize(size)
	}

	if overrides.NumMemtablesSet {
		opts = opts.WithNumMemtables(overrides.NumMemtables)
	}

	if overrides.NumLevelZeroTablesSet {
		opts = opts.WithNumLevelZeroTables(overrides.NumLevelZeroTables)
	}

	if overrides.NumLevelZeroTablesStallSet {
		opts = opts.WithNumLevelZeroTablesStall(overrides.NumLevelZeroTablesStall)
	}

	if overrides.NumCompactorsSet {
		opts = opts.WithNumCompactors(overrides.NumCompactors)
	}

	if overrides.BaseTableSizeSet {
		size, err := parseBadgerSizeFlag("badger-base-table-size", overrides.BaseTableSize)
		if err != nil {
			return badger.Options{}, err
		}
		opts = opts.WithBaseTableSize(size)
	}

	if overrides.ValueLogFileSizeSet {
		size, err := parseBadgerSizeFlag("badger-value-log-file-size", overrides.ValueLogFileSize)
		if err != nil {
			return badger.Options{}, err
		}
		opts = opts.WithValueLogFileSize(size)
	}

	if overrides.BlockCacheSizeSet {
		size, err := parseBadgerSizeFlag("badger-block-cache-size", overrides.BlockCacheSize)
		if err != nil {
			return badger.Options{}, err
		}
		opts = opts.WithBlockCacheSize(size)
	}

	if overrides.IndexCacheSizeSet {
		size, err := parseBadgerSizeFlag("badger-index-cache-size", overrides.IndexCacheSize)
		if err != nil {
			return badger.Options{}, err
		}
		opts = opts.WithIndexCacheSize(size)
	}

	if overrides.ValueThresholdSet {
		size, err := parseBadgerSizeFlag("badger-value-threshold", overrides.ValueThreshold)
		if err != nil {
			return badger.Options{}, err
		}
		opts = opts.WithValueThreshold(size)
	}

	if err := validateBadgerOptions(opts); err != nil {
		return badger.Options{}, err
	}

	return opts, nil
}

func parseBadgerSizeFlag(flagName, raw string) (int64, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, fmt.Errorf("--%s must not be empty", flagName)
	}

	size, err := humanize.ParseBytes(value)
	if err != nil {
		return 0, fmt.Errorf("parse --%s: %w", flagName, err)
	}
	if size > math.MaxInt64 {
		return 0, fmt.Errorf("--%s exceeds the maximum supported size", flagName)
	}
	return int64(size), nil
}

func validateBadgerOptions(opts badger.Options) error {
	switch {
	case opts.MemTableSize <= 0:
		return fmt.Errorf("--badger-memtable-size must be greater than 0")
	case opts.BaseTableSize <= 0:
		return fmt.Errorf("--badger-base-table-size must be greater than 0")
	case opts.NumMemtables < 1:
		return fmt.Errorf("--badger-num-memtables must be at least 1")
	case opts.NumLevelZeroTables < 1:
		return fmt.Errorf("--badger-num-level-zero-tables must be at least 1")
	case opts.NumLevelZeroTablesStall <= opts.NumLevelZeroTables:
		return fmt.Errorf("--badger-num-level-zero-tables-stall must be greater than --badger-num-level-zero-tables")
	case opts.NumCompactors == 1:
		return fmt.Errorf("--badger-num-compactors cannot be 1; Badger requires 0 or at least 2 compactors")
	case opts.BlockCacheSize < 0:
		return fmt.Errorf("--badger-block-cache-size must be greater than or equal to 0")
	case opts.IndexCacheSize < 0:
		return fmt.Errorf("--badger-index-cache-size must be greater than or equal to 0")
	case opts.ValueLogFileSize < badgerMinValueLogFileSize || opts.ValueLogFileSize >= badgerMaxValueLogFileSize:
		return fmt.Errorf("--badger-value-log-file-size must be in range [1MiB, 2GiB)")
	case opts.ValueThreshold < 0:
		return fmt.Errorf("--badger-value-threshold must be greater than or equal to 0")
	case opts.ValueThreshold > badgerMaxValueThreshold:
		return fmt.Errorf("--badger-value-threshold must be less than or equal to %s", FormatBytes(badgerMaxValueThreshold))
	}

	if opts.Compression != badgeroptions.None && opts.BlockCacheSize == 0 {
		return fmt.Errorf("--badger-block-cache-size must be greater than 0 while Badger compression is enabled")
	}

	maxValueThresholdForMemtable := (15 * opts.MemTableSize) / 100
	if opts.ValueThreshold > maxValueThresholdForMemtable {
		return fmt.Errorf(
			"--badger-value-threshold must be less than or equal to 15%% of --badger-memtable-size (%s)",
			FormatBytes(maxValueThresholdForMemtable),
		)
	}

	return nil
}
