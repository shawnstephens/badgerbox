package main

import (
	"github.com/dgraph-io/badger/v4"
	"github.com/shawnstephens/badgerbox/cmd/badgerbox-demo/internal/demo"
	cli "github.com/urfave/cli/v3"
)

func producerBadgerOptions(cmd *cli.Command) (badger.Options, error) {
	return demo.BuildBadgerOptions(cmd.String("db-path"), demo.BadgerOptionsOverrides{
		SyncWritesSet:              cmd.IsSet("badger-sync-writes"),
		SyncWrites:                 cmd.Bool("badger-sync-writes"),
		MemTableSizeSet:            cmd.IsSet("badger-memtable-size"),
		MemTableSize:               cmd.String("badger-memtable-size"),
		NumMemtablesSet:            cmd.IsSet("badger-num-memtables"),
		NumMemtables:               cmd.Int("badger-num-memtables"),
		NumLevelZeroTablesSet:      cmd.IsSet("badger-num-level-zero-tables"),
		NumLevelZeroTables:         cmd.Int("badger-num-level-zero-tables"),
		NumLevelZeroTablesStallSet: cmd.IsSet("badger-num-level-zero-tables-stall"),
		NumLevelZeroTablesStall:    cmd.Int("badger-num-level-zero-tables-stall"),
		NumCompactorsSet:           cmd.IsSet("badger-num-compactors"),
		NumCompactors:              cmd.Int("badger-num-compactors"),
		BaseTableSizeSet:           cmd.IsSet("badger-base-table-size"),
		BaseTableSize:              cmd.String("badger-base-table-size"),
		ValueLogFileSizeSet:        cmd.IsSet("badger-value-log-file-size"),
		ValueLogFileSize:           cmd.String("badger-value-log-file-size"),
		BlockCacheSizeSet:          cmd.IsSet("badger-block-cache-size"),
		BlockCacheSize:             cmd.String("badger-block-cache-size"),
		IndexCacheSizeSet:          cmd.IsSet("badger-index-cache-size"),
		IndexCacheSize:             cmd.String("badger-index-cache-size"),
		ValueThresholdSet:          cmd.IsSet("badger-value-threshold"),
		ValueThreshold:             cmd.String("badger-value-threshold"),
	})
}
