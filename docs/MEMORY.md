# Badger Memory Tuning

`badgerbox` accepts a caller-owned `*badger.DB`, so Badger memory tuning still happens at the Badger layer. This repo currently pins `github.com/dgraph-io/badger/v4 v4.9.1`, and the guidance below is written for that version.

The demo producer now exposes the main Badger memory knobs as CLI flags and `BADGERBOX_DEMO_...` environment variables, but the same settings can be applied directly in Go with `badger.DefaultOptions(...).WithX(...)`.

## Badger v4.9.1 memory knobs and defaults

| Option | Default | What it changes | When to tweak it first |
| --- | --- | --- | --- |
| `IndexCacheSize` | `0` | `0` means Badger keeps all table indices and bloom filters in memory. | First knob to cap if memory grows with table count. |
| `BlockCacheSize` | `256MiB` | Cache for decompressed table blocks. | First knob to reduce for a fast, fixed RAM win. |
| `MemTableSize` | `64MiB` | Size of each memtable before flush. | Reduce when write-heavy workloads keep too much mutable state resident. |
| `NumMemtables` | `5` | Number of memtables allowed before writers stall. | Reduce early if you want a direct cap on in-memory write buffering. |
| `NumLevelZeroTables` | `5` | Level 0 compaction trigger. | Adjust alongside `NumMemtables`. |
| `NumLevelZeroTablesStall` | `15` | Level 0 hard stall threshold. | Adjust alongside `NumMemtables` and `NumLevelZeroTables`. |
| `ValueThreshold` | `1MiB` | Values larger than this go to the value log instead of the LSM tree. | Lower when large values are inflating LSM-related memory. |
| `NumCompactors` | `4` | Number of concurrent compaction workers. | Not a first-line RAM knob; changing it mostly affects write pressure and backlog. |
| `BaseTableSize` | `2MiB` | Base LSM table size target. | Secondary tuning knob after caches and memtables. |
| `ValueLogFileSize` | `1073741823` bytes | Size of each value log file before rotation. | Mostly affects rotation and GC granularity, not steady-state RAM. |

Practical tuning order for most services:

1. Cap `IndexCacheSize`.
2. Reduce `BlockCacheSize`.
3. Reduce `NumMemtables` and, if needed, `MemTableSize`.
4. Lower `ValueThreshold` if large values are common.
5. Adjust `NumLevelZeroTables` and `NumLevelZeroTablesStall` to stay aligned with lower memtable buffering.

Notes:

- `IndexCacheSize=0` is not “disabled cache”; it means “keep all indices in memory”.
- The demo keeps Badger compression enabled, so `BlockCacheSize` must stay positive there.
- `NumCompactors=0` is allowed by Badger but is risky for long-lived writer workloads because compactions stop completely.

## Demo producer flags

The demo producer treats every memory flag as an override. If you omit a flag or env var, it leaves that field at `badger.DefaultOptions(dbPath)`.

| Flag | Env var | Default shown in help | Notes |
| --- | --- | --- | --- |
| `--badger-memtable-size` | `BADGERBOX_DEMO_BADGER_MEMTABLE_SIZE` | `64MiB` | Human-readable size string. |
| `--badger-num-memtables` | `BADGERBOX_DEMO_BADGER_NUM_MEMTABLES` | `5` | Integer count. |
| `--badger-num-level-zero-tables` | `BADGERBOX_DEMO_BADGER_NUM_LEVEL_ZERO_TABLES` | `5` | Integer count. |
| `--badger-num-level-zero-tables-stall` | `BADGERBOX_DEMO_BADGER_NUM_LEVEL_ZERO_TABLES_STALL` | `15` | Must be greater than `--badger-num-level-zero-tables`. |
| `--badger-num-compactors` | `BADGERBOX_DEMO_BADGER_NUM_COMPACTORS` | `4` | `0` or `>= 2`; `1` is rejected. |
| `--badger-base-table-size` | `BADGERBOX_DEMO_BADGER_BASE_TABLE_SIZE` | `2MiB` | Human-readable size string. |
| `--badger-value-log-file-size` | `BADGERBOX_DEMO_BADGER_VALUE_LOG_FILE_SIZE` | `1073741823 bytes (~1024MiB)` | Must stay in `[1MiB, 2GiB)`. |
| `--badger-block-cache-size` | `BADGERBOX_DEMO_BADGER_BLOCK_CACHE_SIZE` | `256MiB` | Must stay positive in the demo because compression is enabled. |
| `--badger-index-cache-size` | `BADGERBOX_DEMO_BADGER_INDEX_CACHE_SIZE` | `0` | `0` means all indices stay in memory. |
| `--badger-value-threshold` | `BADGERBOX_DEMO_BADGER_VALUE_THRESHOLD` | `1MiB` | Must be `<= 1MiB` and `<= 15%` of `MemTableSize`. |

Example demo producer command:

```bash
go run ./demo producer \
  --badger-index-cache-size 128MiB \
  --badger-block-cache-size 64MiB \
  --badger-memtable-size 32MiB \
  --badger-num-memtables 3 \
  --badger-num-level-zero-tables 3 \
  --badger-num-level-zero-tables-stall 9
```

The same overrides can be passed with environment variables:

```bash
BADGERBOX_DEMO_BADGER_INDEX_CACHE_SIZE=128MiB \
BADGERBOX_DEMO_BADGER_BLOCK_CACHE_SIZE=64MiB \
BADGERBOX_DEMO_BADGER_MEMTABLE_SIZE=32MiB \
go run ./demo producer
```

## Direct Go examples

### Lower memory first

This is the first profile to try when you want a materially smaller Badger footprint without changing your application structure.

```go
opts := badger.DefaultOptions("./data").
	WithLogger(nil).
	WithIndexCacheSize(128 << 20).
	WithBlockCacheSize(64 << 20).
	WithMemTableSize(32 << 20).
	WithNumMemtables(3).
	WithNumLevelZeroTables(3).
	WithNumLevelZeroTablesStall(9)

db, err := badger.Open(opts)
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

### Large values: lower the threshold

If payloads are often tens or hundreds of KiB, lowering `ValueThreshold` pushes more values into the value log and keeps less value data in the LSM tree.

```go
opts := badger.DefaultOptions("./data").
	WithLogger(nil).
	WithIndexCacheSize(128 << 20).
	WithBlockCacheSize(64 << 20).
	WithValueThreshold(64 << 10)

db, err := badger.Open(opts)
if err != nil {
	log.Fatal(err)
}
defer db.Close()
```

### Secondary knobs

These are usually not the first changes to make, but they are sometimes useful after the cache and memtable reductions above.

```go
opts := badger.DefaultOptions("./data").
	WithLogger(nil).
	WithBaseTableSize(1 << 20).
	WithValueLogFileSize(256 << 20).
	WithNumCompactors(2)
```

`ValueLogFileSize` mainly changes value-log rotation and GC granularity. `NumCompactors` affects compaction throughput more than raw memory, and setting it too low can let Level 0 back up.
