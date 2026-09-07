# Sustained churn and reclamation

Build the demo, then run one experiment at a time on the measurement host:

```sh
(cd cmd/badgerbox-demo && GOWORK=off go build -o /tmp/badgerbox-demo .)
python3 scripts/benchmark/soak.py --binary /tmp/badgerbox-demo --output-dir /tmp/badgerbox-soak
```

The default experiment offers 500 messages/s with 16 KiB values for three minutes,
then observes ordinary maintenance for 30 seconds after verified delivery drains.
It uses synchronous writes, small memtables and 8 MiB value-log files to exercise
repeated flushes, compaction, rotation and GC in a short reproducible run. These
settings are an experiment, not a recommended profile for every application.
The generated database is retained. `--disable-gc` runs a comparable control with
the same workload and storage settings but no periodic value-log GC.

The output directory contains the command and binary hash in `manifest.json`, the
full schema-versioned `report.json`, `timeline.csv`, descriptive `summary.json`,
and the process log. Reports include build settings and dependency versions;
capture the source revision in the built binary by building from a committed
checkout. Measure each experiment in a fresh process and directory. Avoid other
CPU/disk workloads while collecting comparative results.

Schema version 2 preserves the final labeled counter series, including each GC
operation/outcome. The resource timeline records timestamps, elapsed seconds,
RSS, heap, cumulative allocation/CPU, accepted and independently verified delivery
counts, apparent SST/value-log/WAL sizes, and cumulative maintenance counters.
Points are sampled with `--timeline-interval` (one second by default) and bounded
by `--timeline-max-points` (600 by default, maximum 3600). Longer runs retain every
second, fourth, eighth, etc. observation across the complete run; the first and
last point survive. The report makes this downsampling explicit. Sampling can
miss short peaks and sources within one point are not an atomic snapshot.

`--observe-after-drain` keeps ordinary maintenance and sampling active after
delivery verification. This phase is excluded from delivery throughput and
included in resource totals and the overall timeout. No forced compaction,
value-log rewrite, database reopen, or `runtime.GC` changes the experiment. Worker
and consumer shutdown completes before the final counters and snapshot are frozen.

Badger v4.9.6 value-log GC selects a sealed file with enough discardable data.
LSM compaction generates those discard statistics; the current writable value-log
file is not eligible. A short run can therefore record repeated `no_rewrite`
outcomes even after queue delivery drains. Successful rewrites and observed drops
in the value-log footprint provide stronger reclamation evidence. File sizes
include preallocation; they are not physical disk consumption or exact reclaimed
byte counts. Post-close sizes can shrink merely because Badger truncates files.

Inspect the time series through warmup, steady intake, drain and idle observation.
The summary's slopes describe the latter half of intake, with no automatic
"plateau passed" verdict. A three-minute experiment does not prove a long-term
memory bound, sustained production capacity, Kafka behavior, or crash durability.
Extend duration, repeat against the actual filesystem and payload distribution,
and compare only matching durability and maintenance settings.
