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

The harness also accepts the demo resource-control flags:
`--max-retained-messages`, `--max-retained-bytes`,
`--processor-claim-max-bytes`, `--min-free-disk-bytes`,
`--disk-check-interval`, and `--admission-retry-interval`. All capacity limits
remain disabled by default. Add `--outage-seconds 20` with a message quota to
exercise recovery from a full namespace; this is a local sink outage. Reports
and summaries preserve explicit settings, rejection counts by reason, and final
usage. Timeline points record retained messages/bytes and cumulative admission
rejections. Admission wait contributes to enqueue and end-to-end latency, and
backpressure can lower achieved throughput below the offered rate. The full
semantics and persisted quota rules are in [the producer guide](../../cmd/badgerbox-demo/README.md).

## Linux cgroup evidence

`cgroup_probe.go` is a standalone static Linux supervisor for a scratch container.
It launches the benchmark, samples cgroup v2 once per second, and records a final
sample before the cgroup disappears. Its JSON preserves kernel `memory.max`,
`memory.swap.max`, `cpu.max`, `memory.peak`, memory events (including OOM kills),
CPU usage/throttling, and selected memory statistics. Up to 1024 points cover the
whole run through even downsampling. Missing statistics fail the run; a default
10-minute supervisor timeout kills a stuck child. Signals are forwarded.

Build both binaries into an otherwise empty temporary context (set `GOARCH` to
the Docker engine architecture), then build `Dockerfile.cgroup` with that context:

```sh
mkdir /tmp/badgerbox-cgroup-image /tmp/badgerbox-cgroup-output
(cd cmd/badgerbox-demo && CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOWORK=off \
  go build -o /tmp/badgerbox-cgroup-image/badgerbox-demo .)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOWORK=off go build \
  -o /tmp/badgerbox-cgroup-image/cgroup-probe scripts/benchmark/cgroup_probe.go
docker build -f scripts/benchmark/Dockerfile.cgroup \
  -t badgerbox-cgroup-evidence /tmp/badgerbox-cgroup-image
docker run --name badgerbox-cgroup-evidence --network=none \
  --cpus=2 --memory=256m --memory-swap=256m \
  -e GOMAXPROCS=2 -e GOMEMLIMIT=96MiB \
  -v /tmp/badgerbox-cgroup-output:/output \
  badgerbox-cgroup-evidence --output /output/cgroup.json -- \
  /badgerbox-demo benchmark --messages 90000 --rate 500 --payload-bytes 16384 \
  --observe-after-drain 30s --timeout 330s --db-path /data \
  --output /output/report.json --max-retained-messages 128 \
  --max-retained-bytes 64MiB --processor-claim-max-bytes 2MiB \
  --min-free-disk-bytes 512MiB
```

Choose Badger settings explicitly as in `soak.py` for comparative GC evidence;
the command above otherwise uses demo defaults. Database writes use the writable
container layer, while only reports use the host bind mount. Retain `docker
inspect` output before removing the stopped container. Cgroup memory includes
supervisor overhead, file cache and kernel accounting, so compare it separately
with benchmark process RSS. The kernel peak captures short memory peaks that
periodic samples miss; the finite experiment still does not prove a long-term
bound, production capacity or crash durability. Docker Desktop adds a VM and
storage layer that differs from a native production Linux host.
