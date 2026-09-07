# Demo measurements — 2026-09-06

All 12 saturation runs passed delivery conservation, payload validation, an empty
queue and a complete final audit. They verified **448,000 messages**, with zero
observed duplicates or invalid payloads. Every run enabled Badger SyncWrites.
Resource sampling succeeded in every report.

Host: Apple M4, 32 GiB RAM, macOS/arm64, Go 1.26.2, 10 logical CPUs and
GOMAXPROCS=10, local APFS storage. No explicit GOMEMLIMIT or GOGC override was
set. Processes ran sequentially; local test suites were stopped during this
matrix. This is a developer machine, not a dedicated capacity-test environment.

The destination was an **in-process verified sink**. Each profile/size had two
fresh-process repetitions: 100,000 messages at 1 KiB; 10,000 at 64 KiB; 2,000 at
512 KiB. Values contain deterministic incompressible data and verification
metadata, then use the demo's normal JSON serialization. Throughput spans intake
through verified drain. Latency includes the backlog created by saturation.

| Profile | Payload | Median messages/s | Median delivery p99 bound | Maximum sampled RSS |
| --- | --- | ---: | ---: | ---: |
| small-memory | 1 KiB | 1,881 | 45.23 s | 517 MiB |
| throughput | 1 KiB | 7,224 | 2.04 s | 535 MiB |
| small-memory | 64 KiB | 840 | 9.43 s | 1,293 MiB |
| throughput | 64 KiB | 888 | 9.84 s | 1,464 MiB |
| small-memory | 512 KiB | 110 | 15.84 s | 2,623 MiB |
| throughput | 512 KiB | 111 | 16.45 s | 2,903 MiB |

The `small-memory` label describes smaller configured caches/write buffers; it is
not a process-memory promise. At 512 KiB, its sampled heap stayed below 95 MiB
while RSS reached about 2.6 GiB. Memory mappings and other process allocations
must be accounted for separately. More aggressive concurrency helped the 1 KiB
workload substantially here but barely improved the larger-payload throughput.
Several settings differ between profiles, so these runs cannot identify the
effect of any single knob.

These runs lasted roughly 11–55 seconds. They finish before the default 60-second
GC tick, so their post-close apparent disk sizes include obsolete value-log data.
They do not prove a stable long-term RSS/disk plateau or reclamation efficiency.
Apparent file sizes also include preallocation/sparse space; physical allocated
blocks were not measured. The [readiness ledger](../../READINESS.md) tracks the
longer churn and admission-control work still needed.

## Real Kafka verification

The [Kafka report](kafka-64k.json) used the same smaller-buffer configuration,
10,000 × 64 KiB messages, a disposable single broker
`confluentinc/confluent-local:7.5.0` under Docker Desktop, and a separate consumer
inside the benchmark process. It passed with 10,000 unique deliveries, zero
duplicates/invalid values, and an empty audited queue. Measured intake-to-consumed
drain was 20.45 seconds, or **489 messages/s**, with a sampled RSS peak of
1,331 MiB. Broker resources are excluded. This one run is transport verification
and a local measurement, not a replicated Kafka capacity or failover result.

## Reproduction and evidence

The [rate/outage/GC report](rate-outage-gc-64k.json) offered 500 messages/s,
injected an initial two-second outage plus first-attempt failures for every tenth
message, and ran GC every second. It verified all 10,000 × 64 KiB messages with
12,536 attempts and 2,536 persisted requeues; there were no observed duplicates or
invalid values. Drain completed in 20.10 seconds, sampled RSS peaked at 775 MiB,
and telemetry recorded 20 maintenance attempts. It recorded no successful GC
rewrites during this short window. This verifies retry/catch-up behavior and
maintenance scheduling, not sustained disk reclamation.

Including the separate Kafka and fault scenarios, these reports verify 468,000
accepted messages. Both additional reports contain their full configuration;
they are single runs and are excluded from the matrix medians.

The [manifest](manifest.json) preserves every matrix command, environment
override, and binary SHA-256. The source was committed in `c3203fe` (the binary
was built immediately before that source commit, so its embedded VCS marker
records the predecessor with local changes). Later edits to error-string casing
do not change the measured successful path. The [CSV](summary.csv),
[aggregate medians](medians.json), and per-run JSON files retain the measurements
and configuration. Each raw report includes process/runtime/build metadata and
the final queue, audit, and cumulative metrics.

To repeat on the target host, build the demo and run:

```sh
python3 scripts/benchmark/matrix.py --binary /tmp/badgerbox-demo \
  --output-dir /tmp/badgerbox-results --repeats 2
```

For exact flags for a particular experiment, use its manifest entry. The matrix
removes successful temporary databases after collecting results. Raw reports
retain their original temporary paths for provenance; those paths no longer
contain a database. See the [benchmark guide](../../../cmd/badgerbox-demo/BENCHMARK.md)
and [tuning guide](../../TUNING.md) before interpreting or extending these results.
