# Tuning and capacity planning

Choose a durability boundary, a maximum outage backlog, and a delivery-latency
target before tuning throughput. A benchmark only measures its recorded workload
and machine. Repeat the demo benchmark on the intended filesystem and container
limits, with realistic payloads and the actual downstream service.

## Choose the durability boundary

Use `badger.DefaultOptions(path).WithSyncWrites(true)` when a successful commit
must be synchronized to storage. The demo exposes `--badger-sync-writes`.
Badger's default is false; compare performance using the setting you will deploy.
Synchronization still depends on filesystem/device behavior. The subprocess
tests verify process crashes, not power cuts or loss of a volume.

`EnqueueTx` can atomically commit an event with application state **in the same
Badger transaction**. It cannot atomically couple a PostgreSQL/MySQL update with
a Badger enqueue. For application state in another database, use an outbox
transaction in that database or another explicit recovery protocol; two
independent successful writes do not establish an atomic outbox.

Delivery is at least once. Use a stable application event ID for downstream
deduplication. A Badger message ID is scoped to its namespace/database; it is not
a globally unique business-event ID. Parallel workers, retry delays, lease
recovery, and Kafka partitioning can reorder deliveries. A single worker alone
does not preserve strict application order across retries.

The Kafka adapter requires `AllISRAcks`, rejects transactional clients, and
rejects a forced default topic that overrides the destination. Configure broker
replication and `min.insync.replicas` for your failure budget. The single-broker
demo validates the adapter path; it does not validate replicated availability.

## Start with a resource profile

These are starting configurations to measure, not RAM ceilings or guaranteed
throughput. Explicitly enable SyncWrites in every profile when required.

| Setting | Smaller memory budget | General starting point | Large payloads |
| --- | --- | --- | --- |
| Badger `MemTableSize` | 16 MiB | 32 MiB | 32 MiB |
| `NumMemtables` | 3 | 3 | 3 |
| `BlockCacheSize` | 16 MiB | 64 MiB | 32 MiB |
| `IndexCacheSize` | 16 MiB | 64 MiB | 32 MiB |
| `ValueThreshold` | 64 KiB | 64 KiB | 16 KiB |
| `ValueLogFileSize` | 64 MiB | 256 MiB | 256 MiB |
| `NumCompactors` | 2 | 4 | 4 |
| Batch processor `Concurrency` | 2 | 4 | 2 |
| `ClaimBatchSize` | 8 | 32 | 4 |

Keep the default Level 0 trigger/stall thresholds initially. If you change them,
the stall threshold must exceed the trigger. `IndexCacheSize=0` keeps all table
indexes in memory; it does not disable index caching. Compression requires a
positive block cache. Compactors compete for CPU and disk bandwidth, but turning
them off can eventually stall a sustained writer. See the complete
[Badger flag reference](MEMORY.md).

For example, from `cmd/badgerbox-demo`:

```sh
GOWORK=off go run . producer --logging-producer --badger-sync-writes \
  --badger-memtable-size 16MiB --badger-num-memtables 3 \
  --badger-block-cache-size 16MiB --badger-index-cache-size 16MiB \
  --badger-value-threshold 64KiB --badger-value-log-file-size 64MiB \
  --badger-num-compactors 2 \
  --processor-concurrency 2 --processor-claim-batch-size 8
```

This continuous demo logs deliveries; use the finite benchmark below to compare
capacity without per-message logging overhead.

## Budget memory across the whole process

Account for application allocations, Badger caches and write buffers, decoded
claimed messages, serialization copies, Kafka buffers, tracing/export queues,
and memory mappings. Cache settings and `MemTableSize * NumMemtables` describe
components of this budget, not total resident memory.

A batch processor can own up to `Concurrency * ClaimBatchSize` messages.
For 4 workers, batches of 32, and 512 KiB payloads, one decoded copy alone is
64 MiB. Serialization, destination data and client-owned copies add to that.
The adapter clones bytes before asynchronous delivery. Bound franz-go with
`kgo.MaxBufferedBytes` and `kgo.MaxBufferedRecords`, especially for shared clients
and late callbacks. Core claim sizes count records; they are not a configurable
decoded-byte or process-memory limit. Adaptive transaction shrinking protects
Badger transaction limits, not arbitrary application codec expansion.

Reduce batch size first for large payloads. Increase enqueue parallelism only
while accepted throughput improves: each producer can retain an encoded record
and create transaction conflicts. Avoid retaining entire producer request bodies
in an unbounded application queue ahead of Badger.

Use `GOMEMLIMIT` for Go runtime memory control with room left for memory mappings,
other libraries and the surrounding process. It is a soft limit, not a total RSS
cap. Compare RSS with Go heap and GC CPU; an aggressive limit can increase CPU
cost. See the [Go GC guide](https://go.dev/doc/gc-guide#Memory_limit) for the
runtime's accounting and limits.

## Match throughput and latency to the load

Measure the durable settlement rate, not just enqueue or callback rate. For
arrival rate λ and sustained successful service rate μ, backlog grows when
λ > μ. To drain an outage backlog B while traffic continues, a first estimate is
`B / (μ - λ)` seconds, and is only defined when μ > λ. For 1,000 messages/s
arriving, 1,500/s settling, and a 300,000-message backlog, the estimate is ten
minutes. Measure the actual catch-up behavior because compaction and contention
change μ as the queue grows.

| Symptom | First adjustment | Verify afterward |
| --- | --- | --- |
| Ready depth and age rise with idle CPU | Increase concurrency or batch size gradually; inspect downstream latency | Settlement rate improves without increased timeouts/RSS |
| CPU saturated by encoding/GC | Reduce concurrent payload copies; use a compact custom codec; try smaller batches | CPU per settled message and p99 both improve |
| CPU/disk busy with Level 0 backlog | Provide compaction resources, lower large-value threshold, reduce intake | Compaction catches up and writes stop stalling |
| Low-load dispatch latency | Reduce `PollInterval` | Snapshot/reaper/idle CPU remains acceptable |
| Lease expiry during normal delivery | Increase `LeaseDuration`; set downstream timeouts below it | Retries/duplicates fall and recovery delay remains acceptable |
| Large-value claim reductions | Reduce batch size | `badgerbox_claim_transaction_too_big_total` stops growing excessively |
| Expensive monitoring scans | Increase telemetry `PollInterval` or reduce namespace scrape work | Snapshot duration stays well below its interval |

The default lease is 30 seconds. Include callback scheduling, broker buffering,
delivery, and settlement when budgeting it. A batch shares a lease budget;
serially processing many slow messages inside one batch can exhaust it. Callbacks
must observe cancellation and return; their concurrency slot remains occupied
during cleanup. The library cannot forcibly terminate application callbacks.

Set `RetryBaseDelay`, `RetryMaxDelay`, and `MaxAttempts` to cover the intended
outage. Core defaults are 1 second, 1 hour and 36 attempts. Retries use capped
exponential delay; they have no randomized jitter. Many failures at once can
therefore create synchronized retry traffic. Rate-limit intake and downstream
requests as needed, and include the retry schedule in outage testing. A lease
timeout and recovery also consume the retry allowance once work is attempted.

`RequeuePageSize` bounds candidates recovered per reaper pass (default 64);
recovery uses one transaction per record. Smaller pages lower work per pass but
can extend recovery. `SettlementTimeout`
(default 10 seconds) bounds detached settlement, including contention. Increasing
it helps slow storage finish settlements but lengthens shutdown. Always stop
intake, join processors, flush/close delivery clients, then close stores and DB.

## Provision disk for outages and reclamation

Estimate backlog as arrival rate × outage duration × measured stored bytes per
message, then add dead letters, indexes, obsolete versions, active value logs,
compaction output, and free workspace for GC. Stored envelopes base64-encode
codec bytes; JSON encoding a `[]byte` inside the payload can add a second base64
layer. Measure allocated disk on representative incompressible data as well as
the compressible data your service normally emits.

Use `pkg/admission.DiskGuard` through `Options.EnqueueGuard` to reject intake
below a free-space margin, including when filesystem measurement fails. The
guard checks both configured Badger directories and allows settlement to
continue. It is advisory: sampling cannot reserve physical bytes against other
writers. See [admission](ADMISSION.md) for setup, error handling, and caching.
Applications must handle enqueue errors without reporting success. A snapshot
followed by an enqueue is not an atomic quota check with concurrent producers.
Set `Options.AdmissionLimits.MaxRetainedMessages` and `MaxRetainedBytes` to
enforce transactional backlog quotas across all stores in a namespace. Limits
include processing messages and dead letters; only acknowledgement releases
capacity. Zero means unlimited. Use `Usage` to inspect current capacity and
`CompareAndSwapAdmissionLimits` to tune limits live. All stores must specify
the current persisted limits when reopening. The byte quota measures canonical
record bytes, excluding physical amplification. Retain workspace for settlement
and reclamation. Do not delete an undrained DB.

Acknowledgement deletes logical records; it does not immediately release disk.
Enable periodic `maintenance.Service`/runner value-log GC. Defaults allow eight
calls or one second of new calls per tick. A smaller value-log file rotates
sooner, improving reclamation granularity at the cost of more files. A successful
rewrite can require temporary disk space. Compaction and GC must keep up through
sustained churn, not merely a short enqueue/drain cycle. Bound and investigate
dead-letter retention as a separate operational workload.

Monitor every volume if `Dir` and `ValueDir` are on different filesystems. The
database collector's disk-capacity gauge reports one `DiskPath` (default `Dir`),
not both. Badger size gauges can lag about a minute; use filesystem monitoring for
capacity alerts. See [observability](OBSERVABILITY.md) for metric semantics.

## Run a finite benchmark

Build once so compilation does not contaminate process resource measurements:

```sh
cd cmd/badgerbox-demo
GOWORK=off go build -o /tmp/badgerbox-demo .
/tmp/badgerbox-demo benchmark --messages 10000 --payload-bytes 1024 \
  --badger-sync-writes --output /tmp/badgerbox-1k.json
/tmp/badgerbox-demo benchmark --messages 1000 --payload-bytes 524288 \
  --processor-concurrency 2 --processor-claim-batch-size 4 \
  --badger-sync-writes --output /tmp/badgerbox-512k.json
```

Vary one setting at a time, then test combinations. Include small/medium/large
payloads, both low arrival rate and saturation, and an outage followed by catch-up.
Use fresh processes for comparisons of RSS high-water marks. Keep machine type,
Go version, `GOMAXPROCS`, durability, filesystem, payload distribution, sample
count, and broker configuration with every report. Repeat runs; avoid treating
one noisy ranking as a universal recommendation.

The local sink verifies queue behavior without a broker. Supply `--brokers` to
also verify consumption from a real Kafka topic. A report must conserve accepted
messages, drain the live queue, and complete its consistency audit before it is
used as successful throughput evidence. Check errors and duplicates separately.
For the full flag list, finite retry/outage recipes, matrix runner, and precise
measurement definitions, see the [demo benchmark guide](../cmd/badgerbox-demo/BENCHMARK.md).
The matrix is intentionally an experiment in two configurations; it does not
claim its higher-concurrency profile will win on every payload or host.

The [recorded local measurements](benchmarks/2026-09-06/README.md) include the
12-run matrix, a real Kafka run, and a rate-limited outage/retry experiment. Their
large-payload RSS and retained disk demonstrate why short-run throughput and
small caches are insufficient evidence of a fixed resource ceiling.

Benchmarks complement the race, process-crash, retry, broker-outage and storage
tests; they do not establish deployment-specific availability or power-loss
guarantees.
