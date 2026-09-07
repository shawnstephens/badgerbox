# Reproducible demo benchmarks

Build once, then benchmark the compiled binary. Each invocation creates a fresh,
disk-backed Badger database, enqueues exactly the requested count, stops intake,
and waits for delivery and durable settlement. There is no per-message logging.

```sh
cd cmd/badgerbox-demo
GOWORK=off go build -o /tmp/badgerbox-demo .
/tmp/badgerbox-demo benchmark \
  --messages 100000 --payload-bytes 1024 \
  --badger-sync-writes=true \
  --badger-memtable-size 16MiB --badger-num-memtables 2 \
  --badger-block-cache-size 8MiB --badger-index-cache-size 4MiB \
  --badger-value-log-file-size 16MiB --badger-value-threshold 1KiB \
  --enqueue-parallelism 2 --processor-concurrency 2 \
  --processor-claim-batch-size 8 --poll-interval 5ms \
  --output /tmp/badgerbox-report.json
```

These settings are an experiment, not a universal capacity recommendation. Change
one variable at a time when identifying a bottleneck. Use `--rate 1000` to offer
an aggregate 1,000 messages per second; the default rate of zero saturates the
enqueue workers. The offered rate is a schedule, not a guarantee: enqueue stalls
can make it fall behind and later catch up. `--timeout` bounds startup, intake,
and drain; cleanup uses separate bounded shutdown contexts.
The benchmark requires at least two Badger compactors. Unlike the producer's
explicit zero-compactor mode, a finite write benchmark must keep compaction
running: exhausted L0 capacity can block Badger commits beyond context deadlines.

Without `--brokers`, the destination is an in-process verified sink. With
`--brokers localhost:9092`, the command creates a unique Kafka topic and uses a
separate consumer in the benchmark process. It verifies consumption through the
partition end offsets captured after outbox drain and producer flush. Use the
existing `badgerbox-demo kafka` command to start a disposable broker, or supply
your own brokers. Topics are retained for inspection; dispose of the broker or
remove the named topics afterward.

For reproducible comparisons from the repository root:

```sh
python3 scripts/benchmark/matrix.py \
  --binary /tmp/badgerbox-demo \
  --output-dir /tmp/badgerbox-results \
  --repeats 3
```

The output directory must be new. Add `--brokers localhost:9092` for Kafka.
The matrix runs sequential fresh processes with two sets of tuning values and
1 KiB, 64 KiB, and 512 KiB payloads. All profiles explicitly enable synchronous
writes. It retains raw JSON, logs, a CSV summary, medians, and a manifest with the
exact commands, environment overrides, and binary SHA-256. Successful databases
are removed after measurement unless `--keep-data` is set; failed databases are
retained. Individual `benchmark` invocations always retain their database and
report its path. `--quick` is a smoke test with much smaller counts, not evidence
for capacity sizing. Use a quiet host, sufficient disk space, repeated runs, and
a load duration long enough to exercise compaction and value-log GC.

## What a report proves

`passed: true` requires the requested count to have been accepted, every sequence
to have reached the destination, no invalid values, empty lifecycle/dead-letter
queues, a complete clean queue audit, matching enqueue telemetry, and successful
shutdown. The verifier checks sequence bounds, deterministic payload content, and
a CRC32 covering the sequence, embedded timestamp, and content. Payload size is
the exact Kafka value size, including 20 bytes of verification metadata. The
queue's JSON serialization and indexes consume additional storage.

Duplicate deliveries are counted separately. They are permitted by the outbox's
at-least-once contract, so duplicate observations do not by themselves make a run
fail. Consumer verification is independent of successful producer callbacks.

Local fault experiments use `--outage 2s` to reject publishes initially,
`--fail-every 10` to reject every tenth sequence on its first attempt, or
`--delivery-delay 10ms` to delay each batch. They exercise real persisted retries
and drain. These flags are rejected with Kafka; broker interruption and recovery
must be tested against the actual transport. A deadline, corrupt value, dead
letter, or runner error returns a nonzero status and writes a failed report with
available counts, queue state, and metrics. On failure, delivered records may
still be queued for retry; unique deliveries and remaining queue rows are not
disjoint sets.

## Measurement definitions

- Enqueue throughput spans worker startup through the last completed enqueue.
  Delivery throughput spans the same start through verified drain, including
  backlog clearance and Kafka consumption. Both exclude database open and close.
- Enqueue latency measures the `Enqueue` call. Delivery latency starts when the
  payload is constructed and ends when its first valid destination receipt is
  observed. It includes payload construction, enqueue, queueing, and transport.
  The timestamp uses the host wall clock; keep that clock stable during a run.
- Latency histograms use fixed memory. Reported percentile bounds have at most
  2% bucket quantization error above one nanosecond; mean and maximum use all
  samples. Duplicate receipts do not add a second delivery-latency sample.
- CPU, allocations, and GC are process measurements over intake and validation.
  They include payload generation, the verifier, resource sampling, and the
  Kafka producer and consumer. Kafka broker resources are outside these numbers.
- RSS and heap are sampled peaks, not exact high-water marks. Sampling begins
  after database open and finishes before database close; current RSS includes
  allocations already made during setup. `GOMAXPROCS`, `GOMEMLIMIT`, and `GOGC`
  can be set in the process environment. A Go memory limit does not cap all RSS.
- Disk peaks are sampled apparent file lengths, including Badger's preallocated
  or sparse files, not filesystem allocated blocks. Final apparent disk size is
  measured after database close. Measure actual filesystem allocation separately
  when sizing physical storage.
- `resources.measurements_complete` is false if a resource measurement fails;
  details appear in `measurement_errors`. A missing resource reading must not be
  interpreted as zero cost. `passed` describes delivery verification, not whether
  every platform resource counter is available.
- Verification retains one bit per expected sequence plus fixed-size latency
  histograms. This overhead is included in RSS. Full queue scans, auditing, and
  telemetry collection add measurement overhead; short runs amplify that cost.

The report is evidence for its recorded environment, configuration, workload,
and failure scenario. It does not establish a universal throughput guarantee or
replace storage power-loss, broker failover, long-running GC, and application
idempotency validation. Consult the repository's tuning and reliability guidance
alongside measured results.

The focused tests include the real persisted retry and timeout paths:

```sh
GOWORK=off go test -race -run Benchmark .
BADGERBOX_BENCHMARK_TEST_BROKERS=localhost:9092 \
  GOWORK=off go test -race -run TestBenchmarkKafkaConsumerConservation .
```
