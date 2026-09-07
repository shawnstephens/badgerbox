# Demo resource controls

Both `producer` and `benchmark` accept these flags. All capacity limits default
to zero (disabled or unlimited); enabled limits also appear in producer startup
logs and benchmark `config.resource_controls`.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--max-retained-messages` | `0` | Persisted namespace count limit across ready, processing, retries and dead letters. |
| `--max-retained-bytes` | `0` | Persisted namespace limit on canonical ready-record JSON bytes, including encoded payload, destination and metadata. |
| `--processor-claim-max-bytes` | `0` | Maximum conservative committed source-value bytes loaded per claim. |
| `--min-free-disk-bytes` | `0` | Advisory free-space margin checked on both Badger directories before enqueue. |
| `--disk-check-interval` | `100ms` | Shared disk sample lifetime; `0s` probes on every check. |
| `--admission-retry-interval` | `10ms` | Positive cancellable wait between rejected enqueue attempts. |

Byte flags accept values such as `64MiB`, `1.5GiB`, or exact byte counts, from
one through MaxInt64 bytes; zero disables the limit. A positive fraction smaller
than one byte is rejected. Each flag has an environment variable named by adding
`BADGERBOX_DEMO_`, capitalizing, and replacing hyphens with underscores, for example
`BADGERBOX_DEMO_PROCESSOR_CLAIM_MAX_BYTES`.

For example, a local logging producer with a bounded retained queue:

```sh
badgerbox-demo producer --logging-producer --db-path /tmp/badgerbox-limited \
  --max-retained-messages 5000 --max-retained-bytes 128MiB \
  --processor-claim-max-bytes 2MiB --min-free-disk-bytes 1GiB
```

The quota counts are transactional and persist with the namespace. Reopening an
existing directory requires its current persisted quota settings; changing flags
alone does not change persisted limits. Dead letters retain quota until resolved,
and an admission retry can wait indefinitely if retained rows cannot drain. The
producer stops that wait when cancelled; benchmark uses its overall `--timeout`.
Use the library quota update API for an existing namespace, or select a fresh demo
directory when comparing settings.

Logical retained bytes exclude auxiliary indexes, failure text, and Badger storage
overhead. They are not a physical disk or Go heap/RSS cap. Claim bytes bound source
materialization across a claim, not decoder allocation or total process memory.
A row larger than the entire claim-byte limit is quarantined without loading the
source payload, so set this above the largest legitimate encoded record. A
benchmark with quarantined rows fails its complete-delivery verification.

The disk guard uses cached filesystem observations; concurrent writes and other
processes can consume remaining space. It does not reserve bytes or provide a
hard disk cap. Disk pressure and namespace quota rejections are counted and retry
the same logical message after the configured wait. A message that cannot fit
inside the entire namespace byte quota fails immediately. Filesystem probe errors,
encoding failures and other enqueue errors fail immediately rather than being
hidden by pressure retries. Producer pressure logs are limited to once per second
per enqueue worker.

Benchmark reports include `admission_rejected_attempts`, the labeled
`admission_rejections` counts, and `final_usage`. Success requires zero final
retained messages and bytes, as well as all accepted messages independently
verified and an empty queue audit. Timeline points include retained messages,
retained logical bytes, and cumulative rejection attempts. Those samples may miss
brief peaks and are not an atomic snapshot with the other point fields. Enqueue
latency includes admission waiting; delivery latency starts at the first offered
enqueue attempt. Offered `--rate` is therefore a target, not guaranteed throughput
under pressure. A recovered producer can catch up with that schedule.

See [the sustained churn harness](../../scripts/benchmark/README.md) for repeatable
runs and the limits of finite GC/RSS evidence.
