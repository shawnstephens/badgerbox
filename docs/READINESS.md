# Production-readiness verification

This document tracks the evidence from the production-readiness audit. Passing
tests prove the behaviors they exercise; deployment capacity and failure budgets
still require measurement on the target filesystem and downstream service.

## Verified behaviors

| Requirement | Authoritative checks |
| --- | --- |
| No message-ID reuse on reservation conflict | `TestConcurrentStoresNeverReuseMessageIDs` and the deterministic sequence reservation/encoding/overflow tests |
| Worker concurrency includes canceled callback cleanup | `TestLeaseExpiryRetainsCallbackConcurrencySlot` in `pkg/badgerbox` |
| Invalid processor tuning rejected before use | `processor_options_test.go` |
| Acknowledgement requires appropriate Kafka acceptance | `client_safety_test.go`: weak ack, transaction and forced-topic rejection |
| Recovery from unresponsive downstream | `TestBatchDeliverySurvivesBrokerOutage`: pause real Kafka, verify retained outbox payloads, resume and independently consume every message |
| Transaction persistence and payload integrity across size profiles | `TestDurablePayloadProfiles`: on-disk 1 KiB, 64 KiB and 512 KiB random binary payloads, synchronized commits, application state in the same transaction, retry, DLQ, exact requeue, reopen and final audit |
| Process-crash and graceful shutdown recovery | `TestRecoveryAcrossGracefulAndAbruptProcessExit`, `TestAcknowledgedMessagesStayDeletedAfterSIGKILL` |
| Trustworthy library duration metrics | `internal/instrumentation/histograms_test.go`: units, useful default buckets, explicit application-view override |
| Unsupported database metric modes rejected | `pkg/telemetry` configuration tests for in-memory DBs and disabled Badger metrics |
| Reproducible performance comparisons | Demo `benchmark`, `scripts/benchmark/matrix.py`, raw JSON reports and command manifests |
| Atomic retained-count and canonical-byte quotas | `admission_test.go`: concurrent producers/stores/constructors, rollback, conflicts, restart, exact limits, live CAS, corruption/overflow detection |
| Safe external capacity checks | `pkg/admission` and `enqueue_guard_test.go`: coalesced cancellable disk probes, fail-closed errors, no pre-admission allocation/writes, continued settlement |
| Byte-bounded single and batch claims | `claim_bytes_test.go`: mixed sizes, conservative Badger value-log sizing, healthy progress behind a 16 MiB malformed oversized head without loading it |
| Codec errors/panics remain recoverable | `quarantine_test.go`: opaque bytes preserved, healthy records continue, callback metrics stay truthful |
| Quarantine preserves capacity and exact recovery | `quarantine_admission_test.go` and `pkg/adminhttp/quarantine_test.go`: reference/ordinary DLQ reconciliation, bounded inspection/replay, changed-source rejection, acknowledgement under intake pressure |
| Inspectable admission state | `/usage`, quota gauges, and independent bounded audit reconciliation with usage-error telemetry |
| Full-filesystem admission and recovery | [Disposable APFS fault test](RESOURCE_FAULTS.md): actual `ENOSPC`, both enqueue paths rejected, application transaction rolled back, all 32 retained messages verified after reopening and drain |
| Sustained ordinary GC | [Three-minute churn evidence](../scripts/benchmark/evidence/2026-09-07-local-churn/INTERPRETATION.md): 90,000 verified deliveries, 601 successful rewrites, bounded timeline and labeled outcomes |
| Final controls under container resource limits | [Linux cgroup evidence](../scripts/benchmark/evidence/2026-09-07-cgroup-controls/README.md): 90,000 verified deliveries at 499.87/s, 37 quota rejections retried, 618 successful GC rewrites, no OOM or swap, final usage zero |

Run `just check` for both modules and `just test-integration` with Docker.
Integration tests require real dependencies and fail when setup fails. The
benchmark's local sink and independently consumed Kafka mode must be identified
separately in reports. Preserve failures as well as successful runs.

## Capacity and failure scope

Admission limits bound logical retained data; claim budgets bound stored source
reads. Neither is an RSS or physical-disk cap. Codec expansion, external client
buffers, compaction, mmap/page cache, and filesystem overhead still need a
process/container budget. The free-space guard samples available capacity and
cannot reserve physical space against concurrent writers.

The three-minute churn run demonstrates normal reclamation, including repeated
value-log rotation and successful GC. Its disk peaks still drifted upward, so
it does not establish a long-term disk plateau. The final-controls run verifies
continued intake and reclamation under a configured 2-CPU quota, 256 MiB cgroup
memory limit, and no swap. Its message quota reached 128 and blocked new intake
until capacity became available. Kernel memory peaked at 256 MiB plus 4 KiB,
and sampled process RSS reached 257.74 MiB; these are different accounting
measurements, so this is not an exact RSS-ceiling claim. The raw report preserves
memory pressure, peak, and OOM counters. Repeat on the deployment's filesystem
and payload distribution to choose sustained capacity and outage drain margin.

The full-volume test verifies guarded rejection at real filesystem exhaustion;
the crash tests separately verify process recovery. They do not inject power
loss, device failure, or failed synchronization after admission succeeds. The
library propagates storage errors and requires callers to discard failed
transactions. Synchronization still depends on the filesystem/device contract.

Delivery remains at least once, retries can synchronize, and an uncooperative
application callback cannot be terminated by the library. The Kafka tests use
real brokers but do not certify replicated availability. Select and verify
deployment-specific durability, ordering, deduplication, and outage budgets
using [tuning](TUNING.md), [admission](ADMISSION.md), and [quarantine](QUARANTINE.md).
