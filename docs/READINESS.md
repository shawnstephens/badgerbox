# Production-readiness evidence and remaining work

This document tracks the scope of the production-readiness audit. Passing tests
prove the behaviors they exercise; they do not constitute a blanket production
certification. The next changes must close the remaining resource-control and
operational gaps below before that broader claim is made.

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

Run `just check` for both modules and `just test-integration` with Docker.
Integration tests require real dependencies and fail when setup fails. The
benchmark's local sink and independently consumed Kafka mode must be identified
separately in reports. Preserve failures as well as successful runs.

## Remaining production work

1. **Hard resource admission.** Core claims are record-count bounded but have no
   configurable encoded-byte budget. An unusually large batch or expanding
   application codec can exceed an intended memory budget even when each record
   is admissible to Badger. Add and test a byte-aware claim budget, including the
   behavior for a single record larger than the requested budget.
2. **Backlog and disk exhaustion policy.** There is no atomic queue-count or
   backlog-byte quota and no built-in free-space admission policy. Specify how
   producers receive backpressure while workers retain enough space to settle
   and reclaim existing work. Test simultaneous producers and abrupt capacity
   changes without dropped or falsely acknowledged messages.
3. **Operational handling of poison records.** A codec decode failure currently
   stops the queue. Define a bounded, inspectable quarantine/recovery path that
   distinguishes application decode failures from corrupt storage metadata.
   Operators need a way to recover healthy work without deleting undelivered data.
4. **Sustained churn and failure budgets.** Run longer mixed-size load/outage
   tests through repeated value-log rotations and GC, under explicit CPU, RSS
   and disk limits. Measure the drain margin while intake continues. Short
   finite benchmarks alone cannot prove a stable disk/RSS plateau or an outage
   SLA. Include retry exhaustion, synchronized retry traffic, full disks and
   slow/failed synchronization in fault testing.

The tuning guide documents current limits and application responsibilities.
Documentation is not a substitute for the missing controls or their validation.
