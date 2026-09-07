# Implementation and review order

The modernization is a linear stack. Each branch builds on the preceding branch; review and merge from the bottom upward. Every public library package lives under `pkg`; the executable remains a separate module under `cmd`.

| Layer | Branch | Change | Primary validation |
| --- | --- | --- | --- |
| 1 | `modernize/01-layout` | Public package layout and demo module paths | Both module builds/tests |
| 2 | `modernize/02-validation` | Dependency refresh, Justfile, lint and CI | Race tests and lint in both modules |
| 3 | `modernize/03-records` | Versioned records, opaque injected codecs, strict contexts | Binary bytes, independent defaults, incompatible-format rejection |
| 4 | `modernize/04-snapshots` | Index-derived snapshots; remove counters and repair command | Lifecycle transitions and index/row benchmark |
| 5 | `modernize/05-bounded-recovery` | Adaptive claim size and paged expired-lease recovery | Actual transaction-size limit and page boundary tests |
| 6 | `modernize/06-batch-settlement` | Per-message batch results and detached settlement | Mixed results, errors, panic, cancellation, duplicates and missing results |
| 7 | `modernize/07-worker-lifecycle` | Reserve capacity before claims; join callbacks | Single-message claims, undispatched release, message trace propagation and callback shutdown |
| 8 | `modernize/08-kafka-delivery` | Asynchronous Kafka adapter and partition control | Late callbacks, byte ownership and real-broker delivery |
| 9 | `modernize/09-audit` | Read-only index/row reconciliation | Corrupt indexes, shared key/byte budgets, incomplete reports and nonmutation |
| 10 | `modernize/10-dead-letters` | Byte-bounded pages and exact timestamp requeue | Oversized metadata pagination, bounded exact requeue and binary round trips |
| 11 | `modernize/11-admin` | Generic bounded HTTP routes | Validation, cursor isolation, timeout and admission through flush |
| 12 | `modernize/12-maintenance` | Explicit database maintenance ownership | Validation and joining an in-flight maintenance call |
| 13 | `modernize/13-telemetry` | Separate native queue/database/delivery instrumentation | Provider injection, counter resets, collector exclusivity and lifecycle |
| 14 | `modernize/14-runner` | Shared database runner with typed registration | Mixed types/codecs, duplicate names and safe shutdown timeout |
| 15 | `modernize/15-demo` | Runner-backed demo, admin and modern OTLP transports | Async scheduling, HTTP/gRPC export and graceful demo smoke test |
| 16 | `modernize/16-validation-docs` | Process recovery, executable examples and documentation | SIGTERM/SIGKILL replay, all module checks and integration suite |

The storage format intentionally rejects incompatible existing data; no automatic conversion is provided. Applications retain codec ownership and must use compatible codecs when reopening a namespace. Delivery remains at least once, including the acceptance-before-acknowledgement crash window.

Validation is scoped: local unit/race tests, real-broker integration, and local process recovery establish the tested behavior. Throughput and memory figures are benchmarks, not production capacity guarantees. The CI run associated with each PR is a separate validation result.
