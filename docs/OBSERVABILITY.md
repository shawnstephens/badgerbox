# Observability

Badgerbox uses native OpenTelemetry metrics and tracing. The public `telemetry.Options` accepts meter and tracer providers, scope names, a text-map propagator, and a snapshot interval. `badgerbox.Options.Observability` accepts the same options. Providers are caller-owned and may use any compatible exporter.

The library does not configure global providers. Missing providers disable the corresponding export. Trace context and baggage are persisted in the record envelope and extracted when processing a message after restart. Application payload encoding remains controlled by the injected codec.

## Ownership

Constructing a store registers instruments but starts no polling goroutine. `StartObservability` starts polling once; processors start it automatically. The poller is store-owned and survives cancellation of the context that started it. `Store.Close` cancels and joins it and unregisters its callbacks. Stop intake and join processors before closing stores.

`runner.Open` registers database and maintenance observers when a meter provider is supplied. One process-wide database collector is permitted because Badger's compaction statistics are process-global. Its callback must be closed before the database or the next collector is created. Directory-size values are refreshed by Badger, typically once per minute; they are not filesystem scans on every queue tick.

## Queue metrics

Names are fixed across namespaces. Namespace, outcome, mode, and failure are bounded attributes rather than parts of instrument names. Message IDs are span attributes and never metric labels.

The existing `badgerbox_enqueue_duration_seconds_max` and `badgerbox_process_duration_seconds_max` gauges use `telemetry.Options.DurationMaxWindow` (zero selects one minute; negative values are invalid). Each queue retains maxima from its current and immediately previous fixed window and exports the larger value per attribute set. Windows advance with time, not collection: multiple readers and repeated collections do not consume observations. Samples remain eligible for between one and two window lengths; older data expires and idle series stop producing points. Configure the window at least as long as the longest reader collection interval. Storage holds at most two maxima per attribute set, independent of event rate. Runner queue options inherit this window when zero, and explicit queue values override the runner setting. These are window maxima, not exact rolling five-minute maxima or lifetime high-water marks.

| Instruments | Meaning |
| --- | --- |
| `badgerbox_enqueue_total`, `badgerbox_enqueue_duration_seconds` | Prepared/committed enqueue operations and latency |
| `badgerbox_claim_total`, `badgerbox_claim_batch_size` | Claimed messages and batch sizes |
| `badgerbox_claim_transaction_too_big_total`, `badgerbox_claim_retry_size` | Claim transaction reductions |
| `badgerbox_process_attempt_total`, `badgerbox_process_duration_seconds` | Per-message settlement outcomes and duration |
| `badgerbox_process_batch_total`, `badgerbox_process_batch_size`, `badgerbox_process_batch_duration_seconds` | Batch execution |
| `badgerbox_batch_result_missing_total`, `badgerbox_batch_result_invalid_total` | Missing, duplicate, and unknown result diagnostics |
| `badgerbox_dead_letter_total`, `badgerbox_requeue_total`, `badgerbox_retry_delay_seconds` | Failure disposition and rescheduling |
| `badgerbox_conflict_retry_total` | Badger transaction conflicts |
| `badgerbox_schedule_lag_seconds`, `badgerbox_message_age_seconds` | Dispatch lag and message age |
| `badgerbox_queue_ready`, `badgerbox_queue_processing`, `badgerbox_queue_dead_letter` | Index-derived queue depths |
| `badgerbox_queue_oldest_ready_age_seconds`, `badgerbox_queue_oldest_processing_age_seconds` | Oldest created-time index ages |
| `badgerbox_workers_active`, `badgerbox_work_channel_depth` | Active batch workers and queued messages |
| `badgerbox_snapshot_duration_seconds`, `badgerbox_snapshot_error_total` | Polling cost and failures |
| `badgerbox_kafka_produce_total`, `badgerbox_kafka_produce_error_total`, `badgerbox_kafka_promise_duration_seconds` | Kafka scheduling and asynchronous callback outcomes |

Enqueue and processing duration maxima use `_max` gauges over the current and previous time windows; collection does not reset them. Index snapshots scan keys without loading payload records. They are O(N), so set the polling interval to suit queue scale. Use explicit audits for row/index consistency checks.

`telemetry.NewDeliveryObserver` records `badgerbox_delivery_flush_total`, `badgerbox_delivery_flush_error_total`, and `badgerbox_delivery_flush_duration_seconds`, with one configured `delivery` attribute. Record shared-client flushes once rather than once per queue using the client.

## Database metrics

Database gauges cover `badgerbox_badger_lsm_size_bytes`, `badgerbox_badger_vlog_size_bytes`, `badgerbox_badger_total_size_bytes`, disk total/available bytes, pending memtable writes, and active compaction tables. `badgerbox_badger_compaction_written_bytes` is a reset-safe monotonic counter by level. Read failures increment `badgerbox_badger_collection_errors` with a bounded source attribute.

Maintenance counters cover attempts, outcomes, and successful rewrites; the duration histogram is `badgerbox_badger_maintenance_duration_seconds`. Attributes identify `flatten` or `value_log_gc` and `success`, `no_rewrite`, or `error`. Badger's `ErrNoRewrite` is a normal outcome, not an error or successful rewrite.

Prometheus exporters may add counter suffixes such as `_total`. Dashboard queries use the exported names.

## Local stack

From the repository root:

```sh
docker compose -p badgerbox-observability -f deployments/observability/docker-compose.yml up -d
```

Use the same project name and file when stopping this stack. The deployment binds configurable loopback ports:

| Service | Default URL/endpoint | Override |
| --- | --- | --- |
| Grafana | http://localhost:33000 | `BADGERBOX_GRAFANA_PORT` |
| Prometheus | http://localhost:39090 | `BADGERBOX_PROMETHEUS_PORT` |
| Tempo | http://localhost:33200 | `BADGERBOX_TEMPO_PORT` |
| OTLP HTTP | localhost:34318 | `BADGERBOX_OTLP_HTTP_PORT` |
| OTLP gRPC | localhost:34317 | `BADGERBOX_OTLP_GRPC_PORT` |
| Collector metrics | http://localhost:39464/metrics | `BADGERBOX_COLLECTOR_METRICS_PORT` |

The local Grafana login is `admin` / `admin`. The provisioned dashboard includes queue depth, latency percentiles, batch/callback latency, claim reductions, snapshot failures, disk availability, compaction, and flush outcomes.

## Export from the demo

```sh
cd cmd/badgerbox-demo
GOWORK=off go run . producer --logging-producer \
  --otel-endpoint localhost:34318 --otel-insecure
```

For gRPC, add `--otel-protocol grpc --otel-endpoint localhost:34317`. The protocol defaults to `http/protobuf`. Endpoint URLs are accepted. TLS remains the exporter default unless an insecure URL or `--otel-insecure` explicitly selects plaintext. The standard OTLP endpoint/protocol environment variables are also accepted by the demo CLI; provider setup respects exporter environment configuration.

The optional expvar listener remains available for local Go runtime diagnostics. Queue/database telemetry uses the injected OpenTelemetry providers and does not require that listener. Shut down the runner before shutting down providers so final settlement and flush observations can be exported.
