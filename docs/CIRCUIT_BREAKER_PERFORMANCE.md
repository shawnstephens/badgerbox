# Circuit-breaker recovery measurements

These measurements compare the recovery policy and healthy path implemented in PR #23. They are local workload observations, not production capacity or device-IO guarantees.

## Healthy backlog drain

Measured on Apple M4, macOS arm64, with five runs per configuration and no concurrent test or lint jobs. Each run pre-enqueues 4,096 1 KiB payloads, then drains them through the real Badger store, batch dispatcher, workers, and acknowledgement path. The producer succeeds immediately without network IO. Claim batches contain 16 messages; polling and completion snapshots use 1 ms intervals. Badger uses its default disk-backed options. Enqueue and setup are outside the timed region; final acknowledgement is included.

Latency is measured from processor start to delivery callback entry for each message, so it includes backlog residence. It is not per-request network latency. Table values are medians across five runs; the throughput range includes every run, including the slower first run.

| Workers | Breaker enabled | Messages/s, median | Messages/s, range | p50 backlog latency, ms | p99 backlog latency, ms |
| --- | --- | --- | --- | --- | --- |
| 1 | false | 17,782 | 12,490–17,840 | 111.2 | 226.1 |
| 1 | true | 17,838 | 17,278–17,886 | 109.5 | 225.0 |
| 4 | false | 20,584 | 19,337–21,346 | 102.1 | 191.8 |
| 4 | true | 18,820 | 18,159–19,810 | 105.4 | 207.9 |

At one worker, median throughput was effectively unchanged (+0.3%). At four workers, enabling the breaker reduced median throughput by 8.6%, and median p99 backlog latency increased from 191.8 ms to 207.9 ms (+8.4%). The healthy path pays for breaker locking, generation checks, and rate-limiter admission; this experiment does not isolate those costs or establish statistical significance. Real producer latency, batch size, concurrency, Badger settings, snapshots, and storage behavior can change the result. These numbers do not justify claiming the breaker is free on healthy traffic.

Reproduce without running other tests concurrently:

```sh
GOWORK=off go test ./pkg/badgerbox -run '^$' \
  -bench '^BenchmarkCircuitHealthyDrain$' -benchtime=4096x -count=5
```

## Outage delivery work

`TestCircuitReducesOutageWrites` compares a fixed backlog of 16 messages over 60 simulated seconds, advancing in 100 ms steps. Every delivery immediately returns unavailable. Normal retries use a fixed one-second delay and a high attempt limit. The previous policy uses exact exponential delays starting at five seconds and capped at one minute; the new policy fixes its private random sample at the midpoint, yielding 4.5-second probes. The initial normal batch is allowed to settle, so totals include the initial failure-detection window.

| Policy | Claimed records / producer calls | Badger user-write bytes |
| --- | --- | --- |
| Breaker disabled | 960 | 1,130,574 |
| Previous exponential policy | 19 | 22,335 |
| New fixed policy, midpoint jitter | 29 | 34,281 |

The new policy made ten more deliveries (+52.6%) and wrote about 53.5% more user bytes than the previous exponential policy. It still reduced user-write bytes by about 97.0% relative to disabled recovery in this workload. The earlier approximately 98% reduction is not the result for the new defaults.

This is Badger's `badger_write_bytes_user` counter, not physical disk traffic or total application IO. There are no concurrent enqueues, lease reaping, snapshots, or maintenance in this simulation. Payloads, initial in-flight work, jitter samples, publish durations, and outage length affect the totals. Fixed intervals improve detection latency by spending more outage IO; the 4–5 second interval only bounds breaker admission waits, not end-to-end recovery.

```sh
GOWORK=off go test ./pkg/badgerbox \
  -run '^TestCircuitReducesOutageWrites$' -count=1 -v
```

See [configuration, timing limits, and the delivery diagram](CIRCUIT_BREAKER.md).
