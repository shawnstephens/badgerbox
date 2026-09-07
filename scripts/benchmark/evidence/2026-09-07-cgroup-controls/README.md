# Churn with namespace quotas and Linux cgroup limits

This single run passed independent verification for 90,000 accepted messages at
499.87 messages/s while exercising the 128-message namespace quota and a Linux
memory limit. It ended with an empty queue, zero retained admission usage, and a
consistent audit. It completed 618 value-log rewrites without GC errors. These
are observations from a finite experiment, not a long-term resource guarantee.

The run started at 2026-09-07 05:34:27 UTC from clean source
`a4e70e195dfbd66379a89daecd090e093c5733bc`. Both static binaries used Go 1.26.2,
Linux arm64, and CGO disabled. Docker Desktop used Linux 6.12.76, cgroup v2 and
`overlay2`; the database used the container's writable layer. Only report files
used the host bind mount. The local verified sink sent no network traffic and
used no broker. The small supervisor belongs to the same cgroup.

The workload offered 500 messages/s with 16 KiB values for three minutes, then
observed ordinary maintenance for 30 seconds after delivery drained. Settings:
128 retained messages, 64 MiB retained logical bytes, 2 MiB claim source bytes,
512 MiB free-space margin, `GOMAXPROCS=2`, and `GOMEMLIMIT=96MiB`. Badger used
synchronous writes, 4 MiB memtables (two), two compactors, 8 MiB block / 4 MiB
index caches, 1 MiB base tables, 8 MiB value-log files, 1 KiB value threshold,
and periodic GC every second with ratio 0.5, at most eight rewrites / one second
per maintenance cycle. The complete command is in [manifest.json](manifest.json).

| Measurement | Observed result |
| --- | ---: |
| Accepted / independently verified unique messages | 90,000 / 90,000 |
| Duplicate / corrupt deliveries | 0 / 0 |
| Namespace quota / disk pressure rejection attempts | 37 / 0 |
| Sampled peak retained messages / logical bytes | 128 / 3,761,664 |
| Final retained messages / bytes | 0 / 0 |
| Delivery / total resource observation time | 180.045 / 210.057 s |
| Delivery latency p99 histogram upper bound / maximum | 195.24 / 271.07 ms |
| Sampled process RSS / Go heap peaks | 257.74 / 37.50 MiB |
| Cgroup kernel memory peak | 268,439,552 bytes (256 MiB + 4 KiB) |
| Cgroup memory max events / OOM / OOM kills | 11,447 / 0 / 0 |
| Sampled cgroup anonymous / file memory peaks | 44.16 / 210.05 MiB |
| Cgroup CPU usage / throttled periods | 120.066 s / 0 |
| Value-log GC successful rewrites / no rewrite / errors | 618 / 138 / 0 |
| Timeline apparent value-log peak / final before close | 421.76 / 104.81 MiB |
| Largest adjacent timeline value-log decrease | 100.42 MiB |

The kernel confirmed `cpu.max = 200000 100000`, `memory.max = 268435456`, and
`memory.swap.max = 0`; Docker configuration matched two CPUs, 256 MiB memory,
and no swap. CPU usage averaged about 0.57 cores and did not exercise CPU
throttling. The configured memory limit was exercised: `memory.events.max`
recorded 11,447 attempted crossings. No OOM events or kills occurred, and the
container exited successfully. The kernel permits temporary usage above
`memory.max`, consistent with the observed 4 KiB excess; the events count does
not mean 11,447 OOMs. See the [kernel memory controller documentation](https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#memory).

At the nine-second observation, `memory.current` was 256 MiB; `memory.stat`
reported 40.81 MiB anonymous, 203.55 MiB file, and 7.86 MiB kernel memory. These
fields are separate reads and do not form an exact atomic sum. The independent
sampled anonymous and file maxima in the table occurred at different times.
Process RSS and cgroup memory are separate observations: the measured process
RSS peak was 257.74 MiB. This run does not establish a strict RSS ceiling of
256 MiB or identify the exact cause of the difference between the two accounts.

The message quota bound was reached and rejected attempts subsequently succeeded.
The 64 MiB byte quota and free-space guard did not bind. The claim-byte setting
accepted all legitimate records without quarantine. All accepted messages were
verified, and final audit usage matched persisted accounting at zero.

The GC outcomes and multiple apparent file-size decreases demonstrate actual
rewrites and reclamation activity. The latter half of delivery had descriptive
RSS and value-log slopes of approximately -204 KiB/s and -25 KiB/s; neither is a
plateau verdict. Apparent sizes include preallocation and omit deleted-but-open
files. A rewrite counter does not quantify physical bytes reclaimed. The table
uses timeline observations before database close, because close truncation can
also reduce apparent sizes. `resources.final_apparent_disk_bytes` is the separate
post-close total across file types.

There was no matched uncapped control, Kafka outage, crash/replay phase, or long
soak. The results therefore do not isolate quota overhead, prove failure recovery
under these limits, or establish production capacity on native Linux storage.

[report.json](report.json) and [cgroup.json](cgroup.json) are the raw reports;
[summary.json](summary.json) contains derived measurements. The process
[timeline.csv](timeline.csv) and [cgroup-timeline.csv](cgroup-timeline.csv) make
sample comparisons convenient. The manifest includes exact arguments, binary and
image hashes, verified Docker limits and exit status; [build-info.txt](build-info.txt)
confirms the clean source revision. The process sampled every 100 ms, with a
bounded approximately one-second timeline; cgroup observations were once per
second, plus baseline and final samples. All measurement sources completed.

To reproduce, build the static binaries and scratch image using the
[cgroup guide](../../README.md#linux-cgroup-evidence), then use the arguments in
the manifest with fresh container and output names. Avoid other CPU or disk
workloads on the measurement host. This evidence run retained its test database
inside the stopped container; it is not included in the repository.

The [smoke](smoke/) directory preserves a separate successful four-message
check of source `7321cc7` before the final lint-only attribute conversion change.
It verified cgroup statistics and limits; it is not part of the measured workload
or a performance comparison. There were no failed smoke runs. Both process logs
are empty because the JSON reports were written directly to files.
