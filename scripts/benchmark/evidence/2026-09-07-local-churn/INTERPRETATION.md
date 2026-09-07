# Three-minute local churn experiment

This run verifies repeated normal value-log reclamation during sustained intake.
It does **not** establish a long-term disk plateau or replace Kafka, crash, or
filesystem-specific capacity testing.

Source revision: `36682ed7cc7785f252bbae1af0d0193f2a7164bc` (clean build).
Started: `2026-09-07T05:02:52.410267Z`. Raw report schema: **2**.
The exact command and binary SHA-256 are in [manifest.json](manifest.json).
All values below come from [report.json](report.json), its labeled counters, and
the timestamped [timeline.csv](timeline.csv).

The local verified sink accepted and independently verified **90,000** unique
16 KiB payloads, with zero duplicates or corrupt values. Offered rate was 500/s;
measured delivery rate was **499.96/s** over **180.013 seconds**. The subsequent
30-second maintenance observation is excluded from this throughput. The final
snapshot and full audit showed an empty, consistent queue. Measurement sources
reported no errors.

The experiment used synchronous Badger writes, `GOMAXPROCS=4`, two enqueue and
processor workers, 4 MiB memtables × 2, two compactors, 8 MiB value-log files,
a 1 KiB value threshold, 8 MiB block cache, 4 MiB index cache and GC every second
with discard ratio 0.5 (at most eight GC calls per tick). These settings accelerate
rotation/compaction for this experiment; they are not a universal default profile.
No manual GC, forced Flatten, `runtime.GC`, database reopen, or external disk load
was introduced.

Normal maintenance completed **601 successful value-log rewrites**, **140
no-rewrite outcomes**, and **zero GC errors**. The largest observed decrease
between sampled value-log footprints was **104.2 MiB**. The peak observed vlog
footprint was **512.9 MiB**, and the final pre-close value-log footprint was
**256.4 MiB**. These are apparent file sizes, including preallocation; they are
not exact reclaimed byte counts. File creation and deletion can occur between
samples, and deleted-but-open files are not included.

| Elapsed window | Retained points | RSS range (MiB) | Vlog range (MiB) | Cumulative rewrites, first → last |
| --- | ---: | ---: | ---: | ---: |
| 0–60 s | 56 | 24.48–81.03 | 16.00–496.87 | 0 → 176 |
| 60–120 s | 56 | 72.16–83.16 | 232.39–504.88 | 180 → 360 |
| 120–180 s | 56 | 63.59–82.23 | 232.70–512.90 | 360 → 601 |
| 180–210.04 s | 29 | 81.14–81.48 | 256.44–256.44 | 601 → 601 |

RSS peaked at **84.0 MiB** across the finer 100 ms resource samples. During the
latter half of delivery it ranged from **63.6 to 83.2 MiB**, with a descriptive
least-squares slope of **−56.9 KiB/s**. This supports stable memory use over the
observed interval, not an application-independent memory bound. Heap peaked at
34.4 MiB; the process allocated 40.4 GiB cumulatively and spent 0.260 seconds in
reported Go GC pauses. The process, including verification and measurement,
consumed 188.6 CPU seconds over the 210.03-second resource interval.

The value log repeatedly shrank during intake, but its latter-half fitted slope
was still **+479.2 KiB/s**, and the per-minute sampled peaks increased slightly.
The successful rewrite count proves that GC was exercised; the data does **not**
prove a disk plateau. During the idle observation, no more rewrites occurred and
256.4 MiB of apparent vlog files remained despite an empty logical queue. Badger
requires compaction-derived discard statistics and excludes the current writable
log; draining delivery alone does not promise immediate physical reclamation.
The post-close total directory size was 250.0 MiB, which must not be compared
directly with the pre-close value-log series: close can truncate files.

The p99 delivery-latency upper bound was 4.273 ms (maximum 88.489 ms). This is an
offered-rate experiment with a local verified sink, not a saturation capacity
claim or a Kafka latency result. Repeat longer runs on the deployment filesystem,
with its real payload distribution and the final integrated quota/admission
configuration, before drawing production sizing conclusions.
