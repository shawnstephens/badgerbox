#!/usr/bin/env python3
"""Run one sustained, paced churn experiment and retain raw reclamation evidence."""

import argparse
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import sys
import time


def slope(points, value):
    """Descriptive OLS slope over the observed samples; not a plateau verdict."""
    if len(points) < 2:
        return None
    times = [p["elapsed_seconds"] for p in points]
    values = [value(p) for p in points]
    center_t, center_v = statistics.mean(times), statistics.mean(values)
    denominator = sum((t - center_t) ** 2 for t in times)
    return sum((t - center_t) * (v - center_v) for t, v in zip(times, values)) / denominator if denominator else None


def analyze(report):
    points = report["resource_timeline"]["points"]
    delivery = [p for p in points if p["phase"] == "delivery" and p["measurements_complete"]]
    midpoint = report["delivery_seconds"] / 2
    late = [p for p in delivery if p["elapsed_seconds"] >= midpoint]
    gc = {"success": 0, "no_rewrite": 0, "error": 0}
    for series in report["metric_series"]:
        labels = series["attributes"]
        if series["name"] == "badgerbox_badger_maintenance_outcomes" and labels.get("operation") == "value_log_gc":
            gc[labels["outcome"]] = series["value"]
    vlog = [p["apparent_disk"]["value_log_bytes"] for p in points]
    return {
        "passed": report["passed"],
        "measurements_complete": report["resources"]["measurements_complete"],
        "schema_version": report["schema_version"],
        "started_at": report["started_at"],
        "accepted": report["accepted"],
        "unique_delivered": report["unique_delivered"],
        "duplicates": report["duplicate_deliveries"],
        "delivery_seconds": report["delivery_seconds"],
        "observed_resource_seconds": report["resources"]["elapsed_seconds"],
        "messages_per_second": report["delivery_messages_per_second"],
        "gc_outcomes": gc,
        "peak_observed_value_log_bytes": max(vlog, default=0),
        "final_observed_value_log_bytes_before_close": vlog[-1] if vlog else None,
        "largest_observed_value_log_drop_bytes": max(0, max((a - b for a, b in zip(vlog, vlog[1:])), default=0)),
        "sampled_peak_rss_bytes": report["resources"]["sampled_peak_rss_bytes"],
        "descriptive_trends": {
            "late_delivery_points": len(late),
            "late_delivery_rss_slope_bytes_per_second": slope(late, lambda p: p["rss_bytes"]),
            "late_delivery_value_log_slope_bytes_per_second": slope(late, lambda p: p["apparent_disk"]["value_log_bytes"]),
            "late_delivery_rss_min_bytes": min((p["rss_bytes"] for p in late), default=None),
            "late_delivery_rss_max_bytes": max((p["rss_bytes"] for p in late), default=None),
        },
        "limits": [
            "One local verified-sink experiment includes producer, verifier, sampling and database CPU/RSS; it does not measure Kafka broker behavior.",
            "Normal periodic GC and compaction run without forced Flatten, DB reopen, runtime.GC or manual reclamation.",
            "Sampled apparent file sizes include preallocation and omit deleted-but-open files; changes are not physical I/O or exact bytes reclaimed.",
            "A GC success proves a successful value-log rewrite, not a fixed amount of reclaimed space.",
            "Slopes describe this finite interval; they do not establish a long-term memory or disk bound.",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--duration-seconds", type=int, default=180)
    parser.add_argument("--observe-seconds", type=int, default=30)
    parser.add_argument("--rate", type=float, default=500)
    parser.add_argument("--payload-bytes", type=int, default=16384)
    parser.add_argument("--disable-gc", action="store_true", help="Control experiment: preserve the same workload but disable periodic value-log GC")
    args = parser.parse_args()
    if args.duration_seconds < 1 or args.observe_seconds < 0 or not math.isfinite(args.rate) or args.rate <= 0 or args.payload_bytes < 20:
        parser.error("duration and rate must be positive, observe duration nonnegative, payload at least 20 bytes")
    binary = args.binary.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    data = output / "data"
    data.mkdir()
    command = [
        str(binary), "benchmark", "--messages", str(math.ceil(args.duration_seconds * args.rate)),
        "--payload-bytes", str(args.payload_bytes), "--rate", str(args.rate),
        "--observe-after-drain", f"{args.observe_seconds}s", "--timeout", f"{args.duration_seconds + args.observe_seconds + 120}s",
        "--sample-interval", "100ms", "--timeline-interval", "1s", "--timeline-max-points", "600",
        "--badger-sync-writes=true", "--badger-value-threshold", "1KiB",
        "--badger-memtable-size", "4MiB", "--badger-num-memtables", "2",
        "--badger-num-level-zero-tables", "2", "--badger-num-level-zero-tables-stall", "6",
        "--badger-block-cache-size", "8MiB", "--badger-index-cache-size", "4MiB",
        "--badger-base-table-size", "1MiB", "--badger-value-log-file-size", "8MiB",
        "--badger-num-compactors", "2", "--badger-gc-interval", "0s" if args.disable_gc else "1s",
        "--badger-gc-discard-ratio", "0.5", "--badger-gc-max-runs", "8", "--badger-gc-max-duration", "1s",
        "--enqueue-parallelism", "2", "--processor-concurrency", "2", "--processor-claim-batch-size", "16",
        "--poll-interval", "5ms", "--retry-base-delay", "10ms", "--retry-max-delay", "100ms",
        "--db-path", str(data), "--output", str(output / "report.json"),
    ]
    manifest = {"started_at_unix": time.time(), "command": command, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "disable_gc": args.disable_gc, "environment": {name: os.environ.get(name, "") for name in ("GOMAXPROCS", "GOMEMLIMIT", "GOGC")}}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Running {args.duration_seconds}s paced churn plus {args.observe_seconds}s maintenance observation: {output}", flush=True)
    with (output / "run.log").open("w") as log:
        result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
    manifest.update(finished_at_unix=time.time(), exit_code=result.returncode)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    report_path = output / "report.json"
    if not report_path.exists():
        print(f"No report was emitted (exit {result.returncode}); inspect {output / 'run.log'}", file=sys.stderr)
        return 1
    report = json.loads(report_path.read_text())
    summary = analyze(report)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    with (output / "timeline.csv").open("w", newline="") as stream:
        rows = [{**{key: value for key, value in point.items() if key != "apparent_disk"}, **{f"disk_{key}": value for key, value in point["apparent_disk"].items()}} for point in report["resource_timeline"]["points"]]
        if rows:
            writer = csv.DictWriter(stream, fieldnames=sorted(set().union(*(row.keys() for row in rows))))
            writer.writeheader()
            writer.writerows(rows)
    print(json.dumps(summary, indent=2), flush=True)
    return 0 if result.returncode == 0 and report["passed"] and report["resources"]["measurements_complete"] else 1


if __name__ == "__main__":
    sys.exit(main())
