#!/usr/bin/env python3
"""Run independent, sequential demo processes and retain raw evidence plus CSV.

Build first in cmd/badgerbox-demo: GOWORK=off go build -o /tmp/badgerbox-demo .
Run: python3 scripts/benchmark/matrix.py --binary /tmp/badgerbox-demo --output-dir /tmp/badgerbox-results
Use --brokers localhost:9092 for an independently consumed Kafka delivery run.
"""

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--brokers", default="")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--quick", action="store_true", help="Smoke counts only; unsuitable for capacity sizing")
    parser.add_argument("--keep-data", action="store_true", help="Retain each generated Badger database after reporting")
    parser.add_argument("--timeout", default="10m", help="Per-process startup, intake and drain deadline")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be positive")
    binary = args.binary.resolve(strict=True)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    data = output / "data"
    data.mkdir()
    # These are experiments, not application-independent recommended settings.
    profiles = {
        "small-memory": ["--badger-memtable-size", "16MiB", "--badger-num-memtables", "2", "--badger-block-cache-size", "8MiB", "--badger-index-cache-size", "4MiB", "--badger-value-log-file-size", "16MiB", "--enqueue-parallelism", "2", "--processor-concurrency", "2", "--processor-claim-batch-size", "8"],
        "throughput": ["--badger-memtable-size", "64MiB", "--badger-num-memtables", "5", "--badger-block-cache-size", "64MiB", "--badger-index-cache-size", "16MiB", "--badger-value-log-file-size", "128MiB", "--enqueue-parallelism", "8", "--processor-concurrency", "8", "--processor-claim-batch-size", "64"],
    }
    counts = [(1024, 2000), (65536, 512), (524288, 128)] if args.quick else [(1024, 100000), (65536, 10000), (524288, 2000)]
    rows = []
    failures = 0
    manifest = {"started_at_unix": time.time(), "binary": str(binary), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "quick": args.quick, "repeats": args.repeats, "brokers": args.brokers, "environment": {name: os.environ.get(name, "") for name in ("GOMAXPROCS", "GOMEMLIMIT", "GOGC")}, "commands": []}
    for profile, tuning in profiles.items():
        for payload, messages in counts:
            for repeat in range(1, args.repeats + 1):
                name = f"{profile}-{payload}-r{repeat}"
                report_path = output / f"{name}.json"
                command = [str(binary), "benchmark", "--messages", str(messages), "--payload-bytes", str(payload), "--badger-sync-writes=true", "--badger-value-threshold", "1KiB", "--poll-interval", "5ms", "--retry-base-delay", "10ms", "--retry-max-delay", "100ms", "--db-path", str(data), "--output", str(report_path), "--timeout", args.timeout, *tuning]
                if args.brokers:
                    command.extend(["--brokers", args.brokers])
                manifest["commands"].append(command)
                (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
                print(f"Running {name}", flush=True)
                with (output / f"{name}.log").open("w") as log:
                    result = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=False)
                if not report_path.exists():
                    print(f"No report for {name}; see its log", file=sys.stderr)
                    failures += 1
                    continue
                report = json.loads(report_path.read_text())
                passed = report["passed"] and result.returncode == 0
                failures += not passed
                resource = report["resources"]
                rows.append({"profile": profile, "payload_bytes": payload, "repeat": repeat, "passed": passed, "measurements_complete": resource["measurements_complete"], "accepted": report["accepted"], "unique_delivered": report["unique_delivered"], "duplicates": report["duplicate_deliveries"], "delivery_seconds": report["delivery_seconds"], "messages_per_second": report["delivery_messages_per_second"], "payload_mib_per_second": report["delivery_payload_mib_per_second"], "p99_seconds_upper_bound": report["delivery_latency_seconds"]["p99_upper_bound"], "peak_rss_bytes": resource["sampled_peak_rss_bytes"], "peak_heap_bytes": resource["sampled_peak_heap_bytes"], "peak_apparent_disk_bytes": resource["sampled_peak_apparent_disk_bytes"], "final_apparent_disk_bytes": resource["final_apparent_disk_bytes"], "cpu_seconds": resource["cpu_seconds"], "average_cpu_cores": resource["average_cpu_cores"], "report": report_path.name})
                if not args.keep_data and passed:
                    database = Path(report["db_path"]).resolve()
                    # Only remove the fresh run directory below our own data parent.
                    if database.parent == data and database.name.startswith("badgerbox-benchmark-"):
                        shutil.rmtree(database)
    if rows:
        with (output / "summary.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        medians = []
        for profile in profiles:
            for payload, _ in counts:
                group = [r for r in rows if r["profile"] == profile and r["payload_bytes"] == payload and r["passed"]]
                if group:
                    medians.append({"profile": profile, "payload_bytes": payload, "successful_repeats": len(group), "median_messages_per_second": statistics.median(r["messages_per_second"] for r in group), "median_p99_seconds_upper_bound": statistics.median(r["p99_seconds_upper_bound"] for r in group), "resource_complete_repeats": sum(r["measurements_complete"] for r in group), "max_sampled_peak_rss_bytes": max((r["peak_rss_bytes"] for r in group if r["measurements_complete"]), default=None)})
        (output / "medians.json").write_text(json.dumps(medians, indent=2) + "\n")
    manifest["finished_at_unix"] = time.time()
    manifest["failures"] = failures
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Results: {output}; failed runs: {failures}", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
