"""Writes experiment files and collects reproducibility metadata.

JSON snapshots use atomic replacement; event and sample logs are incremental.
Hardware inspection and SHA-256 fingerprints happen outside timed queries.
This module performs no Git operations and does not calculate statistics.
"""

import csv
import hashlib
import json
import os
import platform
import subprocess
from pathlib import Path
from typing import TextIO

from parallel_bench import models

SAMPLE_FIELDS = (
    "repetition",
    "condition",
    "warmup",
    "slot",
    "label",
    "status",
    "error",
    "arrival_ns",
    "release_ns",
    "start_ns",
    "finish_ns",
    "completion_s",
    "execute_fetch_s",
    "rows",
    "thread_id",
)
PAIRED_FIELDS = (
    "repetition",
    "slot",
    "label",
    "isolated_latency_s",
    "concurrent_completion_s",
    "slowdown",
)


def write_json(path: Path, value: object) -> None:
    """Atomically replaces a JSON snapshot with finite, UTF-8 data."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def sha256_file(path: Path) -> str:
    """Fingerprints file content without loading a large extension into RAM."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _gpu_info() -> models.JsonObject:
    """Queries GPU identity, retaining driver errors as diagnostic metadata."""
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,name,uuid,memory.total,driver_version",
                "--format=csv",
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }
    except (OSError, subprocess.TimeoutExpired) as exc:
        return {"unavailable": str(exc)}


def hardware_info() -> models.JsonObject:
    """Collects CPU, memory, affinity and GPU information for this process."""
    result: models.JsonObject = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "cpu_model": platform.processor(),
    }
    if hasattr(os, "sched_getaffinity"):
        result["cpu_affinity"] = sorted(os.sched_getaffinity(0))
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                result["cpu_model"] = line.partition(":")[2].strip()
                break
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        result["host_mem_total"] = meminfo.read_text(
            encoding="utf-8"
        ).splitlines()[0]
    result["nvidia_smi"] = _gpu_info()
    return result


def write_paired_latencies(
    path: Path, rows: list[models.PairedLatency]
) -> None:
    """Exports one explicit isolated/concurrent latency pair per query trial."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PAIRED_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


class ArtifactLog:
    """Flushes completed batches and events so earlier work survives a crash."""

    def __init__(self, output: Path) -> None:
        """Opens the event and sample logs for a new run directory."""
        self.events: TextIO = (output / "events.jsonl").open(
            "w", buffering=1, encoding="utf-8"
        )
        try:
            self.samples: TextIO = (output / "samples.csv").open(
                "w", newline="", encoding="utf-8"
            )
        except BaseException:
            self.events.close()
            raise
        self.writer = csv.DictWriter(
            self.samples, fieldnames=SAMPLE_FIELDS, extrasaction="ignore"
        )
        self.writer.writeheader()
        self.samples.flush()

    def event(self, value: models.JsonObject) -> None:
        """Appends one JSON event immediately."""
        self.events.write(json.dumps(value, allow_nan=False) + "\n")

    def batch(self, batch: models.Batch) -> None:
        """Appends all observations in a finished trial and flushes the CSV."""
        for record in batch["records"]:
            self.writer.writerow({**batch, **record})
        self.samples.flush()

    def failure(
        self,
        phase: models.JsonObject,
        status: models.RunStatus,
        error: str | None,
    ) -> None:
        """Records a terminated trial without inventing missing timings."""
        for slot in phase.get("slots", []):
            self.writer.writerow(
                {
                    "repetition": phase["repetition"],
                    "condition": phase["name"],
                    "warmup": phase["warmup"],
                    "slot": slot,
                    "status": status,
                    "error": error,
                }
            )
        self.samples.flush()

    def close(self) -> None:
        """Closes both logs after terminal status has been written."""
        self.samples.close()
        self.events.close()
