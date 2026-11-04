"""Reads reference measurements and writes portable benchmark JSON.

The execution core returns dataclasses; this module defines their file shape.
References in result and index files are relative to the containing file.
Plotting can import the readers without importing DuckDB or running queries.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from core import BenchmarkResult, QueryLatencies, RunSettings

JsonObject: TypeAlias = dict[str, Any]


def read_json(path: Path) -> JsonObject:
    """Reads a JSON object, allowing missing or malformed files to fail."""
    return json.loads(path.read_text(encoding="utf-8"))


def encode_path(value: object) -> str:
    """Converts filesystem paths while rejecting unsupported JSON values."""
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Cannot encode {type(value).__name__} as JSON")


def write_json(path: Path, document: JsonObject) -> None:
    """Writes an indented JSON document."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(document, indent=2, default=encode_path, allow_nan=False)
        + "\n",
        encoding="utf-8",
    )


def create_result_directory(root: Path, prefix: str) -> Path:
    """Creates a new directory without overwriting a previous experiment."""
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    path = root / f"{prefix}-{timestamp}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def relative_path(path: Path | None, directory: Path) -> str | None:
    """Expresses a reference relative to its containing JSON directory."""
    if path is None:
        return None
    return os.path.relpath(path.resolve(), directory.resolve())


def load_baseline(path: Path) -> QueryLatencies:
    """Reads per-query latencies from a separate single-stream reference."""
    result = read_json(path)
    if result["connections"] != 1:
        raise ValueError(f"Expected an N=1 baseline: {path}")
    return {
        int(query_id): latency
        for query_id, latency in result["streams"][0][
            "query_latencies_s"
        ].items()
    }


def save_settings(path: Path, settings: RunSettings) -> None:
    """Records the effective single-run settings."""
    write_json(path, asdict(settings))


def result_document(result: BenchmarkResult, output: Path) -> JsonObject:
    """Converts the core result into the public per-run JSON format."""
    settings = result.settings
    document = {
        "device": settings.device,
        "database": str(settings.db.resolve()),
        "sample": settings.sample,
        "duckdb_version": result.duckdb_version,
        "cpu_fallback": result.cpu_fallback,
        "process_id": result.process_id,
        "connections": settings.connections,
        "seed": settings.seed,
        "role": "baseline"
        if settings.connections == 1 and settings.baseline_json is None
        else "measurement",
        "baseline_json": relative_path(settings.baseline_json, output.parent),
        "baseline_latencies_s": result.baseline_latencies_s,
        "total_queries": sum(
            len(stream.latencies) for stream in result.streams
        ),
        "elapsed_s": result.elapsed_s,
        "qps": result.qps,
        "streams": [
            {
                "stream_id": index,
                "query_order": order,
                "query_latencies_s": stream.latencies,
            }
            for index, (order, stream) in enumerate(
                zip(result.orders, result.streams)
            )
        ],
    }
    if result.mean_slowdown is not None:
        document["mean_slowdown"] = result.mean_slowdown
    return document


def save_result(output: Path, result: BenchmarkResult) -> None:
    """Serializes a successful measured workload."""
    write_json(output, result_document(result, output))
