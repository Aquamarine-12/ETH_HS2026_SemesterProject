"""Measures concurrent TPC-H streams on DuckDB CPU or Sirius GPU execution.

Generate tables once, then benchmark N streams with optional seeded shuffling.
Each query is also timed alone to provide the baseline for its slowdown.
Results include query latencies, overall QPS, and mean stream slowdown.
"""

import argparse
import json
import math
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import TypeAlias

import duckdb

Queries: TypeAlias = dict[int, str]
QueryOrder: TypeAlias = list[int]
QueryLatencies: TypeAlias = dict[int, float]


class Device(StrEnum):
    """Selects the engine used for both isolated and concurrent queries."""

    CPU = "cpu"
    GPU = "gpu"


@dataclass
class StreamResult:
    """Contains a stream's per-query seconds and completion timestamp."""

    latencies: QueryLatencies
    finished_at: float


def generate_tables(database: Path, scale_factor: float) -> None:
    """Generates all TPC-H tables in a new persistent database."""
    if database.exists():
        raise FileExistsError(f"Database already exists: {database}")
    database.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database)) as connection:
        connection.execute("INSTALL tpch")
        connection.execute("LOAD tpch")
        connection.execute(f"CALL dbgen(sf={scale_factor})")
    print(f"Generated TPC-H SF{scale_factor:g} in {database}")


def load_queries(connection: duckdb.DuckDBPyConnection) -> Queries:
    """Reads the 22 fixed query instances supplied by DuckDB's extension."""
    connection.execute("LOAD tpch")
    return dict(
        connection.execute(
            "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
        ).fetchall()
    )


def make_streams(
    query_ids: QueryOrder, count: int, seed: int | None = None
) -> list[QueryOrder]:
    """Copies the input order, or shuffles streams using an explicit seed."""
    if seed is None:
        return [query_ids.copy() for _ in range(count)]
    rng = random.Random(seed)
    return [rng.sample(query_ids, len(query_ids)) for _ in range(count)]


def load_sirius(
    connection: duckdb.DuckDBPyConnection,
    extension: Path,
    connections: int,
    output: Path,
) -> None:
    """Loads Sirius with startup admission for N concurrent queries."""
    if not extension.is_file():
        raise FileNotFoundError(f"Sirius extension does not exist: {extension}")
    config_path = output.with_suffix(".sirius.yaml")
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        f"sirius:\n  max_concurrent_queries: {connections}\n", encoding="utf-8"
    )
    # Sirius reads this startup-only setting when the extension is loaded.
    os.environ["SIRIUS_CONFIG_FILE"] = str(config_path.resolve())
    escaped_path = str(extension.resolve()).replace("'", "''")
    connection.execute(f"LOAD '{escaped_path}'")
    connection.execute("SET gpu_execution = false")


def configure_connection(
    connection: duckdb.DuckDBPyConnection, device: Device
) -> None:
    """Enables GPU execution explicitly and disables CPU fallback."""
    if device == Device.GPU:
        connection.execute("SET enable_duckdb_fallback = true")
        connection.execute("SET gpu_execution = true")


def run_stream(
    connection: duckdb.DuckDBPyConnection,
    queries: Queries,
    order: QueryOrder,
    barrier: threading.Barrier | None = None,
) -> StreamResult:
    """Returns per-query seconds and the stream's completion timestamp."""
    if barrier is not None:
        barrier.wait()
    timings: QueryLatencies = {}
    for query_id in order:
        start = time.perf_counter()
        # Fetch the entire answer before submitting the next query.
        connection.execute(queries[query_id]).fetchall()
        timings[query_id] = time.perf_counter() - start
    return StreamResult(timings, time.perf_counter())


def run_concurrent(
    root: duckdb.DuckDBPyConnection,
    queries: Queries,
    orders: list[QueryOrder],
    device: Device = Device.CPU,
) -> tuple[list[StreamResult], float]:
    """Runs streams together and measures release-to-last-completion time."""
    started_at = 0.0

    def mark_start() -> None:
        """Records the start while the barrier still holds every worker."""
        nonlocal started_at
        started_at = time.perf_counter()

    barrier = threading.Barrier(len(orders), action=mark_start)
    # TODO: what is this?
    with ExitStack() as stack:
        # Each cursor is a connection; the stack closes them even on failure.
        connections = [stack.enter_context(root.cursor()) for _ in orders]
        for connection in connections:
            configure_connection(connection, device)
        with ThreadPoolExecutor(max_workers=len(orders)) as pool:
            futures = [
                pool.submit(run_stream, connection, queries, order, barrier)
                for connection, order in zip(connections, orders)
            ]
            results = [future.result() for future in futures]
    elapsed = max(stream.finished_at for stream in results) - started_at
    return results, elapsed


def calculate_metrics(
    isolated: QueryLatencies,
    concurrent: list[StreamResult],
    elapsed: float,
) -> dict[str, float]:
    """Calculates QPS and summed query slowdowns averaged over streams."""
    total_queries = sum(len(stream.latencies) for stream in concurrent)
    total_slowdown = sum(
        latency / isolated[query_id]
        for stream in concurrent
        for query_id, latency in stream.latencies.items()
    )
    return {
        "qps": total_queries / elapsed,
        "aggregate_slowdown": total_slowdown / len(concurrent),
    }


def save_results(
    output: Path,
    device: Device,
    seed: int | None,
    orders: list[QueryOrder],
    isolated: QueryLatencies,
    concurrent: list[StreamResult],
    elapsed: float,
) -> None:
    """Saves results only after every query succeeds and prints the metrics."""
    metrics = calculate_metrics(isolated, concurrent, elapsed)
    results = {
        "device": device,
        "connections": len(orders),
        "seed": seed,
        "total_queries": sum(len(stream.latencies) for stream in concurrent),
        "elapsed_s": elapsed,
        **metrics,
        "isolated_latencies_s": isolated,
        "streams": [
            {
                "stream_id": stream_id,
                "query_order": order,
                "query_latencies_s": stream.latencies,
            }
            for stream_id, (order, stream) in enumerate(
                zip(orders, concurrent)
            )
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"QPS: {metrics['qps']:.3f}")
    print(f"Aggregate slowdown: {metrics['aggregate_slowdown']:.3f}")
    print(f"Results: {output}")


def benchmark(
    database: Path,
    connections: int,
    seed: int | None,
    output: Path,
    device: Device = Device.CPU,
    sirius_extension: Path | None = None,
) -> None:
    """Measures isolated and concurrent queries using the selected device."""
    if device == Device.GPU and sirius_extension is None:
        raise ValueError("GPU execution requires --sirius-extension")
    options = (
        {"allow_unsigned_extensions": "true"} if device == Device.GPU else {}
    )
    with duckdb.connect(str(database), read_only=True, config=options) as root:
        if device == Device.GPU:
            assert sirius_extension is not None
            load_sirius(root, sirius_extension, connections, output)
        queries = load_queries(root)
        orders = make_streams(list(queries), connections, seed)
        configure_connection(root, device)
        print(f"Measuring isolated {device} query latencies...")
        isolated = run_stream(root, queries, list(queries))
        print(f"Running {connections} concurrent {device} streams...")
        concurrent, elapsed = run_concurrent(root, queries, orders, device)
    save_results(
        output, device, seed, orders, isolated.latencies, concurrent, elapsed
    )


def parse_arguments() -> argparse.Namespace:
    """Parses the two commands and checks their required arguments."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser(
        "generate-table", help="Generate TPC-H tables"
    )
    generate.add_argument("--db", type=Path, default=Path("data/tpch.duckdb"))
    generate.add_argument("--scale-factor", type=float, default=1.0)
    run = commands.add_parser("benchmark", help="Measure N concurrent streams")
    run.add_argument("--db", type=Path, default=Path("data/tpch.duckdb"))
    run.add_argument("--connections", type=int, default=1)
    run.add_argument("--seed", type=int, help="Shuffle streams reproducibly")
    run.add_argument(
        "--device", type=Device, choices=list(Device), default=Device.CPU
    )
    run.add_argument(
        "--sirius-extension", type=Path, help="Local Sirius binary"
    )
    run.add_argument(
        "--output", type=Path, default=Path("results/results.json")
    )
    args = parser.parse_args()
    if args.command == "generate-table":
        if not math.isfinite(args.scale_factor) or args.scale_factor <= 0:
            parser.error("--scale-factor must be a finite positive number")
    else:
        if args.connections < 1:
            parser.error("--connections must be at least 1")
        if args.device == Device.GPU and args.sirius_extension is None:
            parser.error("--device gpu requires --sirius-extension")
    return args


def main() -> None:
    """Calls table generation or the benchmark directly."""
    args = parse_arguments()
    if args.command == "generate-table":
        generate_tables(args.db, args.scale_factor)
    else:
        benchmark(
            args.db,
            args.connections,
            args.seed,
            args.output,
            args.device,
            args.sirius_extension,
        )


if __name__ == "__main__":
    main()
