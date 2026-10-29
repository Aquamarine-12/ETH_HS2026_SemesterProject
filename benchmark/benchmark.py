"""Measures concurrent streams of the 22 TPC-H queries using DuckDB.

Generate tables once, then benchmark N independently shuffled query streams.
Each query is also timed alone to provide the baseline for its slowdown.
Results include query latencies, overall QPS, and mean stream slowdown.
"""

import argparse
import json
import math
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb


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


def load_queries(connection: duckdb.DuckDBPyConnection) -> dict[int, str]:
    """Reads the 22 fixed query instances supplied by DuckDB's extension."""
    connection.execute("LOAD tpch")
    return dict(
        connection.execute(
            "SELECT query_nr, query FROM tpch_queries() ORDER BY query_nr"
        ).fetchall()
    )


def make_streams(
    query_ids: list[int], count: int, seed: int
) -> list[list[int]]:
    # TODO: put here an option for an identical permutation, so without randomness. maybe use seed: int | None = None.
    """Creates one reproducible permutation per connection."""
    rng = random.Random(seed)
    return [rng.sample(query_ids, len(query_ids)) for _ in range(count)]


def run_stream(
    connection: duckdb.DuckDBPyConnection,
    queries: dict[int, str],
    order: list[int],
    barrier: threading.Barrier | None = None,
) -> tuple[dict[int, float], float]:
    """Returns per-query seconds and the stream's completion timestamp."""
    if barrier is not None:
        barrier.wait()
    timings = {}
    for query_id in order:
        start = time.perf_counter()
        # Fetch the entire answer before submitting the next query.
        connection.execute(queries[query_id]).fetchall()
        timings[query_id] = time.perf_counter() - start
    return timings, time.perf_counter()


def run_concurrent(
    root: duckdb.DuckDBPyConnection,
    queries: dict[int, str],
    orders: list[list[int]],
) -> tuple[list[dict[int, float]], float]:
    """Runs streams together and measures release-to-last-completion time."""
    started_at = 0.0

    def mark_start() -> None:
        """Records the start while the barrier still holds every worker."""
        nonlocal started_at
        started_at = time.perf_counter()

    barrier = threading.Barrier(len(orders), action=mark_start)
    # Each cursor is an independent connection to the same database instance.
    connections = [root.cursor() for _ in orders]

    # TODO: remove the try block, if it fails then the whole benchmark should fail, otherwise the results are kinda useless
    try:
        with ThreadPoolExecutor(max_workers=len(orders)) as pool:
            futures = [
                pool.submit(run_stream, connection, queries, order, barrier)
                for connection, order in zip(connections, orders)
            ]
            results = [future.result() for future in futures]
    finally:
        for connection in connections:
            connection.close()
    elapsed = max(finished_at for _, finished_at in results) - started_at
    return [timings for timings, _ in results], elapsed


def calculate_metrics(
    isolated: dict[int, float],
    concurrent: list[dict[int, float]],
    elapsed: float,
) -> dict[str, float]:
    """Calculates QPS and summed query slowdowns averaged over streams."""
    total_queries = sum(len(stream) for stream in concurrent)
    total_slowdown = sum(
        latency / isolated[query_id]
        for stream in concurrent
        for query_id, latency in stream.items()
    )
    return {
        "qps": total_queries / elapsed,
        "aggregate_slowdown": total_slowdown / len(concurrent),
    }


def benchmark(
    database: Path, connections: int, seed: int, output: Path
) -> None:
    """Measures isolated baselines and concurrent streams, then saves JSON."""
    with duckdb.connect(str(database), read_only=True) as root:
        queries = load_queries(root)
        orders = make_streams(list(queries), connections, seed)
        print("Measuring isolated query latencies...")
        isolated, _ = run_stream(root, queries, list(queries))
        print(f"Running {connections} concurrent streams...")
        concurrent, elapsed = run_concurrent(root, queries, orders)
    metrics = calculate_metrics(isolated, concurrent, elapsed)
    results = {
        "connections": connections,
        "seed": seed,
        "total_queries": sum(len(stream) for stream in concurrent),
        "elapsed_s": elapsed,
        **metrics,
        "isolated_latencies_s": isolated,
        "streams": [
            {
                "stream_id": stream_id,
                "query_order": order,
                "query_latencies_s": timings,
            }
            for stream_id, (order, timings) in enumerate(
                zip(orders, concurrent)
            )
        ],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    print(f"QPS: {metrics['qps']:.3f}")
    print(f"Aggregate slowdown: {metrics['aggregate_slowdown']:.3f}")
    print(f"Results: {output}")


def main() -> None:
    """Parses the two commands and calls their implementation directly."""
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
    run.add_argument("--seed", type=int, default=0)
    run.add_argument(
        "--output", type=Path, default=Path("results/results.json")
    )
    args = parser.parse_args()
    if args.command == "generate-table":
        if not math.isfinite(args.scale_factor) or args.scale_factor <= 0:
            parser.error("--scale-factor must be a finite positive number")
        generate_tables(args.db, args.scale_factor)
    else:
        if args.connections < 1:
            parser.error("--connections must be at least 1")
        benchmark(args.db, args.connections, args.seed, args.output)


if __name__ == "__main__":
    main()
