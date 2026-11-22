"""Executes TPC-H streams and returns typed measurement results.

One database instance serves independent worker connections in each process.
Workers start together after database-file cache eviction and engine setup.
Optional shuffled warmup streams complete before measured workers start.
Query timers include execute and full fetch; setup and warmup are excluded.
File formats and command-line parsing belong to the surrounding modules.
"""

from __future__ import annotations

import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import TypeAlias

import duckdb

Queries: TypeAlias = dict[int, str]
QueryOrder: TypeAlias = list[int]
QueryLatencies: TypeAlias = dict[int, float]
DEFAULT_SIRIUS_EXTENSION = (
    Path.home() / ".local/lib/sirius/duckdb-2.0.0/sirius.duckdb_extension"
)


class Device(StrEnum):
    """Selects the execution backend."""

    CPU = "cpu"
    GPU = "gpu"


class IoBackend(StrEnum):
    """Selects Sirius's native io_uring or KvikIO/POSIX input path."""

    IO_URING = "io_uring"
    KVIKIO = "kvikio"


class StreamOrder(StrEnum):
    """Names deterministic query stream orders."""

    ASC = "ASC"
    DESC = "DESC"
    ASC_DESC = "ASC-DESC"


StreamSeed: TypeAlias = int | StreamOrder


@dataclass
class RunSettings:
    """Contains the settings for one measured workload."""

    db: Path = Path("data/tpch.duckdb")
    device: Device = Device.CPU
    connections: int = 1
    seed: StreamSeed = StreamOrder.ASC
    warmup: bool = False
    warmup_streams: int = 0  # 0 means no warmup.
    sirius_extension: Path = DEFAULT_SIRIUS_EXTENSION
    io_backend: IoBackend = IoBackend.IO_URING
    baseline_json: Path | None = None
    output: Path | None = None
    results_dir: Path = Path("results")
    sample: int = 1


@dataclass
class StreamResult:
    """Contains measured per-query seconds and a completion timestamp."""

    latencies: QueryLatencies
    finished_at: float


@dataclass
class BenchmarkResult:
    """Carries one measurement from execution to the JSON producer."""

    settings: RunSettings
    orders: list[QueryOrder]
    streams: list[StreamResult]
    elapsed_s: float
    qps: float
    mean_slowdown: float | None
    baseline_latencies_s: QueryLatencies | None
    duckdb_version: str
    process_id: int
    cpu_fallback: bool = False
    warmup_orders: list[QueryOrder] = field(default_factory=list)


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
    query_ids: QueryOrder, count: int, seed: StreamSeed
) -> list[QueryOrder]:
    """Builds ascending, descending, alternating, or seeded query streams."""
    if seed == StreamOrder.ASC:
        return [query_ids.copy() for _ in range(count)]
    if seed == StreamOrder.DESC:
        return [list(reversed(query_ids)) for _ in range(count)]
    if seed == StreamOrder.ASC_DESC:
        return [
            query_ids.copy() if index % 2 == 0 else list(reversed(query_ids))
            for index in range(count)
        ]
    rng = random.Random(seed)
    return [rng.sample(query_ids, len(query_ids)) for _ in range(count)]


def make_warmup_streams(
    query_ids: QueryOrder, settings: RunSettings
) -> list[QueryOrder]:
    """Builds reproducible shuffled warmup orders independent of measured N."""
    seed = settings.seed if isinstance(settings.seed, int) else settings.sample
    # 0 means no warmup.
    count = settings.warmup_streams if settings.warmup else 0
    return make_streams(query_ids, count, seed)


def reset_database_cache(database: Path) -> None:
    """Requests best-effort Linux eviction of the database file's pages."""
    with database.open("rb") as file:
        # Dirty pages must be flushed before DONTNEED can discard them.
        os.fsync(file.fileno())
        os.posix_fadvise(file.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)


def sirius_config_yaml(connections: int, io_backend: IoBackend) -> str:
    """Builds startup YAML with query admission and the selected I/O path."""
    # Older Sirius names its native io_uring backend "sirius"; newer builds
    # retain that name as an alias for "native".
    backend = "sirius" if io_backend == IoBackend.IO_URING else "kvikio"
    config = (
        f"sirius:\n  max_concurrent_queries: {connections}\n"
        "  topology:\n    num_gpus: 1\n"
        f"  executor:\n    scan_manager:\n      backend: {backend}\n"
    )
    return config


def load_sirius(
    connection: duckdb.DuckDBPyConnection,
    extension: Path,
    connections: int,
    config_path: Path,
    io_backend: IoBackend = IoBackend.IO_URING,
) -> None:
    """Initializes Sirius with admission for all concurrent streams."""
    if not extension.is_file():
        raise FileNotFoundError(f"Sirius extension does not exist: {extension}")
    # Admission is startup-only YAML, with no SET/connect(config=...) option.
    # Write it before LOAD initializes the shared Sirius runtime.
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        sirius_config_yaml(connections, io_backend), encoding="utf-8"
    )
    os.environ["SIRIUS_CONFIG_FILE"] = str(config_path.resolve())
    if io_backend == IoBackend.KVIKIO:
        # Use the tested POSIX/page-cache path without io_uring or cuFile.
        # Set these before LOAD initializes KvikIO's process-wide defaults.
        os.environ["KVIKIO_COMPAT_MODE"] = "ON"
        os.environ["KVIKIO_AUTO_DIRECT_IO_READ"] = "0"
    escaped_path = str(extension.resolve()).replace("'", "''")
    connection.execute(f"LOAD '{escaped_path}'")
    connection.execute("SET gpu_execution = false")


def configure_connection(
    connection: duckdb.DuckDBPyConnection, device: Device
) -> None:
    """Enables GPU execution without CPU fallback on timed GPU workers."""
    if device == Device.GPU:
        connection.execute("SET enable_duckdb_fallback = false")
        connection.execute("SET gpu_execution = true")


def run_stream(
    connection: duckdb.DuckDBPyConnection,
    queries: Queries,
    order: QueryOrder,
    barrier: threading.Barrier,
) -> StreamResult:
    """Measures query execution and full fetch after synchronized release."""
    barrier.wait()
    timings: QueryLatencies = {}
    for query_id in order:
        start = time.perf_counter()
        connection.execute(queries[query_id]).fetchall()
        timings[query_id] = time.perf_counter() - start
    return StreamResult(timings, time.perf_counter())


def run_concurrent(
    root: duckdb.DuckDBPyConnection,
    queries: Queries,
    orders: list[QueryOrder],
    settings: RunSettings,
    warmup_orders: list[QueryOrder] | None = None,
) -> tuple[list[StreamResult], float]:
    """Warms the shared instance, then measures independent stream counts."""
    warmup_orders = warmup_orders or []
    count = max(len(orders), len(warmup_orders))
    # Keep all connections open so shared engine caches survive both phases.
    with ExitStack() as stack:
        connections = [stack.enter_context(root.cursor()) for _ in range(count)]
        for connection in connections:
            configure_connection(connection, settings.device)
        with ThreadPoolExecutor(max_workers=count) as pool:
            if warmup_orders:
                print(
                    f"Running {len(warmup_orders)} warmup streams...",
                    flush=True,
                )
                barrier = threading.Barrier(len(warmup_orders))
                futures = [
                    pool.submit(run_warmup_stream, con, queries, order, barrier)
                    for con, order in zip(connections, warmup_orders)
                ]
                # Propagate warmup failures before submitting measured queries.
                for future in futures:
                    future.result()
            print(
                f"Measuring {len(orders)} {settings.device} streams...",
                flush=True,
            )
            return measure_streams(pool, connections, queries, orders)


def run_warmup_stream(
    connection: duckdb.DuckDBPyConnection,
    queries: Queries,
    order: QueryOrder,
    barrier: threading.Barrier,
) -> None:
    """Executes and fully fetches one warmup stream without query timers."""
    barrier.wait()
    for query_id in order:
        connection.execute(queries[query_id]).fetchall()


def measure_streams(
    pool: ThreadPoolExecutor,
    connections: list[duckdb.DuckDBPyConnection],
    queries: Queries,
    orders: list[QueryOrder],
) -> tuple[list[StreamResult], float]:
    """Times measurement release to final completion on existing workers."""
    started_at = 0.0

    def mark_start() -> None:
        """Records measurement start before any worker leaves the barrier."""
        nonlocal started_at
        started_at = time.perf_counter()

    barrier = threading.Barrier(len(orders), action=mark_start)
    futures = [
        pool.submit(run_stream, connection, queries, order, barrier)
        for connection, order in zip(connections, orders)
    ]
    results = [future.result() for future in futures]
    elapsed = max(stream.finished_at for stream in results) - started_at
    return results, elapsed


def mean_slowdown(
    baseline: QueryLatencies | None, streams: list[StreamResult]
) -> float | None:
    """Averages query latency ratios, with one meaning unchanged latency."""
    if baseline is None:
        return None
    ratios = [
        latency / baseline[query_id]
        for stream in streams
        for query_id, latency in stream.latencies.items()
    ]
    return sum(ratios) / len(ratios)


def initialize_database(
    root: duckdb.DuckDBPyConnection,
    settings: RunSettings,
    sirius_config: Path,
) -> tuple[Queries, str]:
    """Loads the backend and reads metadata before any timed work."""
    if settings.device == Device.GPU:
        # Admission must accommodate either phase, including warm N=1 baselines.
        admission = max(
            settings.connections,
            settings.warmup_streams if settings.warmup else 0,
        )
        load_sirius(
            root, settings.sirius_extension, admission,
            sirius_config, settings.io_backend,
        )
    return load_queries(root), root.execute("PRAGMA version").fetchone()[0]


def connect_database(settings: RunSettings) -> duckdb.DuckDBPyConnection:
    """Opens a read-only database, allowing a local unsigned GPU extension."""
    options = (
        {"allow_unsigned_extensions": "true"}
        if settings.device == Device.GPU
        else {}
    )
    return duckdb.connect(str(settings.db), read_only=True, config=options)


def benchmark(
    settings: RunSettings,
    baseline: QueryLatencies | None,
    sirius_config: Path,
) -> BenchmarkResult:
    """Measures one complete workload on a fresh database instance.

    Eviction, setup, and warmup precede timing; timers include full fetch.

    Args:
        settings: Backend, database, stream order, and connection count.
        baseline: Per-query seconds from a separate N=1 run, or None.
        sirius_config: Destination for the GPU runtime's startup YAML.

    Returns:
        Stream latencies, elapsed time, QPS, and optional mean slowdown.
    """
    print("Requesting database-file cache eviction...", flush=True)
    reset_database_cache(settings.db)
    with connect_database(settings) as root:
        queries, version = initialize_database(root, settings, sirius_config)
        orders = make_streams(
            list(queries), settings.connections, settings.seed
        )
        warmup_orders = make_warmup_streams(list(queries), settings)
        streams, elapsed = run_concurrent(
            root, queries, orders, settings, warmup_orders
        )
    count = sum(len(stream.latencies) for stream in streams)
    return BenchmarkResult(
        settings=settings,
        orders=orders,
        streams=streams,
        elapsed_s=elapsed,
        qps=count / elapsed,
        mean_slowdown=mean_slowdown(baseline, streams),
        baseline_latencies_s=baseline,
        duckdb_version=version,
        process_id=os.getpid(),
        warmup_orders=warmup_orders,
    )
