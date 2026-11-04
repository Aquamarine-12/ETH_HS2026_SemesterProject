"""Collects diagnostic timings while reusing the existing stream benchmark.

The cache reset, workload, backend settings, and barriers come from benchmark.
Each query is fully fetched before its connection's profile is retrieved.
Profile strings remain in memory until the workload finishes.
Profile collection changes stream pacing; use ordinary runs for throughput.
"""

import json
from dataclasses import dataclass
from pathlib import Path
import threading
import time

import duckdb

import benchmark as bench

TRACKED_METRICS = [
    "query.sql",
    "query.total_time",
    "parser.total_time",
    "planner.total_time",
    "optimizer.total_time",
    "physical_planner.total_time",
]
PROFILING_NOTE = (
    "Diagnostic run: profile collection adds gaps between queries. "
    "Use unprofiled benchmark.py runs for throughput. DuckDB phase timings "
    "may omit Sirius-specific planning."
)


@dataclass
class QueryProfile:
    """Keeps client timings and the raw engine profile for one query."""

    stream_id: int
    query_id: int
    execute_s: float
    fetch_s: float
    wall_s: float
    profile_json: str

    def as_dict(self) -> dict[str, object]:
        """Decodes the profile only after the measured workload finishes."""
        return {
            "stream_id": self.stream_id,
            "query_id": self.query_id,
            "execute_s": self.execute_s,
            "fetch_s": self.fetch_s,
            "wall_s": self.wall_s,
            "profile": json.loads(self.profile_json),
        }


class ProfilingExperiment:
    """Supplies profiling hooks for a single benchmark invocation."""

    def __init__(self) -> None:
        """Captures backend setup and creates per-stream profile storage."""
        self.configure_backend = bench.configure_connection
        self.stream_ids: dict[duckdb.DuckDBPyConnection, int] = {}
        self.profiles: list[list[QueryProfile]] = []

    def configure_connection(
        self, connection: duckdb.DuckDBPyConnection, device: bench.Device
    ) -> None:
        """Preserves backend settings and enables connection-local profiling."""
        self.configure_backend(connection, device)
        # DuckDB 2.0 renamed metrics and replaced the detailed-mode setting.
        if int(duckdb.__version__.split(".")[0]) >= 2:
            connection.execute(f"SET tracked_metrics = {TRACKED_METRICS!r}")
        else:
            connection.execute("SET profiling_mode = 'detailed'")
        connection.execute("SET enable_profiling = 'no_output'")
        # Configuration runs serially before workers start. Each worker then
        # writes only its own list, so profile collection needs no shared lock.
        self.stream_ids[connection] = len(self.profiles)
        self.profiles.append([])

    def run_stream(
        self,
        connection: duckdb.DuckDBPyConnection,
        queries: bench.Queries,
        order: bench.QueryOrder,
        barrier: threading.Barrier | None = None,
    ) -> bench.StreamResult:
        """Times complete queries and captures profiles outside query timing."""
        stream_id = self.stream_ids[connection]
        timings: bench.QueryLatencies = {}
        if barrier is not None:
            barrier.wait()
        for query_id in order:
            start = time.perf_counter()
            result = connection.execute(queries[query_id])
            executed = time.perf_counter()
            # execute() can return before execution finishes. Fetching all rows
            # preserves our original completion boundary for both backends.
            result.fetchall()
            finished = time.perf_counter()
            timings[query_id] = finished - start
            self.profiles[stream_id].append(
                QueryProfile(
                    stream_id, query_id, executed - start, finished - executed,
                    timings[query_id], connection.get_profiling_information(),
                )
            )
        return bench.StreamResult(timings, finished)

    def save(self, output: Path) -> None:
        """Labels the diagnostic result and saves profiles beside its JSON."""
        result = json.loads(output.read_text(encoding="utf-8"))
        result.update(
            profiling_enabled=True,
            duckdb_version=duckdb.__version__,
            profiling_note=PROFILING_NOTE,
        )
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        path = output.with_suffix(".profiles.json")
        payload = {
            key: result[key]
            for key in ["device", "connections", "process_id", "duckdb_version"]
        }
        payload["note"] = PROFILING_NOTE
        payload["records"] = [
            record.as_dict() for stream in self.profiles for record in stream
        ]
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(f"Diagnostic profiles: {path}")


def main() -> None:
    """Runs the CLI with temporary hooks and restores the original functions."""
    args = bench.parse_arguments()
    experiment = ProfilingExperiment()
    original_stream = bench.run_stream
    bench.configure_connection = experiment.configure_connection
    bench.run_stream = experiment.run_stream
    try:
        if args.command == "benchmark":
            print(PROFILING_NOTE)
        bench.main()
    finally:
        bench.configure_connection = experiment.configure_backend
        bench.run_stream = original_stream
    if args.command == "benchmark":
        experiment.save(args.output)


if __name__ == "__main__":
    main()
