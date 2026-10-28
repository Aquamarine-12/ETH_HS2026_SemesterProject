"""Protocol and end-to-end checks using temporary databases.

Tests cover synchronization, metrics, real queries and hard timeouts.
No GPU behavior is inferred from successful CPU integration tests.
"""

import csv
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from time import monotonic, perf_counter_ns
from typing import Self

from parallel_bench import models
from parallel_bench.backend import Backend
from parallel_bench.config import Config, load_config
from parallel_bench.dataset import prepare, queries_for_run
from parallel_bench.metrics import summarize
from parallel_bench.runner import run, run_batch
from parallel_bench.validation import compare_results

try:
    import duckdb
except ImportError:
    duckdb = None


class ConfigurationTests(unittest.TestCase):
    """Configuration parsing and rejection of invalid input."""

    def test_paths_overrides_and_query_source(self) -> None:
        """Checks paths overrides and query source."""
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text(
                '[benchmark]\ndb = "dataset.duckdb"\n'
                'sql_files = ["a.sql", "b.sql"]\n'
            )
            config = load_config(path, {"queries": [6, 6]})
            self.assertEqual(config.db, str(Path(directory) / "dataset.duckdb"))
            self.assertEqual(config.queries, [6, 6])
            self.assertEqual(config.sql_files, [])
            self.assertEqual(
                load_config(path, {"db": "cli.duckdb"}).db,
                str(Path.cwd() / "cli.duckdb"),
            )

    def test_invalid_options(self) -> None:
        """Checks invalid options."""
        for changes in (
            {"threads": 0},
            {"warmups": -1},
            {"repetitions": True},
            {"trial_timeout_s": float("nan")},
            {"trial_timeout_s": "slow"},
            {"queries": [0, 23]},
            {"queries": [1]},
            {"label": "../escape"},
            {"validate": "false"},
            {"relative_tolerance": -1},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(Config(), **changes).validate_config()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text("[benchmark]\nrepetitons = 10\n")
            with self.assertRaisesRegex(ValueError, "Unknown"):
                load_config(path)


class ProtocolTests(unittest.TestCase):
    """Synchronization, result validation and metric definitions."""

    def test_two_queries_enter_execute_and_share_release(self) -> None:
        """Checks two queries enter execute and share release."""
        execution_gate = threading.Barrier(2)

        class Connection:
            """Minimal query connection for synchronization tests."""

            def execute(self, sql: str) -> Self:
                """Waits for both fake engine calls to overlap."""
                execution_gate.wait(timeout=2)
                return self

            def fetchall(self) -> models.ResultRows:
                """Returns a deterministic scalar answer."""
                return [(42,)]

            def close(self) -> None:
                """Closes the fake connection."""
                pass

        class FakeBackend:
            """Connection factory for deterministic failure/scheduling tests."""

            def connection(self) -> Connection:
                """Creates a fake connection or injects a setup failure."""
                return Connection()

        queries = [{"label": "same", "sql": "SELECT 42"}] * 2
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = run_batch(
                FakeBackend(),
                pool,
                queries,
                [0, 1],
                Config(),
                perf_counter_ns(),
            )
        records = [r for r, _ in results]
        self.assertEqual([r["status"] for r in records], ["ok", "ok"])
        self.assertEqual(len({r["thread_id"] for r in records}), 2)
        self.assertEqual(len({r["release_ns"] for r in records}), 1)
        for record in records:
            self.assertLessEqual(record["arrival_ns"], record["release_ns"])
            self.assertLessEqual(record["release_ns"], record["start_ns"])
            self.assertLessEqual(record["start_ns"], record["finish_ns"])
            self.assertGreaterEqual(
                record["completion_s"], record["execute_fetch_s"]
            )

    def test_connection_failure_breaks_peer_barrier(self) -> None:
        """Checks connection failure breaks peer barrier."""

        class Connection:
            """Minimal query connection for synchronization tests."""

            def close(self) -> None:
                """Closes the fake connection."""
                pass

        class FakeBackend:
            """Connection factory for deterministic failure/scheduling tests."""

            calls = 0

            def connection(self) -> Connection:
                """Creates a fake connection or injects a setup failure."""
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("connection setup failed")
                return Connection()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = run_batch(
                FakeBackend(),
                pool,
                [{"label": "q"}] * 2,
                [0, 1],
                Config(barrier_timeout_s=2),
                perf_counter_ns(),
            )
        errors = [r["error"] for r, _ in results]
        self.assertTrue(any("connection setup failed" in e for e in errors))
        self.assertTrue(any("BrokenBarrierError" in e for e in errors))

    def test_validation_preserves_duplicates_and_integer_exactness(
        self,
    ) -> None:
        """Checks validation preserves duplicates and integer exactness."""
        compare_results(
            [(None, Decimal("1.2")), ("x", 2.0)],
            [("x", 2.0000001), (None, 1.2)],
            1e-6,
            1e-8,
        )
        with self.assertRaises(ValueError):
            compare_results([(1,), (1,)], [(1,), (2,)], 1e-6, 1e-8)
        with self.assertRaises(ValueError):
            compare_results([(10**12,)], [(10**12 + 1,)], 1e-6, 1e-8)

    def test_metrics_use_paired_baselines_and_exclude_incomplete_rounds(
        self,
    ) -> None:
        """Checks metrics use paired baselines and exclude incomplete rounds."""

        def batch(
            condition: str,
            values: list[float],
            repetition: int = 1,
            warmup: bool = False,
        ) -> models.Batch:
            """Constructs an observation with known timings."""
            slots = (
                [0, 1] if condition == "concurrent" else [int(condition[-1])]
            )
            return {
                "condition": condition,
                "repetition": repetition,
                "warmup": warmup,
                "records": [
                    {
                        "slot": slot,
                        "status": "ok",
                        "completion_s": value,
                        "start_ns": slot * 100_000_000,
                    }
                    for slot, value in zip(slots, values)
                ],
            }

        result = summarize(
            [
                batch("isolated_0", [2]),
                batch("isolated_1", [4]),
                batch("concurrent", [6, 8]),
                batch("concurrent", [99, 99], 2),
                batch("concurrent", [99, 99], warmup=True),
            ]
        )
        self.assertEqual(result["complete_repetitions"], 1)
        self.assertEqual(result["paired_trials"][0]["slowdowns"], [3, 2])
        self.assertEqual(result["aggregate_slowdown"]["median"], 5)
        self.assertEqual(result["pair_makespan_s"]["median"], 8)
        self.assertEqual(result["submission_skew_s"]["median"], 0.1)


@unittest.skipIf(
    duckdb is None, "Install DuckDB to run engine integration tests"
)
class EngineTests(unittest.TestCase):
    """Real DuckDB execution, failures and artifact retention."""

    def setUp(self) -> None:
        """Creates a fresh database and two SQL files."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        db = self.directory / "test.duckdb"
        con = duckdb.connect(str(db))
        con.execute("CREATE TABLE numbers AS SELECT i FROM range(1000) t(i)")
        con.close()
        files = [self.directory / "a.sql", self.directory / "b.sql"]
        files[0].write_text("SELECT sum(i) FROM numbers")
        files[1].write_text("SELECT count(*) FROM numbers")
        self.config = Config(
            db=str(db),
            sql_files=[str(p) for p in files],
            output_dir=str(self.directory / "results"),
            threads=2,
            warmups=1,
            repetitions=2,
            startup_timeout_s=30,
        )

    def test_complete_run_and_artifacts(self) -> None:
        """Checks complete run and artifacts."""
        output, success = run(self.config)
        self.assertTrue(success)
        metadata = json.loads((output / "metadata.json").read_text())
        summary = json.loads((output / "summary.json").read_text())
        self.assertEqual(summary["complete_repetitions"], 2)
        self.assertEqual(metadata["execution_path"], "cpu")
        with (output / "samples.csv").open() as handle:
            samples = list(csv.DictReader(handle))
        self.assertEqual(
            len(samples), 12
        )  # 4 observations per round, including warmup
        self.assertTrue(all(r["status"] == "ok" for r in samples))
        for repetition in ("1", "2"):
            pair = [
                r
                for r in samples
                if r["condition"] == "concurrent"
                and r["warmup"] == "False"
                and r["repetition"] == repetition
            ]
            self.assertEqual(len({r["thread_id"] for r in pair}), 2)
            self.assertEqual(pair[0]["release_ns"], pair[1]["release_ns"])

    def test_query_failure_retains_error_artifacts(self) -> None:
        """Checks query failure retains error artifacts."""
        Path(self.config.sql_files[0]).write_text(
            "SELECT missing FROM nonexistent_table"
        )
        output, success = run(replace(self.config, validate=False, warmups=0))
        self.assertFalse(success)
        self.assertEqual(
            json.loads((output / "status.json").read_text())["status"], "error"
        )
        self.assertIn("nonexistent_table", (output / "samples.csv").read_text())

    def test_hard_timeout_stops_native_query(self) -> None:
        """Checks hard timeout stops native query."""
        for path in self.config.sql_files:
            Path(path).write_text(
                "SELECT sum(sin(i::DOUBLE)) FROM range(1000000000000) t(i)"
            )
        start = monotonic()
        output, success = run(
            replace(self.config, validate=False, warmups=0, trial_timeout_s=0.2)
        )
        self.assertFalse(success)
        self.assertLess(monotonic() - start, 15)
        status = json.loads((output / "status.json").read_text())
        self.assertEqual(status["status"], "timeout")
        self.assertIsNotNone(status["engine_exitcode"])
        self.assertIn("timeout", (output / "samples.csv").read_text())

    def test_source_revision_mismatch_fails_before_measurement(self) -> None:
        """Checks source revision mismatch fails before measurement."""
        output, success = run(
            replace(self.config, expected_source_id="0000000000")
        )
        self.assertFalse(success)
        summary = json.loads((output / "summary.json").read_text())
        self.assertIn("revision mismatch", summary["error"])
        self.assertEqual(summary["complete_repetitions"], 0)

    def test_paired_csv_contains_each_latency(self) -> None:
        """Checks explicit L_i and C_i against the original raw observations."""
        output, success = run(self.config)
        self.assertTrue(success)
        with (output / "samples.csv").open() as handle:
            raw = list(csv.DictReader(handle))
        with (output / "paired_latencies.csv").open() as handle:
            paired = list(csv.DictReader(handle))
        self.assertEqual(len(paired), 4)
        for row in paired:
            slot = row["slot"]
            selected = {
                sample["condition"]: float(sample["completion_s"])
                for sample in raw
                if sample["slot"] == slot
                and sample["warmup"] == "False"
                and sample["repetition"] == row["repetition"]
            }
            self.assertEqual(
                float(row["isolated_latency_s"]), selected[f"isolated_{slot}"]
            )
            self.assertEqual(
                float(row["concurrent_completion_s"]), selected["concurrent"]
            )
            self.assertAlmostEqual(
                float(row["slowdown"]),
                selected["concurrent"] / selected[f"isolated_{slot}"],
            )

    def test_unspecified_resources_preserve_engine_defaults(self) -> None:
        """Compares omitted resources with a fresh unconfigured engine."""
        defaults = duckdb.connect()
        try:
            expected = dict(
                defaults.execute("""
                SELECT name, value FROM duckdb_settings()
                WHERE name IN ('threads', 'external_threads', 'memory_limit')
            """).fetchall()
            )
        finally:
            defaults.close()
        backend = Backend(replace(self.config, threads=None))
        try:
            for name, value in expected.items():
                self.assertEqual(backend.settings[name], value)
        finally:
            backend.close()

    def test_explicit_resources_override_defaults(self) -> None:
        """Checks that user settings still apply to the shared instance."""
        settings = replace(
            self.config, threads=3, external_threads=1, memory_limit="512MB"
        )
        backend = Backend(settings)
        try:
            self.assertEqual(backend.settings["threads"], "3")
            self.assertEqual(backend.settings["external_threads"], "1")
            actual = backend.root.execute("""
                SELECT current_setting('memory_limit')::VARCHAR
            """).fetchone()[0]
            self.assertEqual(backend.settings["memory_limit"], actual)
            self.assertIn("MiB", actual)
        finally:
            backend.close()

    def test_connections_share_instance_but_not_temporary_catalog(self) -> None:
        """Checks connections share instance but not temporary catalog."""
        backend = Backend(self.config)
        self.addCleanup(backend.close)
        first, second = backend.connection(), backend.connection()
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        first.execute("SET threads=3")
        self.assertEqual(
            second.execute("SELECT current_setting('threads')").fetchone()[0], 3
        )
        first.execute("CREATE TEMP TABLE private_table AS SELECT 1 AS i")
        with self.assertRaises(duckdb.CatalogException):
            second.execute("SELECT * FROM private_table")

    def test_reject_multistatement_sql_before_execution(self) -> None:
        """Checks reject multistatement sql before execution."""
        Path(self.config.sql_files[0]).write_text(
            "SELECT 1; DROP TABLE numbers;"
        )
        output, success = run(self.config)
        self.assertFalse(success)
        self.assertIn(
            "exactly one SELECT",
            json.loads((output / "summary.json").read_text())["error"],
        )
        con = duckdb.connect(self.config.db)
        try:
            self.assertEqual(
                con.execute("SELECT count(*) FROM numbers").fetchone()[0], 1000
            )
        finally:
            con.close()


@unittest.skipIf(
    duckdb is None, "Install DuckDB to run TPC-H integration tests"
)
class TPCHTests(unittest.TestCase):
    """Prepared TPC-H workload coverage, including duplicate Q15."""

    @classmethod
    def setUpClass(cls) -> None:
        """Requires an already available TPC-H extension."""
        con = duckdb.connect()
        try:
            con.execute("LOAD tpch")
        except duckdb.Error as exc:
            raise unittest.SkipTest(
                f"TPC-H extension is not locally available: {exc}"
            )
        finally:
            con.close()

    def test_all_queries_and_duplicate_q15(self) -> None:
        """Checks all queries and duplicate q15."""
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            config = Config(
                db=str(base / "tpch.duckdb"),
                scale_factor=0.01,
                threads=2,
                warmups=0,
                repetitions=1,
            )
            prepare(config)
            with self.assertRaisesRegex(ValueError, "new database"):
                prepare(config)
            backend = Backend(config)
            try:
                with ThreadPoolExecutor(max_workers=2) as pool:
                    pairs = [(q, q + 1) for q in range(1, 22, 2)] + [(15, 15)]
                    for first, second in pairs:
                        output = base / f"{first}-{second}"
                        output.mkdir()
                        queries, _ = queries_for_run(
                            replace(config, queries=[first, second]),
                            backend,
                            output,
                        )
                        expected = [
                            backend.root.execute(q["sql"]).fetchall()
                            for q in queries
                        ]
                        results = run_batch(
                            backend,
                            pool,
                            queries,
                            [0, 1],
                            config,
                            perf_counter_ns(),
                        )
                        for slot, (record, rows) in enumerate(results):
                            self.assertEqual(record["status"], "ok", record)
                            compare_results(expected[slot], rows, 1e-6, 1e-8)
            finally:
                backend.close()


if __name__ == "__main__":
    unittest.main()
