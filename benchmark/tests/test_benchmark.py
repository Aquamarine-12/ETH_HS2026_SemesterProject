"""Protocol and end-to-end tests; all data is created in temporary directories."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from decimal import Decimal
import csv
import json
from pathlib import Path
import tempfile
import threading
from time import monotonic, perf_counter_ns
import unittest

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
    def test_paths_overrides_and_query_source(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text('[benchmark]\ndb = "dataset.duckdb"\nsql_files = ["a.sql", "b.sql"]\n')
            config = load_config(path, {"queries": [6, 6]})
            self.assertEqual(config.db, str(Path(directory) / "dataset.duckdb"))
            self.assertEqual(config.queries, [6, 6])
            self.assertEqual(config.sql_files, [])
            self.assertEqual(load_config(path, {"db": "cli.duckdb"}).db,
                             str(Path.cwd() / "cli.duckdb"))

    def test_invalid_options(self):
        for changes in ({"threads": 0}, {"warmups": -1}, {"repetitions": True},
                        {"trial_timeout_s": float("nan")}, {"trial_timeout_s": "slow"},
                        {"queries": [0, 23]}, {"queries": [1]}, {"label": "../escape"},
                        {"validate": "false"}, {"relative_tolerance": -1}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                replace(Config(), **changes).validate_config()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.toml"
            path.write_text('[benchmark]\nrepetitons = 10\n')
            with self.assertRaisesRegex(ValueError, "Unknown"):
                load_config(path)


class ProtocolTests(unittest.TestCase):
    def test_two_queries_enter_execute_and_share_release(self):
        execution_gate = threading.Barrier(2)

        class Connection:
            def execute(self, sql):
                execution_gate.wait(timeout=2)
                return self

            def fetchall(self):
                return [(42,)]

            def close(self):
                pass

        class FakeBackend:
            def connection(self):
                return Connection()

        queries = [{"label": "same", "sql": "SELECT 42"}] * 2
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = run_batch(FakeBackend(), pool, queries, [0, 1], Config(), perf_counter_ns())
        records = [r for r, _ in results]
        self.assertEqual([r["status"] for r in records], ["ok", "ok"])
        self.assertEqual(len({r["thread_id"] for r in records}), 2)
        self.assertEqual(len({r["release_ns"] for r in records}), 1)
        for record in records:
            self.assertLessEqual(record["arrival_ns"], record["release_ns"])
            self.assertLessEqual(record["release_ns"], record["start_ns"])
            self.assertLessEqual(record["start_ns"], record["finish_ns"])
            self.assertGreaterEqual(record["completion_s"], record["execute_fetch_s"])

    def test_connection_failure_breaks_peer_barrier(self):
        class Connection:
            def close(self):
                pass

        class FakeBackend:
            calls = 0

            def connection(self):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("connection setup failed")
                return Connection()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = run_batch(FakeBackend(), pool, [{"label": "q"}] * 2, [0, 1],
                                Config(barrier_timeout_s=2), perf_counter_ns())
        errors = [r["error"] for r, _ in results]
        self.assertTrue(any("connection setup failed" in e for e in errors))
        self.assertTrue(any("BrokenBarrierError" in e for e in errors))

    def test_validation_preserves_duplicates_and_integer_exactness(self):
        compare_results([(None, Decimal("1.2")), ("x", 2.0)],
                        [("x", 2.0000001), (None, 1.2)], 1e-6, 1e-8)
        with self.assertRaises(ValueError):
            compare_results([(1,), (1,)], [(1,), (2,)], 1e-6, 1e-8)
        with self.assertRaises(ValueError):
            compare_results([(10**12,)], [(10**12 + 1,)], 1e-6, 1e-8)

    def test_metrics_use_paired_baselines_and_exclude_incomplete_rounds(self):
        def batch(condition, values, repetition=1, warmup=False):
            slots = [0, 1] if condition == "concurrent" else [int(condition[-1])]
            return {"condition": condition, "repetition": repetition, "warmup": warmup,
                    "records": [{"slot": slot, "status": "ok", "completion_s": value,
                                 "start_ns": slot * 100_000_000}
                                for slot, value in zip(slots, values)]}

        result = summarize([batch("isolated_0", [2]), batch("isolated_1", [4]),
                            batch("concurrent", [6, 8]), batch("concurrent", [99, 99], 2),
                            batch("concurrent", [99, 99], warmup=True)])
        self.assertEqual(result["complete_repetitions"], 1)
        self.assertEqual(result["paired_trials"][0]["slowdowns"], [3, 2])
        self.assertEqual(result["aggregate_slowdown"]["median"], 5)
        self.assertEqual(result["pair_makespan_s"]["median"], 8)
        self.assertEqual(result["submission_skew_s"]["median"], 0.1)


@unittest.skipIf(duckdb is None, "Install DuckDB to run engine integration tests")
class EngineTests(unittest.TestCase):
    def setUp(self):
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
        self.config = Config(db=str(db), sql_files=[str(p) for p in files],
                             output_dir=str(self.directory / "results"), threads=2,
                             warmups=1, repetitions=2, startup_timeout_s=30)

    def test_complete_run_and_artifacts(self):
        output, success = run(self.config)
        self.assertTrue(success)
        metadata = json.loads((output / "metadata.json").read_text())
        summary = json.loads((output / "summary.json").read_text())
        self.assertEqual(summary["complete_repetitions"], 2)
        self.assertEqual(metadata["execution_path"], "cpu")
        with (output / "samples.csv").open() as handle:
            samples = list(csv.DictReader(handle))
        self.assertEqual(len(samples), 12)  # 4 observations per round, including warmup
        self.assertTrue(all(r["status"] == "ok" for r in samples))
        for repetition in ("1", "2"):
            pair = [r for r in samples if r["condition"] == "concurrent" and
                    r["warmup"] == "False" and r["repetition"] == repetition]
            self.assertEqual(len({r["thread_id"] for r in pair}), 2)
            self.assertEqual(pair[0]["release_ns"], pair[1]["release_ns"])

    def test_query_failure_retains_error_artifacts(self):
        Path(self.config.sql_files[0]).write_text("SELECT missing FROM nonexistent_table")
        output, success = run(replace(self.config, validate=False, warmups=0))
        self.assertFalse(success)
        self.assertEqual(json.loads((output / "status.json").read_text())["status"], "error")
        self.assertIn("nonexistent_table", (output / "samples.csv").read_text())

    def test_hard_timeout_stops_native_query(self):
        for path in self.config.sql_files:
            Path(path).write_text("SELECT sum(sin(i::DOUBLE)) FROM range(1000000000000) t(i)")
        start = monotonic()
        output, success = run(replace(self.config, validate=False, warmups=0, trial_timeout_s=0.2))
        self.assertFalse(success)
        self.assertLess(monotonic() - start, 15)
        status = json.loads((output / "status.json").read_text())
        self.assertEqual(status["status"], "timeout")
        self.assertIsNotNone(status["engine_exitcode"])
        self.assertIn("timeout", (output / "samples.csv").read_text())

    def test_source_revision_mismatch_fails_before_measurement(self):
        output, success = run(replace(self.config, expected_source_id="0000000000"))
        self.assertFalse(success)
        summary = json.loads((output / "summary.json").read_text())
        self.assertIn("revision mismatch", summary["error"])
        self.assertEqual(summary["complete_repetitions"], 0)

    def test_connections_share_instance_but_not_temporary_catalog(self):
        backend = Backend(self.config)
        self.addCleanup(backend.close)
        first, second = backend.connection(), backend.connection()
        self.addCleanup(first.close)
        self.addCleanup(second.close)
        first.execute("SET threads=3")
        self.assertEqual(second.execute("SELECT current_setting('threads')").fetchone()[0], 3)
        first.execute("CREATE TEMP TABLE private_table AS SELECT 1 AS i")
        with self.assertRaises(duckdb.CatalogException):
            second.execute("SELECT * FROM private_table")

    def test_reject_multistatement_sql_before_execution(self):
        Path(self.config.sql_files[0]).write_text("SELECT 1; DROP TABLE numbers;")
        output, success = run(self.config)
        self.assertFalse(success)
        self.assertIn("exactly one SELECT", json.loads((output / "summary.json").read_text())["error"])
        con = duckdb.connect(self.config.db)
        try:
            self.assertEqual(con.execute("SELECT count(*) FROM numbers").fetchone()[0], 1000)
        finally:
            con.close()


@unittest.skipIf(duckdb is None, "Install DuckDB to run TPC-H integration tests")
class TPCHTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        con = duckdb.connect()
        try:
            con.execute("LOAD tpch")
        except duckdb.Error as exc:
            raise unittest.SkipTest(f"TPC-H extension is not locally available: {exc}")
        finally:
            con.close()

    def test_all_queries_and_duplicate_q15(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            config = Config(db=str(base / "tpch.duckdb"), scale_factor=0.01,
                            threads=2, warmups=0, repetitions=1)
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
                        queries, _ = queries_for_run(replace(config, queries=[first, second]), backend, output)
                        expected = [backend.root.execute(q["sql"]).fetchall() for q in queries]
                        results = run_batch(backend, pool, queries, [0, 1], config, perf_counter_ns())
                        for slot, (record, rows) in enumerate(results):
                            self.assertEqual(record["status"], "ok", record)
                            compare_results(expected[slot], rows, 1e-6, 1e-8)
            finally:
                backend.close()


if __name__ == "__main__":
    unittest.main()
