#!/usr/bin/env python3
"""Run two TPC-H queries in one DuckDB instance using two application threads."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import platform
import random
import statistics
import threading
from time import perf_counter

import duckdb

TPCH_TABLES = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}


def positive_int(text):
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be at least 1")
    return value


def nonnegative_int(text):
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return value


def scale_factor(text):
    value = float(text)
    if not math.isfinite(value) or value <= 0:
        raise argparse.ArgumentTypeError("scale factor must be finite and positive")
    return value


def query_id(text):
    value = int(text)
    if not 1 <= value <= 22:
        raise argparse.ArgumentTypeError("TPC-H query number must be between 1 and 22")
    return value


def configure(con, args):
    # These are instance-wide settings. Set them once, before starting workers.
    con.execute(f"SET threads={args.threads}")
    con.execute("SET external_threads=1")
    con.execute("SET memory_limit=?", [args.memory])
    con.execute(f"SET scheduler_process_partial={'true' if args.partial else 'false'}")
    con.execute("SET enable_progress_bar=false")


def load_tpch(con):
    try:
        con.execute("LOAD tpch")
    except duckdb.Error:
        # One-time download if the extension is not already installed.
        con.execute("INSTALL tpch")
        con.execute("LOAD tpch")


def initialize(db, args):
    db.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db)) as con:
        configure(con, args)
        if con.execute("SHOW TABLES").fetchall():
            raise ValueError("Database is not empty. Choose another --db filename; existing tables will not be overwritten.")
        load_tpch(con)
        print(f"Generating SF{args.sf:g} in {db} ...", flush=True)
        con.execute(f"CALL dbgen(sf={args.sf!r})")
        con.execute("CREATE TABLE _two_query_dataset(scale_factor DOUBLE, generated_with VARCHAR)")
        con.execute("INSERT INTO _two_query_dataset VALUES (?, ?)", [args.sf, duckdb.__version__])
        con.execute("ANALYZE")
        con.execute("CHECKPOINT")
        count = con.execute("SELECT count(*) FROM lineitem").fetchone()[0]
        print(f"Ready: lineitem has {count:,} rows. Data persists after this program exits.")


def run_one(db, slot, nr, sql, barrier=None):
    """Called in an application thread; that thread opens and owns its connection."""
    try:
        # Identical absolute file paths in this process reuse DuckDB's instance
        # cache, while each connect() creates an independent SQL connection.
        with duckdb.connect(str(db)) as con:
            con.execute("SET enable_progress_bar=false")
            if barrier is not None:
                barrier.wait(timeout=30)
            start = perf_counter()
            rows = con.execute(sql).fetchall()
            end = perf_counter()
            return {"slot": slot, "query": nr, "start_s": start, "end_s": end,
                    "latency_s": end - start, "result_rows": len(rows),
                    "application_thread_id": threading.get_native_id()}
    except BaseException:
        # Release the other participant if opening a connection/query fails.
        if barrier is not None:
            barrier.abort()
        raise


def run_pair(pool, db, numbers, sqls):
    barrier = threading.Barrier(2)
    futures = [pool.submit(run_one, db, slot, nr, sql, barrier)
               for slot, (nr, sql) in enumerate(zip(numbers, sqls))]
    records = []
    errors = []
    for future in futures:
        try:
            records.append(future.result())
        except BaseException as exc:
            errors.append(exc)
    if errors:
        # Prefer the initiating error to the peer's BrokenBarrierError.
        raise next((e for e in errors if not isinstance(e, threading.BrokenBarrierError)), errors[0])
    return records


def benchmark(db, args):
    if not db.is_file():
        raise ValueError(f"Database does not exist: {db}\nCreate it first with: python concurrent_tpch.py init --db {db} --sf 1")
    output = Path(args.output).expanduser().resolve()
    if output.exists():
        raise ValueError(f"Output already exists: {output}\nChoose a different --output filename.")

    # Keep this connection open for the entire experiment so the shared native
    # database instance and its background worker pool remain alive.
    with duckdb.connect(str(db)) as root:
        configure(root, args)
        tables = {row[0] for row in root.execute("SHOW TABLES").fetchall()}
        missing = TPCH_TABLES - tables
        if missing:
            raise ValueError("Missing TPC-H tables: " + ", ".join(sorted(missing)) +
                             ". Generate a complete dataset using the init command in a new database.")
        load_tpch(root)
        numbers = args.queries
        all_queries = dict(root.execute("SELECT query_nr, query FROM tpch_queries()").fetchall())
        sqls = [all_queries[nr] for nr in numbers]
        settings = dict(root.execute("""
            SELECT name, value FROM duckdb_settings()
            WHERE name IN ('threads','external_threads','scheduler_process_partial','memory_limit','temp_directory')
        """).fetchall())
        sf = None
        if "_two_query_dataset" in tables:
            sf = root.execute("SELECT scale_factor FROM _two_query_dataset").fetchone()[0]
        metadata = {"utc": datetime.now(timezone.utc).isoformat(),
                    "duckdb_version": root.execute("PRAGMA version").fetchone(),
                    "python": platform.python_version(), "platform": platform.platform(),
                    "process_id": os.getpid(), "logical_cpus_reported": os.cpu_count(),
                    "database": str(db), "scale_factor": sf, "settings": settings,
                    "queries": numbers, "query_sql": sqls,
                    "warmup_rounds": args.warmups, "repetitions": args.repetitions,
                    "seed": args.seed,
                    "measurement": "execute(sql).fetchall(); includes parse/plan and Python result conversion; excludes connection creation and barrier wait",
                    "correctness": "row counts checked across runs; full answer values are not validated"}
        print(f"DuckDB {duckdb.__version__}; Q{numbers[0]} + Q{numbers[1]}; settings={settings}", flush=True)
        baseline = [[], []]
        concurrent = [[], []]
        raw = []
        expected_counts = {}
        rng = random.Random(args.seed)

        with ThreadPoolExecutor(max_workers=2, thread_name_prefix="query-client") as pool:
            # Warm each query alone and the concurrent pair, without recording timings.
            for _ in range(args.warmups):
                for slot in range(2):
                    pool.submit(run_one, db, slot, numbers[slot], sqls[slot]).result()
                run_pair(pool, db, numbers, sqls)

            for trial in range(args.repetitions):
                phases = ["isolated_0", "isolated_1", "concurrent"]
                rng.shuffle(phases)  # Avoid always measuring one condition first.
                for phase in phases:
                    if phase == "concurrent":
                        records = run_pair(pool, db, numbers, sqls)
                    else:
                        slot = int(phase[-1])
                        records = [pool.submit(run_one, db, slot, numbers[slot], sqls[slot]).result()]
                    origin = min(record["start_s"] for record in records)
                    for record in records:
                        slot = record["slot"]
                        count = record["result_rows"]
                        if slot in expected_counts and expected_counts[slot] != count:
                            raise ValueError(f"Result row count changed for slot {slot}")
                        expected_counts[slot] = count
                        record["start_s"] -= origin
                        record["end_s"] -= origin
                        target = concurrent if phase == "concurrent" else baseline
                        target[slot].append(record["latency_s"])
                    span = max(record["end_s"] for record in records)
                    raw.append({"trial": trial + 1, "phase": phase, "records": records,
                                "makespan_s": span, "batch_queries_per_second": len(records) / span})
                print(f"Completed repetition {trial + 1}/{args.repetitions}", flush=True)

        summary = []
        print("\nslot  query  isolated median (s)  concurrent median (s)  slowdown")
        for slot, nr in enumerate(numbers):
            alone = statistics.median(baseline[slot])
            together = statistics.median(concurrent[slot])
            summary.append({"slot": slot, "query": nr, "isolated_median_s": alone,
                            "concurrent_median_s": together, "slowdown": together / alone})
            print(f"{slot:4d}  Q{nr:<4d}  {alone:19.6f}  {together:21.6f}  {together / alone:8.3f}x")
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("x", encoding="utf-8") as handle:
            json.dump({"metadata": metadata, "summary": summary, "runs": raw}, handle, indent=2)
        print(f"\nSaved {output}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "run"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--db", default="tpch.duckdb", help="persistent database file")
        cmd.add_argument("--threads", type=positive_int, default=4, help="DuckDB thread configuration, not query count")
        cmd.add_argument("--memory", default="4GB", help="instance-wide buffer-manager memory limit")
        cmd.add_argument("--partial", action="store_true", help="enable partial task rescheduling")
        if name == "init":
            cmd.add_argument("--sf", type=scale_factor, default=1.0)
        else:
            cmd.add_argument("--queries", type=query_id, nargs=2, default=[1, 2], metavar=("Q_A", "Q_B"))
            cmd.add_argument("--repetitions", type=positive_int, default=5)
            cmd.add_argument("--warmups", type=nonnegative_int, default=1)
            cmd.add_argument("--seed", type=int, default=0)
            cmd.add_argument("--output", default="results.json")
    args = parser.parse_args()
    db = Path(args.db).expanduser().resolve()
    try:
        (initialize if args.command == "init" else benchmark)(db, args)
    except (duckdb.Error, OSError, ValueError, threading.BrokenBarrierError) as exc:
        parser.exit(1, f"Error: {exc}\n")


if __name__ == "__main__":
    main()
