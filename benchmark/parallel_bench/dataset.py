"""Prepares persistent TPC-H data and freezes concrete query instances.

Preparation writes a manifest with generator identity, row counts and SQL.
Measured runs read that manifest or two explicitly supplied SQL files.
Only one SELECT per slot is accepted, avoiding shared setup/cleanup state.
"""

import dataclasses
import datetime
import hashlib
import json
import uuid
from pathlib import Path

from parallel_bench import artifacts, models
from parallel_bench import backend as engines
from parallel_bench import config as configuration

TPCH_TABLES = frozenset(
    {
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    }
)


def manifest_path(db: str | Path) -> Path:
    """Returns the sidecar path associated with a persistent database."""
    return Path(str(db) + ".benchmark.json")


def _generate(
    config: configuration.Config, engine: engines.Backend
) -> models.JsonObject:
    """Generates tables and builds their manifest outside measured runs."""
    engines.load_tpch(engine.root, config.tpch_extension)
    sql = dict(
        engine.root.execute(
            "SELECT query_nr, query FROM tpch_queries()"
        ).fetchall()
    )
    if set(sql) != set(range(1, 23)):
        raise ValueError("The TPC-H extension did not supply all 22 queries")
    engine.root.execute(f"CALL dbgen(sf={config.scale_factor!r})")
    engine.root.execute("ANALYZE")
    counts = {}
    for table in sorted(TPCH_TABLES):
        row = engine.root.execute(f'SELECT count(*) FROM "{table}"').fetchone()
        assert row is not None
        counts[table] = row[0]
    engine.root.execute("CHECKPOINT")
    return {
        "schema_version": 1,
        "dataset_id": str(uuid.uuid4()),
        "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "scale_factor": config.scale_factor,
        "generator": engine.version,
        "row_counts": counts,
        "queries": sql,
        "query_source": "fixed tpch_queries() instances frozen at preparation",
    }


def prepare(config: configuration.Config) -> None:
    """Creates a new dataset and manifest, refusing to overwrite either."""
    db = Path(config.db)
    manifest = manifest_path(db)
    if db.exists() or manifest.exists():
        raise ValueError("Preparation requires a new database and manifest")
    db.parent.mkdir(parents=True, exist_ok=True)
    engine = engines.Backend(
        dataclasses.replace(config, backend=models.BackendKind.DUCKDB)
    )
    try:
        print(f"Generating TPC-H SF{config.scale_factor:g} in {db}", flush=True)
        value = _generate(config, engine)
    finally:
        engine.close()
    artifacts.write_json(manifest, value)
    print(
        f"Prepared {value['row_counts']['lineitem']:,} lineitem rows; "
        f"saved SQL to {manifest}"
    )


def _query_sources(
    config: configuration.Config,
    engine: engines.Backend,
    manifest: models.JsonObject | None,
) -> list[tuple[str, str]]:
    """Selects fixed SQL from two files or the prepared dataset manifest."""
    if config.sql_files:
        return [
            (Path(path).name, Path(path).read_text(encoding="utf-8"))
            for path in config.sql_files
        ]
    if manifest is None:
        raise ValueError("No dataset manifest; use prepare or --sql-files")
    tables = {row[0] for row in engine.root.execute("SHOW TABLES").fetchall()}
    if TPCH_TABLES - tables:
        raise ValueError(
            f"Missing TPC-H tables: {sorted(TPCH_TABLES - tables)}"
        )
    return [
        (f"Q{query}", manifest["queries"][str(query)])
        for query in config.queries
    ]


def _save_query(
    engine: engines.Backend, output: Path, slot: int, label: str, sql: str
) -> models.Query:
    """Rejects multi-statement workloads and archives an exact SQL instance."""
    statements = engine.root.extract_statements(sql)
    if (
        len(statements) != 1
        or statements[0].type != engine.duckdb.StatementType.SELECT
    ):
        raise ValueError(
            f"Slot {slot}: supply exactly one SELECT (WITH is supported). "
            "For Q15 use a CTE; CREATE/DROP VIEW scripts are rejected."
        )
    (output / f"slot-{slot}.sql").write_text(sql, encoding="utf-8")
    return {
        "slot": slot,
        "label": label,
        "sql": sql,
        "sha256": hashlib.sha256(sql.encode()).hexdigest(),
    }


def queries_for_run(
    config: configuration.Config, engine: engines.Backend, output: Path
) -> tuple[list[models.Query], models.JsonObject]:
    """Freezes the selected pair and returns dataset identity metadata."""
    manifest = manifest_path(config.db)
    dataset = (
        json.loads(manifest.read_text(encoding="utf-8"))
        if manifest.exists()
        else None
    )
    sources = _query_sources(config, engine, dataset)
    sql_dir = output / "sql"
    sql_dir.mkdir()
    queries = [
        _save_query(engine, sql_dir, slot, label, sql)
        for slot, (label, sql) in enumerate(sources)
    ]
    stat = Path(config.db).stat()
    info = {
        "path": config.db,
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "manifest": dataset,
    }
    return queries, info
