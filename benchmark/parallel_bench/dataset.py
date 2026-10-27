"""Prepare once and freeze concrete SQL alongside the persistent dataset."""

from dataclasses import replace
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import uuid

from .artifacts import write_json
from .backend import Backend, load_tpch


TPCH_TABLES = {"customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"}


def manifest_path(db):
    return Path(str(db) + ".benchmark.json")


def prepare(config):
    db = Path(config.db)
    manifest = manifest_path(db)
    if db.exists() or manifest.exists():
        raise ValueError("Preparation requires a new database and manifest; choose another --db path")
    db.parent.mkdir(parents=True, exist_ok=True)
    backend = Backend(replace(config, backend="duckdb"))
    try:
        load_tpch(backend.root, config.tpch_extension)
        sql = dict(backend.root.execute("SELECT query_nr, query FROM tpch_queries()").fetchall())
        if set(sql) != set(range(1, 23)):
            raise ValueError("The TPC-H extension did not supply all 22 queries")
        print(f"Generating TPC-H SF{config.scale_factor:g} in {db}", flush=True)
        backend.root.execute(f"CALL dbgen(sf={config.scale_factor!r})")
        backend.root.execute("ANALYZE")
        counts = {table: backend.root.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
                  for table in sorted(TPCH_TABLES)}
        backend.root.execute("CHECKPOINT")
        value = {"schema_version": 1, "dataset_id": str(uuid.uuid4()),
                 "created_utc": datetime.now(timezone.utc).isoformat(),
                 "scale_factor": config.scale_factor, "generator": backend.version,
                 "row_counts": counts, "queries": sql,
                 "query_source": "fixed instances from tpch_queries(), frozen at preparation"}
    finally:
        backend.close()
    write_json(manifest, value)
    print(f"Prepared {counts['lineitem']:,} lineitem rows; saved SQL to {manifest}")


def queries_for_run(config, backend, output):
    manifest = manifest_path(config.db)
    dataset = json.loads(manifest.read_text()) if manifest.exists() else None
    if config.sql_files:
        sources = [(Path(p).name, Path(p).read_text()) for p in config.sql_files]
    else:
        if dataset is None:
            raise ValueError("No dataset manifest. Use prepare, or provide --sql-files for an existing database")
        tables = {r[0] for r in backend.root.execute("SHOW TABLES").fetchall()}
        if TPCH_TABLES - tables:
            raise ValueError(f"Missing TPC-H tables: {sorted(TPCH_TABLES - tables)}")
        sources = [(f"Q{q}", dataset["queries"][str(q)]) for q in config.queries]
    queries = []
    sql_dir = output / "sql"
    sql_dir.mkdir()
    for slot, (label, sql) in enumerate(sources):
        statements = backend.root.extract_statements(sql)
        if len(statements) != 1 or statements[0].type != backend.duckdb.StatementType.SELECT:
            raise ValueError(
                f"Slot {slot}: supply exactly one SELECT (WITH is supported). "
                "For Q15 use a CTE, as in DuckDB's built-in query; CREATE/DROP VIEW scripts are rejected."
            )
        digest = hashlib.sha256(sql.encode()).hexdigest()
        (sql_dir / f"slot-{slot}.sql").write_text(sql)
        queries.append({"slot": slot, "label": label, "sql": sql, "sha256": digest})
    stat = Path(config.db).stat()
    info = {"path": config.db, "size_bytes": stat.st_size, "mtime_ns": stat.st_mtime_ns,
            "manifest": dataset}
    return queries, info
