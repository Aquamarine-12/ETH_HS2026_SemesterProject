"""Backend setup. Import DuckDB only after setting the Sirius environment."""

import hashlib
import os
from pathlib import Path


def quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def configure_environment(config, output):
    if config.backend != "sirius":
        return
    if not Path(config.sirius.extension_path).is_file():
        raise ValueError(f"Sirius extension not found: {config.sirius.extension_path}")
    try:
        import yaml
    except ImportError as exc:
        raise RuntimeError("Install the Sirius extra: pip install -e '.[sirius]'") from exc
    runtime = {}
    if config.sirius.config_path:
        runtime = yaml.safe_load(Path(config.sirius.config_path).read_text())
    if not isinstance(runtime, dict):
        raise ValueError("Sirius YAML must contain a mapping")
    sirius = runtime.setdefault("sirius", {})
    if not isinstance(sirius, dict):
        raise ValueError("The sirius YAML key must contain a mapping")
    # Set the startup admission limit explicitly, including for isolated controls.
    sirius["max_concurrent_queries"] = 2
    scan = sirius.get("executor", {}).get("scan_manager", {})
    if "max_concurrent_queries" in scan:
        scan["max_concurrent_queries"] = 2
    target = output / "sirius-effective.yaml"
    target.write_text(yaml.safe_dump(runtime, sort_keys=False))
    os.environ["SIRIUS_CONFIG_FILE"] = str(target)
    os.environ["CUDA_VISIBLE_DEVICES"] = config.sirius.gpu


class Backend:
    def __init__(self, config):
        try:
            import duckdb
        except ImportError as exc:
            raise RuntimeError("DuckDB is missing; install the wheel for your chosen engine revision") from exc
        self.duckdb = duckdb
        self.config = config
        options = {}
        if config.backend == "sirius":
            options["allow_unsigned_extensions"] = str(config.sirius.allow_unsigned_extensions).lower()
        self.root = duckdb.connect(config.db, config=options)
        try:
            version = self.root.execute("PRAGMA version").fetchone()
            self.version = {"library_version": version[0], "source_id": version[1],
                            "python_package": duckdb.__version__}
            expected = config.expected_source_id
            actual = version[1]
            if expected and not (len(actual) >= 10 and (expected.startswith(actual) or actual.startswith(expected))):
                raise ValueError(f"DuckDB revision mismatch: expected {expected}, loaded {actual}")
            self.root.execute(f"SET threads = {config.threads}")
            self.root.execute("SET external_threads = 1")
            self.root.execute("SET memory_limit = ?", [config.memory_limit])
            if config.temp_directory:
                self.root.execute("SET temp_directory = ?", [config.temp_directory])
            self.root.execute("SET enable_progress_bar = false")
            if config.backend == "sirius":
                self.root.execute(f"LOAD {quote(config.sirius.extension_path)}")
                # Metadata/reference queries use CPU, workers override this locally.
                self.root.execute("SET gpu_execution = false")
            self.settings = dict(self.root.execute("""
                SELECT name, value FROM duckdb_settings()
                WHERE name IN ('threads', 'external_threads', 'memory_limit',
                               'temp_directory', 'scheduler_process_partial')
            """).fetchall())
        except BaseException:
            self.root.close()
            raise

    def connection(self):
        # cursor() creates a new connection to the SAME native DatabaseInstance.
        # Each worker creates, uses and closes its own connection.
        con = self.root.cursor()
        try:
            con.execute("SET enable_progress_bar = false")
            if self.config.backend == "sirius":
                fallback = str(self.config.sirius.enable_duckdb_fallback).lower()
                con.execute(f"SET enable_duckdb_fallback = {fallback}")
                con.execute("SET gpu_execution = true")
            return con
        except BaseException:
            con.close()
            raise

    def close(self):
        self.root.close()


def load_tpch(con, extension_path=""):
    if extension_path:
        con.execute(f"LOAD {quote(extension_path)}")
        return
    try:
        con.execute("LOAD tpch")
    except Exception:
        con.execute("INSTALL tpch")
        con.execute("LOAD tpch")
