"""Owns a shared DuckDB instance and creates configured worker connections.

DuckDB is imported lazily, after the child sets the Sirius environment.
Only explicit resource settings are applied; effective values are recorded.
The root handles CPU references, while Sirius workers enable GPU execution.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from parallel_bench import config as configuration
from parallel_bench import models

if TYPE_CHECKING:
    import duckdb


RESOURCE_SETTINGS = (
    "threads",
    "external_threads",
    "memory_limit",
    "temp_directory",
)


def quote(value: str) -> str:
    """Quotes a SQL string literal, including filesystem paths."""
    return "'" + value.replace("'", "''") + "'"


class Backend:
    """Keeps one database instance alive across all trial connections."""

    def __init__(self, config: configuration.Config) -> None:
        """Opens the shared database and initializes the selected backend."""
        try:
            import duckdb  # Must follow Sirius/CUDA environment setup.
        except ImportError as exc:
            raise RuntimeError(
                "Install DuckDB for the chosen engine revision"
            ) from exc
        self.duckdb = duckdb
        self.config = config
        options: dict[str, str | bool | int | float | list[str]] = {}
        if config.backend == models.BackendKind.SIRIUS:
            options["allow_unsigned_extensions"] = str(
                config.sirius.allow_unsigned_extensions
            ).lower()
        self.root = duckdb.connect(config.db, config=options)
        try:
            self.version = self._check_revision()
            self._apply_settings()
            self.settings = self._read_settings()
        except BaseException:
            # Native resources must also be released on KeyboardInterrupt.
            self.root.close()
            raise

    def _check_revision(self) -> dict[str, str]:
        """Returns engine identity, rejecting a mismatched source pin."""
        version = self.root.execute("PRAGMA version").fetchone()
        assert version is not None
        actual = version[1]
        expected = self.config.expected_source_id
        matches = len(actual) >= 10 and (
            expected.startswith(actual) or actual.startswith(expected)
        )
        if expected and not matches:
            raise ValueError(
                f"DuckDB revision mismatch: expected {expected}, "
                f"loaded {actual}"
            )
        return {
            "library_version": version[0],
            "source_id": actual,
            "python_package": self.duckdb.__version__,
        }

    def _apply_settings(self) -> None:
        """Applies explicit resources and required backend behavior."""
        for name in RESOURCE_SETTINGS:
            value = getattr(self.config, name)
            if value is not None:
                # Setting names come only from the fixed whitelist above.
                self.root.execute(f"SET {name} = ?", [value])
        self.root.execute("SET enable_progress_bar = false")
        if self.config.backend == models.BackendKind.SIRIUS:
            self.root.execute(
                f"LOAD {quote(self.config.sirius.extension_path)}"
            )
            self.root.execute("SET gpu_execution = false")

    def _read_settings(self) -> dict[str, str]:
        """Reads actual settings, including those chosen by DuckDB itself."""
        return dict(
            self.root.execute("""
            SELECT name, value FROM duckdb_settings()
            WHERE name IN ('threads', 'external_threads', 'memory_limit',
                           'temp_directory', 'scheduler_process_partial')
        """).fetchall()
        )

    def connection(self) -> duckdb.DuckDBPyConnection:
        """Creates a worker connection to the root's database instance."""
        con = self.root.cursor()
        try:
            con.execute("SET enable_progress_bar = false")
            if self.config.backend == models.BackendKind.SIRIUS:
                fallback = str(
                    self.config.sirius.enable_duckdb_fallback
                ).lower()
                con.execute(f"SET enable_duckdb_fallback = {fallback}")
                con.execute("SET gpu_execution = true")
            return con
        except BaseException:
            con.close()
            raise

    def close(self) -> None:
        """Closes the root connection after all worker connections finish."""
        self.root.close()


def load_tpch(con: duckdb.DuckDBPyConnection, extension_path: str = "") -> None:
    """Loads TPC-H, installing it if necessary and no local path is supplied."""
    if extension_path:
        con.execute(f"LOAD {quote(extension_path)}")
        return
    import duckdb  # Only called after the engine has already been initialized.

    try:
        con.execute("LOAD tpch")
    except duckdb.Error:
        con.execute("INSTALL tpch")
        con.execute("LOAD tpch")
