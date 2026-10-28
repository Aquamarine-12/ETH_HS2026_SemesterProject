"""Loads benchmark settings without guessing engine defaults.

TOML paths are relative to the config file; CLI paths use the cwd.
Optional engine settings stay None until explicitly supplied by the user.
Experiment settings, such as repetitions and timeouts, have runner defaults.
"""

from __future__ import annotations

import dataclasses
import math
import re
import tomllib
from pathlib import Path

from parallel_bench import models


@dataclasses.dataclass
class SiriusConfig:
    """Sirius extension, device selection, and CPU-fallback policy."""

    extension_path: str = ""
    config_path: str = ""
    gpu: str | None = None
    enable_duckdb_fallback: bool = False
    allow_unsigned_extensions: bool = True


@dataclasses.dataclass
class Config:
    """Resolved experiment settings; None preserves an engine default."""

    backend: models.BackendKind = models.BackendKind.DUCKDB
    db: str = "data/tpch.duckdb"
    scale_factor: float = 1.0
    queries: list[int] = dataclasses.field(default_factory=lambda: [3, 6])
    sql_files: list[str] = dataclasses.field(default_factory=list)
    warmups: int = 2
    repetitions: int = 10
    seed: int = 0
    threads: int | None = None
    external_threads: int | None = None
    memory_limit: str | None = None
    temp_directory: str | None = None
    output_dir: str = "results"
    label: str = "run"
    barrier_timeout_s: float = 30.0
    trial_timeout_s: float = 300.0
    startup_timeout_s: float = 120.0
    validate: bool = True
    relative_tolerance: float = 1e-6
    absolute_tolerance: float = 1e-8
    explain: bool = False
    expected_source_id: str = ""
    tpch_extension: str = ""
    sirius: SiriusConfig = dataclasses.field(default_factory=SiriusConfig)

    def validate_config(self) -> Config:
        """Validates settings and normalizes the backend enum."""
        self.backend = models.BackendKind(self.backend)
        _validate_numbers(self)
        _validate_strings(self)
        _validate_query_source(self)
        _validate_booleans(self)
        if self.backend == models.BackendKind.SIRIUS:
            _validate_sirius(self)
        return self

    def to_dict(self) -> models.JsonObject:
        """Returns a JSON snapshot, including unspecified settings."""
        return dataclasses.asdict(self)


PATH_FIELDS = frozenset(
    {"db", "output_dir", "temp_directory", "tpch_extension"}
)


def _validate_numbers(config: Config) -> None:
    """Checks numeric bounds, rejecting booleans and nonfinite values."""
    integers = {
        "warmups": 0,
        "repetitions": 1,
        "seed": 0,
        "threads": 1,
        "external_threads": 0,
    }
    for name, minimum in integers.items():
        value = getattr(config, name)
        if value is None and name in {"threads", "external_threads"}:
            continue
        if type(value) is not int or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}")
    names = (
        "scale_factor",
        "barrier_timeout_s",
        "trial_timeout_s",
        "startup_timeout_s",
        "relative_tolerance",
        "absolute_tolerance",
    )
    for name in names:
        value = getattr(config, name)
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError(f"{name} must be a finite number")
        if value < 0 or (value == 0 and not name.endswith("tolerance")):
            raise ValueError(f"Invalid value for {name}: {value}")


def _validate_strings(config: Config) -> None:
    """Checks labels, paths, optional engine strings, and revision pins."""
    for name in ("db", "output_dir", "label"):
        if not isinstance(getattr(config, name), str) or not getattr(
            config, name
        ):
            raise ValueError(f"{name} must be a nonempty string")
    for name in ("memory_limit", "temp_directory"):
        value = getattr(config, name)
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError(f"{name} must be omitted or a nonempty string")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", config.label):
        raise ValueError(
            "label allows letters, digits, underscores and hyphens"
        )
    source_id = config.expected_source_id
    if not isinstance(source_id, str):
        raise ValueError("expected_source_id must be a string")
    if source_id and not re.fullmatch(r"[0-9a-f]{10,40}", source_id):
        raise ValueError(
            "expected_source_id must be a 10–40 character commit ID"
        )


def _validate_query_source(config: Config) -> None:
    """Checks the two-slot workload without rejecting duplicate query IDs."""
    if not isinstance(config.queries, list) or len(config.queries) != 2:
        raise ValueError("queries must contain exactly two TPC-H IDs")
    if any(
        type(query) is not int or not 1 <= query <= 22
        for query in config.queries
    ):
        raise ValueError("TPC-H IDs must be integers in 1..22")
    paths = config.sql_files
    if not isinstance(paths, list) or (paths and len(paths) != 2):
        raise ValueError(
            "sql_files must contain exactly two paths, or be empty"
        )
    if any(not isinstance(path, str) or not path for path in paths):
        raise ValueError("sql_files must contain nonempty paths")


def _validate_booleans(config: Config) -> None:
    """Prevents quoted TOML strings from silently acting as booleans."""
    for name in ("validate", "explain"):
        if type(getattr(config, name)) is not bool:
            raise ValueError(f"{name} must be a boolean")
    for name in ("enable_duckdb_fallback", "allow_unsigned_extensions"):
        if type(getattr(config.sirius, name)) is not bool:
            raise ValueError(f"sirius.{name} must be a boolean")


def _validate_sirius(config: Config) -> None:
    """Checks the mandatory version pin and optional device override."""
    if not config.expected_source_id:
        raise ValueError("Sirius requires expected_source_id; pin CPU runs too")
    if not config.sirius.extension_path:
        raise ValueError("sirius.extension_path is required")
    gpu = config.sirius.gpu
    if gpu is not None and (not isinstance(gpu, str) or not gpu or "," in gpu):
        raise ValueError(
            "sirius.gpu must select one CUDA device, or be omitted"
        )


def resolve_path(value: str, base: Path) -> str:
    """Resolves a user path against its source directory, preserving blanks."""
    if not isinstance(value, str):
        raise ValueError("paths must be strings")
    return str((base / Path(value).expanduser()).resolve()) if value else ""


def _read_tables(path: str | Path | None) -> tuple[models.JsonObject, Path]:
    """Reads TOML and rejects unknown sections or options."""
    raw: models.JsonObject = {}
    base = Path.cwd()
    if path:
        source = Path(path).expanduser().resolve()
        base = source.parent
        with source.open("rb") as handle:
            raw = tomllib.load(handle)
    if set(raw) - {"benchmark", "sirius"}:
        raise ValueError("Unknown TOML sections")
    for section, cls in (("benchmark", Config), ("sirius", SiriusConfig)):
        values = raw.get(section, {})
        if not isinstance(values, dict):
            raise ValueError(f"{section} must be a TOML table")
        allowed = {field.name for field in dataclasses.fields(cls)} - {"sirius"}
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"Unknown configuration options: {unknown}")
    return raw, base


def _resolve_config_paths(config: Config, base: Path) -> None:
    """Resolves paths before CLI overrides are applied."""
    for name in PATH_FIELDS:
        value = getattr(config, name)
        if value is not None:
            setattr(config, name, resolve_path(value, base))
    if not isinstance(config.sql_files, list):
        raise ValueError("sql_files must be an array")
    config.sql_files = [resolve_path(path, base) for path in config.sql_files]
    for name in ("extension_path", "config_path"):
        value = resolve_path(getattr(config.sirius, name), base)
        setattr(config.sirius, name, value)


def _apply_overrides(config: Config, overrides: models.JsonObject) -> None:
    """Applies explicit CLI values, interpreting paths relative to cwd."""
    for name, value in overrides.items():
        if value is None:
            continue
        if name in PATH_FIELDS:
            value = resolve_path(value, Path.cwd())
        elif name == "sql_files":
            value = [resolve_path(path, Path.cwd()) for path in value]
        setattr(config, name, value)
    if overrides.get("queries") is not None:
        config.sql_files = []


def load_config(
    path: str | Path | None = None,
    overrides: models.JsonObject | None = None,
) -> Config:
    """Loads defaults, TOML, then CLI overrides and validates the result."""
    raw, base = _read_tables(path)
    config = Config(
        **raw.get("benchmark", {}), sirius=SiriusConfig(**raw.get("sirius", {}))
    )
    _resolve_config_paths(config, base)
    _apply_overrides(config, overrides or {})
    return config.validate_config()
