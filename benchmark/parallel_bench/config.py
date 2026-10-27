"""Strict TOML configuration; paths in TOML are relative to its directory."""

from dataclasses import asdict, dataclass, field, fields
import math
from pathlib import Path
import re
import tomllib


@dataclass
class SiriusConfig:
    extension_path: str = ""
    config_path: str = ""
    gpu: str = "0"
    enable_duckdb_fallback: bool = False
    allow_unsigned_extensions: bool = True


@dataclass
class Config:
    backend: str = "duckdb"
    db: str = "data/tpch.duckdb"
    scale_factor: float = 1.0
    queries: list[int] = field(default_factory=lambda: [3, 6])
    sql_files: list[str] = field(default_factory=list)
    warmups: int = 2
    repetitions: int = 10
    seed: int = 0
    threads: int = 4
    memory_limit: str = "32GB"
    temp_directory: str = ""
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
    sirius: SiriusConfig = field(default_factory=SiriusConfig)

    def validate_config(self):
        if self.backend not in {"duckdb", "sirius"}:
            raise ValueError("backend must be duckdb or sirius")
        for name in ("warmups", "repetitions", "threads", "seed"):
            value = getattr(self, name)
            minimum = 1 if name in {"repetitions", "threads"} else 0
            if type(value) is not int or value < minimum:
                raise ValueError(f"{name} must be an integer >= {minimum}")
        for name in ("scale_factor", "barrier_timeout_s", "trial_timeout_s", "startup_timeout_s",
                     "relative_tolerance", "absolute_tolerance"):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"{name} must be a finite number")
            if (value < 0 if name.endswith("tolerance") else value <= 0):
                raise ValueError(f"{name} must be a finite {'nonnegative' if name.endswith('tolerance') else 'positive'} number")
        for name in ("db", "memory_limit", "output_dir", "label"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"{name} must be a nonempty string")
        if not isinstance(self.expected_source_id, str):
            raise ValueError("expected_source_id must be a string")
        if not isinstance(self.queries, list) or len(self.queries) != 2 or any(
            type(q) is not int or not 1 <= q <= 22 for q in self.queries
        ):
            raise ValueError("queries must contain exactly two TPC-H IDs in 1..22")
        if not isinstance(self.sql_files, list) or (self.sql_files and len(self.sql_files) != 2):
            raise ValueError("sql_files must contain exactly two paths, or be empty")
        for name in ("validate", "explain"):
            if type(getattr(self, name)) is not bool:
                raise ValueError(f"{name} must be a boolean")
        for name in ("enable_duckdb_fallback", "allow_unsigned_extensions"):
            if type(getattr(self.sirius, name)) is not bool:
                raise ValueError(f"sirius.{name} must be a boolean")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", self.label):
            raise ValueError("label may contain only letters, digits, underscores and hyphens")
        if self.expected_source_id and not re.fullmatch(r"[0-9a-f]{10,40}", self.expected_source_id):
            raise ValueError("expected_source_id must be a 10–40 character lowercase commit ID")
        if self.backend == "sirius":
            if not self.expected_source_id:
                raise ValueError("Sirius requires expected_source_id; use the same pin for CPU runs")
            if not self.sirius.extension_path:
                raise ValueError("sirius.extension_path is required")
            if not isinstance(self.sirius.gpu, str) or not self.sirius.gpu or "," in self.sirius.gpu:
                raise ValueError("sirius.gpu must select exactly one CUDA device (index or UUID)")
        return self

    def to_dict(self):
        return asdict(self)


PATHS = {"db", "output_dir", "temp_directory", "tpch_extension"}


def resolve_path(value, base):
    if not isinstance(value, str):
        raise ValueError("paths must be strings")
    return str((base / Path(value).expanduser()).resolve()) if value else ""


def load_config(path=None, overrides=None):
    raw = {}
    base = Path.cwd()
    if path:
        path = Path(path).expanduser().resolve()
        base = path.parent
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    unknown = set(raw) - {"benchmark", "sirius"}
    if unknown:
        raise ValueError(f"Unknown TOML sections: {sorted(unknown)}")
    common = raw.get("benchmark", {})
    gpu = raw.get("sirius", {})
    if not isinstance(common, dict) or not isinstance(gpu, dict):
        raise ValueError("benchmark and sirius must be TOML tables")
    for values, cls, excluded in ((common, Config, {"sirius"}), (gpu, SiriusConfig, set())):
        unknown = set(values) - ({f.name for f in fields(cls)} - excluded)
        if unknown:
            raise ValueError(f"Unknown configuration options: {sorted(unknown)}")
    config = Config(**common, sirius=SiriusConfig(**gpu))
    for name in PATHS:
        setattr(config, name, resolve_path(getattr(config, name), base))
    if not isinstance(config.sql_files, list):
        raise ValueError("sql_files must be an array")
    config.sql_files = [resolve_path(p, base) for p in config.sql_files]
    for name in ("extension_path", "config_path"):
        setattr(config.sirius, name, resolve_path(getattr(config.sirius, name), base))
    for name, value in (overrides or {}).items():
        if value is None:
            continue
        if name in PATHS:
            value = resolve_path(value, Path.cwd())
        elif name == "sql_files":
            value = [resolve_path(p, Path.cwd()) for p in value]
        setattr(config, name, value)
    if (overrides or {}).get("queries") is not None:
        config.sql_files = []
    return config.validate_config()
