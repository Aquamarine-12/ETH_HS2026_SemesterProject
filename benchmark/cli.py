"""Loads command-line arguments and optional TOML experiment settings.

Explicit arguments override TOML values, which override built-in defaults.
Paths in TOML are relative to that file; explicit CLI paths use the cwd.
This module also defines the small sweep settings used by the runner.
"""

from __future__ import annotations

import argparse
import tomllib
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any

from core import Device, RunSettings, StreamOrder, StreamSeed

PATH_FIELDS = {
    "db",
    "sirius_extension",
    "baseline_json",
    "output",
    "results_dir",
}


@dataclass
class SweepSettings:
    """Describes the repeated parameter combinations in one experiment."""

    connections: list[int]
    samples: int
    devices: list[Device]
    output: Path


def read_config(path: Path | None) -> dict[str, Any]:
    """Reads an optional TOML document."""
    if path is None:
        return {}
    return tomllib.loads(path.read_text(encoding="utf-8"))


def resolve_paths(values: dict[str, Any], base: Path) -> dict[str, Any]:
    """Expands home directories and anchors configured filesystem paths."""
    resolved = values.copy()
    for key in PATH_FIELDS & resolved.keys():
        if resolved[key] is not None:
            path = Path(resolved[key]).expanduser()
            resolved[key] = (base / path).resolve()
    return resolved


def load_run_settings(args: argparse.Namespace) -> RunSettings:
    """Combines built-in, TOML, and explicit single-run settings."""
    config = args.config
    base = config.resolve().parent if config is not None else Path.cwd()
    values = resolve_paths(asdict(RunSettings()), base)
    values.update(resolve_paths(read_config(config).get("run", {}), base))
    names = {field.name for field in fields(RunSettings)}
    overrides = {key: val for key, val in vars(args).items() if key in names}
    values.update(resolve_paths(overrides, Path.cwd()))
    values["device"] = Device(values["device"])
    values["seed"] = parse_seed(values["seed"])
    return RunSettings(**values)


def parse_seed(value: str | int) -> StreamSeed:
    """Parses a numeric shuffle seed or a named deterministic stream order."""
    if isinstance(value, int):
        return value
    try:
        return StreamOrder(value.upper())
    except ValueError:
        return int(value)


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds options for standalone measurements.

    Direct runs use CLI options alone; configured runs also read a TOML file.
    """
    parser.add_argument("--config", type=Path, help="Benchmark TOML settings")
    parser.add_argument("--db", type=Path, default=argparse.SUPPRESS)
    parser.add_argument(
        "--device", choices=list(Device), default=argparse.SUPPRESS
    )
    parser.add_argument("--connections", type=int, default=argparse.SUPPRESS)
    parser.add_argument(
        "--seed",
        type=parse_seed,
        default=argparse.SUPPRESS,
        help="Integer shuffle seed, ASC, DESC, or ASC-DESC (default: ASC)",
    )
    parser.add_argument(
        "--sirius-extension", type=Path, default=argparse.SUPPRESS
    )
    parser.add_argument(
        "--baseline-json",
        "--baseline",
        dest="baseline_json",
        type=Path,
        default=argparse.SUPPRESS,
        help="Reference from a separate N=1 run",
    )
    parser.add_argument("--output", type=Path, default=argparse.SUPPRESS)
    parser.add_argument("--results-dir", type=Path, default=argparse.SUPPRESS)
    parser.add_argument("--sample", type=int, default=argparse.SUPPRESS)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parses args for table generation or a single measurement."""
    parser = argparse.ArgumentParser(description="TPC-H stream benchmark")
    commands = parser.add_subparsers(dest="command", required=True)
    generate = commands.add_parser(
        "generate-table", help="Generate TPC-H tables"
    )
    generate.add_argument("--config", type=Path)
    generate.add_argument("--db", type=Path, default=argparse.SUPPRESS)
    generate.add_argument("--scale-factor", type=float, required=True)
    run = commands.add_parser("benchmark", help="Measure N concurrent streams")
    add_run_arguments(run)
    return parser.parse_args(argv)


def parse_sweep_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parses the experiment file and optional sweep overrides."""
    parser = argparse.ArgumentParser(
        description="Repeat TPC-H parameter sweeps"
    )
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--samples", type=int)
    parser.add_argument("--connections", type=int, nargs="+")
    return parser.parse_args(argv)


def load_sweep_settings(
    args: argparse.Namespace, run: RunSettings
) -> SweepSettings:
    """Resolves sweep values without treating them as single-run overrides."""
    values = read_config(args.config).get("sweep", {})
    output = run.results_dir
    if "output" in values:
        output = resolve_paths(
            {"output": values["output"]}, args.config.resolve().parent
        )["output"]
    if args.output is not None:
        output = args.output.expanduser().resolve()
    return SweepSettings(
        connections=(
            args.connections
            if args.connections is not None
            else values["connections"]
        ),
        samples=(
            args.samples
            if args.samples is not None
            else values.get("samples", 5)
        ),
        devices=[
            Device(value) for value in values.get("devices", [run.device])
        ],
        output=output,
    )
