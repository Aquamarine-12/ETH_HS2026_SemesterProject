"""Parses commands for dataset preparation, experiments and YAML preview.

CLI options override TOML without importing DuckDB in the supervisor.
The sirius-config command can render startup YAML on a CPU-only machine.
All commands return a nonzero exit status on validation or runtime failure.
"""

import argparse
import sys
from pathlib import Path

from parallel_bench import config as configuration
from parallel_bench import dataset, runner, sirius


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    """Adds dataset identity and explicit engine resource overrides."""
    parser.add_argument(
        "--config", help="TOML file with paths relative to its directory"
    )
    parser.add_argument("--db")
    parser.add_argument(
        "--threads", type=int, help="Omit to use DuckDB's default"
    )
    parser.add_argument("--external-threads", type=int)
    parser.add_argument("--memory-limit", help="Omit to use DuckDB's default")
    parser.add_argument("--temp-directory")
    parser.add_argument("--expected-source-id")
    parser.add_argument(
        "--tpch-extension", help="Local extension for a custom build"
    )


def _add_run_options(parser: argparse.ArgumentParser) -> None:
    """Adds workload, measurement and output controls."""
    parser.add_argument("--backend", choices=("duckdb", "sirius"))
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--queries", nargs=2, type=int, metavar=("Q0", "Q1"))
    source.add_argument("--sql-files", nargs=2, metavar=("SQL0", "SQL1"))
    for name in ("warmups", "repetitions", "seed"):
        parser.add_argument(f"--{name}", type=int)
    for name in ("barrier", "trial", "startup"):
        parser.add_argument(f"--{name}-timeout-s", type=float)
    parser.add_argument("--output-dir")
    parser.add_argument("--label")
    for name in ("validate", "explain"):
        parser.add_argument(
            f"--{name}", action=argparse.BooleanOptionalAction, default=None
        )


def _parser() -> argparse.ArgumentParser:
    """Builds the public command-line interface."""
    parser = argparse.ArgumentParser(
        description="Two-query DuckDB/Sirius benchmark"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_parser = commands.add_parser("prepare")
    _add_common_options(prepare_parser)
    prepare_parser.add_argument(
        "--scale-factor", "--sf", dest="scale_factor", type=float
    )
    run_parser = commands.add_parser("run")
    _add_common_options(run_parser)
    _add_run_options(run_parser)
    yaml_parser = commands.add_parser(
        "sirius-config", help="Preview effective YAML without a GPU"
    )
    yaml_parser.add_argument("--config", required=True)
    yaml_parser.add_argument(
        "--output", help="New output file; omit to print YAML"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Dispatches a CLI command and reports user-facing errors concisely."""
    args = vars(_parser().parse_args(argv))
    command, path = args.pop("command"), args.pop("config")
    output = args.pop("output", None)
    try:
        config = configuration.load_config(path, args)
        if command == "prepare":
            dataset.prepare(config)
            return 0
        if command == "sirius-config":
            if output:
                target = sirius.write_config(
                    config, Path(output).expanduser().resolve()
                )
                print(f"Wrote {target}")
            else:
                print(sirius.render_yaml(config), end="")
            return 0
        _, success = runner.run(config)
        return 0 if success else 1
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
