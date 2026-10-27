"""Command-line entry point; no engine import in the supervisor."""

import argparse
import sys

from .config import load_config
from .dataset import prepare
from .runner import run


def main(argv=None):
    parser = argparse.ArgumentParser(description="Two-query DuckDB/Sirius benchmark")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("prepare", "run"):
        cmd = commands.add_parser(name)
        cmd.add_argument("--config", help="TOML file; its paths are relative to its directory")
        cmd.add_argument("--db")
        cmd.add_argument("--threads", type=int)
        cmd.add_argument("--memory-limit")
        cmd.add_argument("--temp-directory")
        cmd.add_argument("--expected-source-id")
        cmd.add_argument("--tpch-extension", help="Local TPC-H extension for a custom engine build")
        if name == "prepare":
            cmd.add_argument("--scale-factor", "--sf", dest="scale_factor", type=float)
        else:
            cmd.add_argument("--backend", choices=("duckdb", "sirius"))
            source = cmd.add_mutually_exclusive_group()
            source.add_argument("--queries", nargs=2, type=int, metavar=("Q0", "Q1"))
            source.add_argument("--sql-files", nargs=2, metavar=("SQL0", "SQL1"))
            cmd.add_argument("--warmups", type=int)
            cmd.add_argument("--repetitions", type=int)
            cmd.add_argument("--seed", type=int)
            cmd.add_argument("--output-dir")
            cmd.add_argument("--label")
            cmd.add_argument("--barrier-timeout-s", type=float)
            cmd.add_argument("--trial-timeout-s", type=float)
            cmd.add_argument("--startup-timeout-s", type=float)
            cmd.add_argument("--validate", action=argparse.BooleanOptionalAction, default=None)
            cmd.add_argument("--explain", action=argparse.BooleanOptionalAction, default=None)
    args = vars(parser.parse_args(argv))
    command, path = args.pop("command"), args.pop("config")
    try:
        config = load_config(path, args)
        if command == "prepare":
            prepare(config)
            return 0
        _, success = run(config)
        return 0 if success else 1
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
