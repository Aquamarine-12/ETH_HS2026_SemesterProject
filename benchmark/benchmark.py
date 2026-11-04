"""Entry point for table generation and a single benchmark measurement."""

from dataclasses import replace
from pathlib import Path

from cli import load_run_settings, parse_arguments
from core import BenchmarkResult, benchmark, generate_tables
from json_io import (
    create_result_directory,
    load_baseline,
    save_result,
    save_settings,
)


def report_result(result: BenchmarkResult, output: Path, name: str) -> None:
    """Writes the typed result and reports the measured metrics."""
    save_result(output, result)
    save_settings(output.parent / name, result.settings)
    print(f"QPS: {result.qps:.3f}")
    if result.mean_slowdown is not None:
        print(f"Mean slowdown: {result.mean_slowdown:+.3f}")
    print(f"Results: {output}")


def main() -> None:
    """Runs the requested operation and writes results after measurement."""
    args = parse_arguments()
    settings = load_run_settings(args)
    if args.command == "generate-table":
        generate_tables(settings.db, args.scale_factor)
        return
    output = settings.output
    name = "settings.json" if output is None else f"{output.stem}.settings.json"
    if output is None:
        directory = create_result_directory(settings.results_dir, "run")
        output = directory / "result.json"
    settings = replace(settings, output=output)
    if settings.connections == 1 and settings.baseline_json is None:
        print(
            "Warning: N=1 is used as the baseline; no independent N=1 "
            "slowdown is measured.",
            flush=True,
        )
    baseline = (
        load_baseline(settings.baseline_json)
        if settings.baseline_json is not None
        else None
    )
    result = benchmark(settings, baseline, output.with_suffix(".sirius.yaml"))
    report_result(result, output, name)


if __name__ == "__main__":
    main()
