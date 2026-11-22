"""Entry point for table generation, one measurement, or a shared warm batch."""

from contextlib import closing
from dataclasses import replace
from pathlib import Path

from cli import load_run_settings, parse_arguments
from core import (
    BenchmarkResult, Device, RunSettings, benchmark, benchmark_batch,
    generate_tables,
)
from json_io import (
    create_result_directory,
    load_baseline,
    save_result,
    save_settings,
)


def report_result(
    result: BenchmarkResult, output_path: Path, name: str
) -> None:
    """Writes the typed result and reports the measured metrics."""
    save_result(output_path, result)
    save_settings(output_path.parent / name, result.settings)
    print(f"QPS: {result.qps:.3f}")
    if result.mean_slowdown is not None:
        print(f"Mean slowdown: {result.mean_slowdown:+.3f}")
    print(f"Results: {output_path}")


def run_single(settings: RunSettings) -> None:
    """Writes one result using the existing single-measurement workflow."""
    output_path = settings.output
    name = (
        "settings.json"
        if output_path is None
        else f"{output_path.stem}.settings.json"
    )
    if output_path is None:
        directory = create_result_directory(settings.results_dir, "run")
        output_path = directory / "result.json"
    settings = replace(settings, output=output_path)
    if settings.connections == 1 and settings.baseline_json is None:
        print(
            "Warning: N=1 is used as the baseline; no independent N=1 "
            "slowdown is measured.",
            flush=True,
        )
    baseline = (
        load_baseline(settings.baseline_json, settings)
        if settings.baseline_json is not None
        else None
    )
    result = benchmark(
        settings, baseline, output_path.with_suffix(".sirius.yaml")
    )
    report_result(result, output_path, name)


def run_batch(
    settings: RunSettings, connections: list[int],
) -> None:
    """Writes each warm measurement immediately while its engine stays open."""
    if settings.warmup_streams == 0:
        raise ValueError("Multiple connection counts require positive warmup")
    baseline = (
        load_baseline(settings.baseline_json, settings)
        if settings.baseline_json is not None
        else None
    )
    counts = sorted(set(connections))
    if baseline is None:
        counts = [1] + sorted(set(connections) - {1})
    first_path = settings.output
    if first_path is None:
        directory = create_result_directory(settings.results_dir, "run")
        first_path = directory / (
            "baseline.json" if baseline is None else f"n{counts[0]}.json"
        )
    settings = replace(settings, output=first_path)
    sirius_config = first_path.with_suffix(".sirius.yaml")
    with closing(
        benchmark_batch(settings, counts, sirius_config, baseline)
    ) as results:
        for result in results:
            count = result.settings.connections
            output = (
                first_path if count == counts[0]
                else first_path.with_name(f"n{count}.json")
            )
            result = replace(
                result, settings=replace(result.settings, output=output)
            )
            if settings.device == Device.GPU and count != counts[0]:
                output.with_suffix(".sirius.yaml").write_text(
                    sirius_config.read_text(encoding="utf-8"), encoding="utf-8"
                )
            report_result(result, output, f"{output.stem}.settings.json")


def main() -> None:
    """Runs the requested operation, retaining successful result files."""
    args = parse_arguments()
    settings = load_run_settings(args)
    if args.command == "generate-table":
        generate_tables(settings.db, args.scale_factor)
        return
    connections = getattr(args, "connection_counts", [settings.connections])
    if len(connections) > 1:
        run_batch(settings, connections)
    else:
        run_single(settings)


if __name__ == "__main__":
    main()
