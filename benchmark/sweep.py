"""Runs repeated parameter combinations in fresh, sequential processes.

Each sample/backend combination gets one isolated reference measurement.
Its concurrent results retain that reference; N=1 reuses the reference itself.
The completed index links raw results without replacing them with averages.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path

from cli import load_run_settings, load_sweep_settings, parse_sweep_arguments
from core import RunSettings
from json_io import (
    JsonObject,
    create_result_directory,
    relative_path,
    write_json,
)


def run_child(settings: RunSettings) -> None:
    """Launches one measurement using the current environment's Python."""
    command = [
        sys.executable,
        str(Path(__file__).with_name("benchmark.py")),
        "benchmark",
        "--db",
        str(settings.db),
        "--device",
        settings.device,
        "--connections",
        str(settings.connections),
        "--sample",
        str(settings.sample),
        "--output",
        str(settings.output),
        "--sirius-extension",
        str(settings.sirius_extension),
    ]
    if settings.seed is not None:
        command.extend(["--seed", str(settings.seed)])
    if settings.baseline_json is not None:
        command.extend(["--baseline-json", str(settings.baseline_json)])
    subprocess.run(command, check=True)


def index_entry(
    settings: RunSettings, root: Path, included: bool = True
) -> JsonObject:
    """Records a run and its baseline association relative to the index."""
    return {
        "sample": settings.sample,
        "device": settings.device,
        "connections": settings.connections,
        "role": "baseline" if settings.baseline_json is None else "measurement",
        "included": included,
        "result_json": relative_path(settings.output, root),
        "baseline_json": relative_path(settings.baseline_json, root),
    }


def run_sample(
    settings: RunSettings, connections: list[int], root: Path
) -> list[JsonObject]:
    """Measures one reference and the requested concurrent workloads."""
    directory = root / settings.device / f"sample-{settings.sample:02d}"
    baseline = replace(
        settings,
        connections=1,
        baseline_json=None,
        output=directory / "baseline.json",
    )
    run_child(baseline)
    entries = [index_entry(baseline, root, included=1 in connections)]
    for count in connections:
        if count == 1:
            continue
        measured = replace(
            settings,
            connections=count,
            baseline_json=baseline.output,
            output=directory / f"n{count}.json",
        )
        run_child(measured)
        entries.append(index_entry(measured, root))
    return entries


def main() -> None:
    """Runs the sweep and publishes an index after every sample succeeds."""
    args = parse_sweep_arguments()
    # Sweep overrides are lists/counts, not single-run argument overrides.
    run = load_run_settings(argparse.Namespace(config=args.config))
    run = replace(run, output=None, baseline_json=None)
    sweep = load_sweep_settings(args, run)
    root = create_result_directory(sweep.output, "sweep")
    write_json(
        root / "settings.json",
        {
            "run": asdict(run),
            "sweep": asdict(sweep),
        },
    )
    entries: list[JsonObject] = []
    for sample in range(1, sweep.samples + 1):
        for device in sweep.devices:
            settings = replace(run, sample=sample, device=device)
            entries.extend(run_sample(settings, sweep.connections, root))
    write_json(
        root / "index.json",
        {
            "settings_json": "settings.json",
            "runs": entries,
        },
    )
    print(f"Sweep results: {root / 'index.json'}")


if __name__ == "__main__":
    main()
