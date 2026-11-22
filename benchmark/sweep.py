"""Runs repeated parameter combinations in fresh, sequential processes.

Each sample/backend/cache/order combination gets one reference measurement.
Its concurrent results retain that reference; N=1 reuses the reference itself.
The completed index links raw results without replacing them with averages.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import asdict, dataclass, replace
from itertools import product
from pathlib import Path

from cli import (
    load_run_settings,
    load_sweep_settings,
    parse_sweep_arguments,
)
from core import RunSettings
from json_io import (
    JsonObject,
    create_result_directory,
    index_entry,
    write_json,
)


@dataclass
class SweepProgress:
    """Counts successfully completed benchmark invocations."""

    total: int
    completed: int = 0

    def report(self) -> None:
        """Prints completed benchmarks and their fraction of the full sweep."""
        print(
            f"Benchmarks completed: {self.completed}/{self.total} "
            f"({self.completed / self.total:.1%})",
            flush=True,
        )

    def run(self, settings: RunSettings) -> None:
        """Reports before launching a child and counts successful completion."""
        self.report()
        run_child(settings)
        self.completed += 1


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
        "--warmup-streams",
        str(settings.warmup_streams),
        "--sample",
        str(settings.sample),
        "--output",
        str(settings.output),
        "--sirius-extension",
        str(settings.sirius_extension),
        "--io-backend",
        settings.io_backend,
        "--seed",
        str(settings.seed),
    ]
    if settings.baseline_json is not None:
        command.extend(["--baseline-json", str(settings.baseline_json)])
    if settings.warmup_streams > 0:
        command.append("--warmup")
    subprocess.run(command, check=True)


def run_sample(
    settings: RunSettings, connections: list[int], root: Path,
    progress: SweepProgress,
) -> list[JsonObject]:
    """Measures one reference and the requested concurrent workloads."""
    directory = (
        root / settings.device / f"warmup-{settings.warmup_streams}"
        / f"seed-{settings.seed}"
        / f"sample-{settings.sample:02d}"
    )
    baseline = replace(
        settings,
        connections=1,
        baseline_json=None,
        output=directory / "baseline.json",
    )
    progress.run(baseline)
    entries = []
    if 1 not in connections:
        entries.append(index_entry(baseline, root, included=False))
    for count in connections:
        if count == 1:
            entries.append(index_entry(baseline, root))
            continue
        measured = replace(
            settings,
            connections=count,
            baseline_json=baseline.output,
            output=directory / f"n{count}.json",
        )
        progress.run(measured)
        entries.append(index_entry(measured, root))
    return entries


def main() -> None:
    """Runs the sweep and publishes an index after every sample succeeds."""
    args = parse_sweep_arguments()
    # Exclude sweep-only overrides when loading the shared run settings.
    run_values = {
        name: value for name, value in vars(args).items()
        if name not in ("connections", "samples", "output", "warmup_streams")
    }
    run = load_run_settings(argparse.Namespace(**run_values))
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
    progress = SweepProgress(
        sweep.samples * len(sweep.devices) * len(sweep.warmup_streams)
        * len(sweep.seeds) * (1 + sum(n != 1 for n in sweep.connections))
    )
    for sample, device, warmup_streams, seed in product(
        range(1, sweep.samples + 1), sweep.devices,
        sweep.warmup_streams, sweep.seeds,
    ):
        settings = replace(
            run, sample=sample, device=device,
            warmup_streams=warmup_streams, seed=seed,
        )
        entries.extend(run_sample(settings, sweep.connections, root, progress))
    write_json(
        root / "index.json",
        {
            "settings_json": "settings.json",
            "runs": entries,
        },
    )
    progress.report()
    print(f"Sweep results: {root / 'index.json'}")


if __name__ == "__main__":
    main()
