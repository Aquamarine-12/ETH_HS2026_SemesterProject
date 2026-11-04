"""Plots raw benchmark JSON with sample-level median confidence intervals.

Load a sweep index or a single result, retaining each reference association.
Aggregate streams within each run before resampling whole experiment samples.
Use the same bootstrap sample indices across a series' parameter combinations.
Figures are written as PNG and PDF without needing DuckDB or a GPU.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TypeAlias

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
from numpy.typing import NDArray

from json_io import JsonObject, load_baseline, read_json

plt.switch_backend("Agg")

FloatArray: TypeAlias = NDArray[np.float64]
METRICS = ("qps", "slowdown", "latency", "query-slowdown")
Y_LABELS = {
    "qps": "Queries per second",
    "slowdown": "Mean slowdown factor",
    "latency": "Query latency (s)",
    "query-slowdown": "Mean query slowdown factor",
}


@dataclass
class PlotRun:
    """Associates a raw result with its location and optional baseline."""

    path: Path
    data: JsonObject
    baseline: dict[int, float] | None = None
    included: bool = True


def load_runs(source: Path) -> list[PlotRun]:
    """Reads all requested runs and fails on any missing result file."""
    document = read_json(source)
    if "runs" not in document:
        return [PlotRun(source, document)]
    runs = []
    for entry in document["runs"]:
        # References exist even when N=1 was not a requested plot point.
        path = source.parent / entry["result_json"]
        data = read_json(path)
        runs.append(PlotRun(path, data, included=entry["included"]))
    return runs


def select_runs(runs: list[PlotRun], metric: str) -> list[PlotRun]:
    """Includes N=1 references for QPS and N>=2 comparisons for slowdown."""
    if metric == "qps":
        return [
            run
            for run in runs
            if run.included
            or (
                run.data["role"] == "baseline"
                and run.data["connections"] == 1
            )
        ]
    if metric in ("slowdown", "query-slowdown"):
        return [
            run
            for run in runs
            if run.included
            and run.data["connections"] >= 2
            and run.data["role"] != "baseline"
        ]
    return [run for run in runs if run.included]


def attach_baseline(run: PlotRun) -> None:
    """Requires a real matching reference for every slowdown measurement."""
    reference = run.data["baseline_json"]
    if reference is None:
        raise ValueError(f"Slowdown requires baseline_json in {run.path}")
    path = run.path.parent / reference
    baseline_data = read_json(path)
    for key in (
        "database",
        "device",
        "sample",
        "duckdb_version",
        "cpu_fallback",
        "seed",
    ):
        if run.data[key] != baseline_data[key]:
            raise ValueError(f"Baseline {key} differs for {run.path}")
    run.baseline = load_baseline(path)
    recorded = run.data["baseline_latencies_s"]
    if (
        recorded is None
        or {int(key): value for key, value in recorded.items()} != run.baseline
    ):
        raise ValueError(f"Baseline latencies differ for {run.path}")
    # Indexing every query also catches incomplete references immediately.
    for stream in run.data["streams"]:
        for query in stream["query_order"]:
            run.baseline[query]


def query_values(run: PlotRun, slowdown: bool) -> dict[int, float]:
    """Summarizes streams within a run before treating it as one sample."""
    grouped: dict[int, list[float]] = defaultdict(list)
    for stream in run.data["streams"]:
        for query, latency in stream["query_latencies_s"].items():
            query_id = int(query)
            value = latency
            if slowdown:
                if run.baseline is None:
                    raise ValueError(f"Missing baseline for {run.path}")
                value = latency / run.baseline[query_id]
            grouped[query_id].append(value)
    return {query: float(np.mean(values)) for query, values in grouped.items()}


def bootstrap_median(
    values: FloatArray,
) -> tuple[FloatArray, FloatArray | None, FloatArray | None]:
    """Returns medians and paired percentile-bootstrap 95% confidence bounds."""
    medians = np.median(values, axis=0)
    if len(values) == 1:
        return medians, None, None
    # Each row is an entire sample block, so parameter columns stay paired.
    rng = np.random.default_rng(0)
    indices = rng.integers(0, len(values), size=(10000, len(values)))
    distribution = np.median(values[indices], axis=1)
    low, high = np.percentile(distribution, [2.5, 97.5], axis=0)
    return medians, low, high


def group_label(run: PlotRun, per_query: bool) -> str:
    """Labels backend series and optionally their connection count."""
    data = run.data
    label = f"{Path(data['database']).stem}: {data['device']}"
    if per_query:
        label += f", N={data['connections']}"
    return label


def make_series(
    runs: list[PlotRun], metric: str
) -> dict[str, tuple[list[int], FloatArray]]:
    """Builds sample-by-parameter matrices for paired resampling."""
    per_query = metric in ("latency", "query-slowdown")
    groups: dict[str, dict[int, dict[int, float]]] = defaultdict(dict)
    for run in runs:
        label = group_label(run, per_query)
        sample = run.data["sample"]
        row = groups[label].setdefault(sample, {})
        if per_query:
            if row:
                raise ValueError(f"Duplicate sample {sample}: {label}")
            row.update(query_values(run, metric == "query-slowdown"))
        else:
            count = run.data["connections"]
            if count in row:
                raise ValueError(
                    f"Duplicate sample {sample}, N={count}: {label}"
                )
            key = "qps" if metric == "qps" else "mean_slowdown"
            row[count] = run.data[key]
    series = {}
    for label, samples in groups.items():
        columns = sorted(next(iter(samples.values())))
        if any(sorted(row) != columns for row in samples.values()):
            raise ValueError(f"Incomplete sample block: {label}")
        values = np.array(
            [
                [samples[sample][column] for column in columns]
                for sample in sorted(samples)
            ],
            dtype=float,
        )
        series[label] = columns, values
    return series


def draw_series(
    axes: Axes,
    label: str,
    positions: FloatArray,
    values: FloatArray,
    connected: bool = True,
) -> None:
    """Draws sample medians and confidence bars, optionally joining points."""
    medians, low, high = bootstrap_median(values)
    (line,) = axes.plot(
        positions, medians, marker="o", markersize=4,
        linestyle="-" if connected else "none", label=label,
    )
    if low is not None and high is not None:
        # Draw interval endpoints directly; do not clip or alter the interval.
        color = line.get_color()
        axes.vlines(positions, low, high, color=color)
        axes.plot(positions, low, "_", color=color)
        axes.plot(positions, high, "_", color=color)


def draw_query_panel(axes: Axes, runs: list[PlotRun], metric: str) -> None:
    """Places connection-count medians side by side within each query group."""
    counts = sorted({run.data["connections"] for run in runs})
    queries = sorted({
        query
        for run in runs
        for query in run.data["streams"][0]["query_order"]
    })
    centers = {query: index for index, query in enumerate(queries)}
    spacing = 0.8 / len(counts)
    for index, count in enumerate(counts):
        subset = [run for run in runs if run.data["connections"] == count]
        ((_, (columns, values)),) = make_series(subset, metric).items()
        offset = (index - (len(counts) - 1) / 2) * spacing
        positions = np.array([centers[query] + offset for query in columns])
        draw_series(axes, f"N={count}", positions, values, connected=False)
    axes.set_xticks(range(len(queries)), [f"Q{query}" for query in queries])
    axes.set_xlim(-0.6, len(queries) - 0.4)
    axes.set_ylabel(Y_LABELS[metric])
    axes.grid(axis="y", alpha=0.2)
    axes.legend(ncols=min(5, len(counts)), fontsize=9)
    if metric == "query-slowdown":
        axes.axhline(1, color="grey", linestyle="--", linewidth=1)


def query_figure(runs: list[PlotRun], metric: str) -> plt.Figure:
    """Separates backends into panels with grouped query medians and CIs."""
    groups: dict[str, list[PlotRun]] = defaultdict(list)
    for run in runs:
        groups[group_label(run, per_query=False)].append(run)
    figure, axes = plt.subplots(
        len(groups), 1, figsize=(16, 4 * len(groups)),
        sharex=True, squeeze=False, layout="constrained",
    )
    for (label, subset), panel in zip(groups.items(), axes[:, 0]):
        draw_query_panel(panel, subset, metric)
        panel.set_title(label)
    axes[-1, 0].set_xlabel("TPC-H query")
    figure.suptitle("Median across samples; bars: 95% bootstrap CI")
    return figure


def connection_figure(runs: list[PlotRun], metric: str) -> plt.Figure:
    """Plots throughput or slowdown against connection count."""
    figure, axes = plt.subplots(figsize=(9, 5), layout="constrained")
    for label, (columns, values) in make_series(runs, metric).items():
        draw_series(axes, label, np.array(columns, dtype=float), values)
    axes.set_xlabel("Connections")
    axes.set_ylabel(Y_LABELS[metric])
    axes.set_title("Median across samples; bars: 95% bootstrap CI")
    if metric == "slowdown":
        axes.axhline(1, color="grey", linestyle="--", linewidth=1)
    axes.set_xticks(sorted({run.data["connections"] for run in runs}))
    axes.grid(alpha=0.2)
    axes.legend()
    return figure


def render_metric(runs: list[PlotRun], metric: str, output: Path) -> None:
    """Renders one metric into shareable PNG and PDF files."""
    runs = select_runs(runs, metric)
    if metric in ("slowdown", "query-slowdown"):
        if not runs:
            raise ValueError("No comparison runs available for slowdown")
        for run in runs:
            attach_baseline(run)
    per_query = metric in ("latency", "query-slowdown")
    figure = (
        query_figure(runs, metric) if per_query
        else connection_figure(runs, metric)
    )
    output.mkdir(parents=True, exist_ok=True)
    for suffix in ("png", "pdf"):
        figure.savefig(output / f"{metric}.{suffix}", dpi=180)
    plt.close(figure)


def parse_arguments() -> argparse.Namespace:
    """Parses a result/index path and the figures to produce."""
    parser = argparse.ArgumentParser(description="Plot TPC-H benchmark results")
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--metrics", nargs="+", choices=METRICS, default=METRICS
    )
    return parser.parse_args()


def main() -> None:
    """Plots requested measurements without silently dropping missing data."""
    args = parse_arguments()
    source = args.source.expanduser().resolve()
    runs = load_runs(source)
    output = args.output or source.parent / "plots"
    # Check all references before creating a partial set of figures.
    if any(metric in ("slowdown", "query-slowdown") for metric in args.metrics):
        comparisons = select_runs(runs, "slowdown")
        if not comparisons:
            raise ValueError("No comparison runs available for slowdown")
        for run in comparisons:
            attach_baseline(run)
    for metric in args.metrics:
        render_metric(runs, metric, output)
    print(f"Figures: {output}")


if __name__ == "__main__":
    main()
