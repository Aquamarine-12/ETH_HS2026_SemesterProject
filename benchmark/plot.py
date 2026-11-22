"""Plots raw benchmark JSON with sample-level median confidence intervals.

Load a sweep index or a single result, retaining each reference association.
Aggregate streams within each run before resampling whole experiment samples.
Use the same bootstrap sample indices across a series' parameter combinations.
Figures are written as PNG and PDF without needing DuckDB or a GPU.
"""

from __future__ import annotations

import argparse
import tomllib
from collections import defaultdict
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import TypeAlias

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.axes import Axes
from numpy.typing import NDArray

from json_io import JsonObject, load_baseline, read_json

plt.switch_backend("Agg")

FloatArray: TypeAlias = NDArray[np.float64]
METRICS = (
    "total_qps", "per_query_latency", "total_slowdown", "per_query_slowdown",
)
GROUP_FIELDS = ("device", "cache", "query_order")
PER_QUERY = ("per_query_latency", "per_query_slowdown")
SLOWDOWN = ("total_slowdown", "per_query_slowdown")
Y_LABELS = {
    "total_qps": "Total QPS (queries/s)",
    "per_query_latency": "Per-query latency (s)",
    "total_slowdown": "Total slowdown factor",
    "per_query_slowdown": "Per-query slowdown factor",
}


@dataclass
class PlotRun:
    """Associates a raw result with its location and optional baseline."""

    path: Path
    data: JsonObject
    baseline: dict[int, float] | None = None
    included: bool = True


@dataclass
class PlotSettings:
    """Selects measurements, metrics, and dimensions for separate series."""

    metrics: list[str] = field(default_factory=lambda: list(METRICS))
    group_by: list[str] = field(default_factory=lambda: list(GROUP_FIELDS))
    devices: list[str] | None = None
    cache_modes: list[str] | None = None
    seeds: list[str | int] | None = None
    queries: list[int] | None = None
    connections: list[int] | None = None


def parse_order(value: str | int) -> str | int:
    """Normalizes query-order filters without importing the execution core."""
    try:
        return int(value)
    except ValueError:
        return value.upper()


def load_plot_settings(args: argparse.Namespace) -> PlotSettings:
    """Loads optional TOML selections, then applies explicit CLI overrides."""
    values = {}
    if args.config is not None:
        document = tomllib.loads(
            args.config.expanduser().read_text(encoding="utf-8")
        )
        values.update(document.get("plot", {}))
    for name in (item.name for item in fields(PlotSettings)):
        if hasattr(args, name):
            values[name] = getattr(args, name)
    settings = PlotSettings(**values)
    if settings.seeds is not None:
        settings.seeds = [parse_order(value) for value in settings.seeds]
    for name, allowed in (("metrics", METRICS), ("group_by", GROUP_FIELDS)):
        unknown = set(getattr(settings, name)) - set(allowed)
        if unknown:
            raise ValueError(f"Unknown plot {name}: {sorted(unknown)}")
    return settings


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
    if metric == "total_qps":
        return [
            run
            for run in runs
            if run.included
            or (
                run.data["role"] == "baseline"
                and run.data["connections"] == 1
            )
        ]
    if metric in SLOWDOWN:
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
        "duckdb_version",
        "cpu_fallback",
        "seed",
    ):
        if run.data[key] != baseline_data[key]:
            raise ValueError(f"Baseline {key} differs for {run.path}")
    if run.data.get("io_backend") != baseline_data.get("io_backend"):
        raise ValueError(f"Baseline io_backend differs for {run.path}")
    if run.data.get("warmup_streams", 0) != baseline_data.get(
        "warmup_streams", 0
    ):
        raise ValueError(f"Baseline warmup stream count differs for {run.path}")
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


def dimension_value(run: PlotRun, name: str) -> str:
    """Reads one comparison dimension from recorded result metadata."""
    return {
        "device": run.data["device"],
        "cache": "hot" if run.data.get("warmup_streams", 0) > 0 else "cold",
        "query_order": str(run.data["seed"]),
    }[name]


def filter_dimensions(
    runs: list[PlotRun], settings: PlotSettings
) -> list[PlotRun]:
    """Selects devices, cache modes, and orders before metric selection."""
    for name, requested in (
        ("device", settings.devices), ("cache", settings.cache_modes),
        ("query_order", settings.seeds),
    ):
        if requested is None:
            continue
        wanted = {str(value) for value in requested}
        available = {dimension_value(run, name) for run in runs}
        missing = wanted - available
        if missing:
            raise ValueError(f"Unavailable {name} values: {sorted(missing)}")
        runs = [run for run in runs if dimension_value(run, name) in wanted]
    if not runs:
        raise ValueError("No measurements match the plot filters")
    return runs


def validate_grouping(runs: list[PlotRun], settings: PlotSettings) -> None:
    """Requires varying conditions to be explicitly represented as series."""
    for name in GROUP_FIELDS:
        values = {dimension_value(run, name) for run in runs}
        if name not in settings.group_by and len(values) > 1:
            raise ValueError(
                f"Multiple {name} values selected: {sorted(values)}; "
                f"add {name} to group_by or filter to one value"
            )


def filter_connections(
    runs: list[PlotRun], settings: PlotSettings, metric: str
) -> list[PlotRun]:
    """Filters every metric's connection counts while retaining references."""
    if settings.connections is None:
        return runs
    wanted = set(settings.connections)
    if metric in SLOWDOWN:
        wanted = {count for count in wanted if count >= 2}
    groups: dict[str, set[int]] = defaultdict(set)
    for run in runs:
        groups[group_label(run, settings)].add(run.data["connections"])
    for label, counts in groups.items():
        missing = wanted - counts
        if missing:
            raise ValueError(
                f"{metric}: unavailable connections {sorted(missing)} "
                f"for {label}"
            )
    return [run for run in runs if run.data["connections"] in wanted]


def validate_queries(runs: list[PlotRun], queries: list[int]) -> None:
    """Requires every selected query in every measured stream."""
    for run in runs:
        for stream in run.data["streams"]:
            available = {int(q) for q in stream["query_latencies_s"]}
            missing = set(queries) - available
            if missing:
                raise ValueError(
                    f"Unavailable queries {sorted(missing)} in {run.path}, "
                    f"stream {stream['stream_id']}"
                )


def prepare_runs(
    runs: list[PlotRun], metric: str, settings: PlotSettings
) -> list[PlotRun]:
    """Selects and checks all data needed by a figure before rendering it."""
    selected = select_runs(runs, metric)
    if metric == "per_query_latency" and settings.connections is not None:
        # Explicitly selecting N=1 may use its otherwise unplotted reference.
        selected = [
            run for run in runs
            if run.included or run.data["role"] == "baseline"
        ]
    selected = filter_connections(selected, settings, metric)
    if not selected:
        raise ValueError(f"No runs available for {metric}")
    validate_grouping(selected, settings)
    if metric in PER_QUERY and settings.queries is not None:
        validate_queries(selected, settings.queries)
    if metric in SLOWDOWN:
        for run in selected:
            if run.baseline is None:
                attach_baseline(run)
    return selected


def query_values(
    run: PlotRun, slowdown: bool, queries: list[int] | None = None
) -> dict[int, float]:
    """Summarizes streams within a run before treating it as one sample."""
    grouped: dict[int, list[float]] = defaultdict(list)
    for stream in run.data["streams"]:
        for query, latency in stream["query_latencies_s"].items():
            query_id = int(query)
            if queries is not None and query_id not in queries:
                continue
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


def group_label(
    run: PlotRun, settings: PlotSettings, per_query: bool = False
) -> str:
    """Builds a series label from selected grouping dimensions."""
    data = run.data
    parts = []
    for name in settings.group_by:
        value = dimension_value(run, name)
        if name == "device":
            value = value.upper()
        elif name == "cache":
            value += " cache"
        parts.append(value)
    comparison = ", ".join(parts) or "measurements"
    label = f"{Path(data['database']).stem}: {comparison}"
    if data.get("io_backend") is not None:
        label += f" ({data['io_backend']})"
    if per_query:
        label += f", N={data['connections']}"
    return label


def make_series(
    runs: list[PlotRun], metric: str, settings: PlotSettings
) -> dict[str, tuple[list[int], FloatArray]]:
    """Builds sample-by-parameter matrices for paired resampling."""
    per_query = metric in PER_QUERY
    groups: dict[str, dict[int, dict[int, float]]] = defaultdict(dict)
    for run in runs:
        label = group_label(run, settings, per_query)
        sample = run.data["sample"]
        row = groups[label].setdefault(sample, {})
        if per_query:
            if row:
                raise ValueError(f"Duplicate sample {sample}: {label}")
            row.update(query_values(
                run, metric == "per_query_slowdown", settings.queries
            ))
        else:
            count = run.data["connections"]
            if count in row:
                raise ValueError(
                    f"Duplicate sample {sample}, N={count}: {label}"
                )
            key = "qps" if metric == "total_qps" else "mean_slowdown"
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


def draw_query_panel(
    axes: Axes,
    runs: list[PlotRun],
    metric: str,
    settings: PlotSettings,
) -> None:
    """Places condition/connection series side by side within each query."""
    series = make_series(runs, metric, settings)
    axes.set_prop_cycle(color=plt.get_cmap("tab20", len(series)).colors)
    query_ids = sorted({
        query for columns, _ in series.values() for query in columns
    })
    centers = {query: index for index, query in enumerate(query_ids)}
    spacing = 0.8 / len(series)
    for index, (label, (columns, values)) in enumerate(series.items()):
        offset = (index - (len(series) - 1) / 2) * spacing
        positions = np.array([centers[query] + offset for query in columns])
        draw_series(axes, label, positions, values, connected=False)
    axes.set_xticks(range(len(query_ids)), [f"Q{query}" for query in query_ids])
    axes.set_xlim(-0.6, len(query_ids) - 0.4)
    axes.set_ylabel(Y_LABELS[metric])
    axes.grid(axis="y", alpha=0.2)
    axes.legend(
        ncols=min(3, len(series)), fontsize=8,
        loc="upper center", bbox_to_anchor=(0.5, -0.16),
    )
    if metric == "per_query_slowdown":
        axes.axhline(1, color="grey", linestyle="--", linewidth=1)


def query_figure(
    runs: list[PlotRun], metric: str, settings: PlotSettings
) -> plt.Figure:
    """Compares all selected series on the same per-query axes."""
    figure, axes = plt.subplots(figsize=(16, 5), layout="constrained")
    draw_query_panel(axes, runs, metric, settings)
    axes.set_xlabel("TPC-H query")
    return figure


def connection_figure(
    runs: list[PlotRun], metric: str, settings: PlotSettings
) -> plt.Figure:
    """Plots throughput or slowdown against connection count."""
    figure, axes = plt.subplots(figsize=(9, 5), layout="constrained")
    series = make_series(runs, metric, settings)
    axes.set_prop_cycle(color=plt.get_cmap("tab20", len(series)).colors)
    for label, (columns, values) in series.items():
        draw_series(axes, label, np.array(columns, dtype=float), values)
    axes.set_xlabel("Connections")
    axes.set_ylabel(Y_LABELS[metric])
    if metric == "total_slowdown":
        axes.axhline(1, color="grey", linestyle="--", linewidth=1)
    axes.set_xticks(sorted({run.data["connections"] for run in runs}))
    axes.grid(alpha=0.2)
    axes.legend()
    return figure


def add_figure_title(
    figure: plt.Figure,
    runs: list[PlotRun],
    metric: str,
) -> None:
    """Adds the cache mode and measurement hardware above the figure."""
    modes = {run.data.get("warmup_streams", 0) > 0 for run in runs}
    if len(modes) == 1:
        cache = "Hot cache" if True in modes else "Cold cache"
    else:
        cache = "Hot and cold cache"
    devices = "/".join(sorted({run.data["device"].upper() for run in runs}))
    orders = ", ".join(sorted({str(run.data["seed"]) for run in runs}))
    hardware = []
    for label, key in (("CPU", "cpu_model"), ("GPU", "gpu_model")):
        models = sorted({run.data[key] for run in runs if run.data.get(key)})
        hardware.append(f"{label}: {', '.join(models) or 'not recorded'}")
    figure.suptitle(
        f"{Y_LABELS[metric]} — {devices} — {cache} — order {orders}\n"
        f"{' | '.join(hardware)}\n"
        "Median across samples; bars: 95% bootstrap CI",
        fontsize=12,
    )


def render_metric(
    runs: list[PlotRun],
    metric: str,
    output: Path,
    settings: PlotSettings | None = None,
) -> None:
    """Renders one metric into shareable PNG and PDF files."""
    settings = settings if settings is not None else PlotSettings()
    runs = prepare_runs(runs, metric, settings)
    per_query = metric in PER_QUERY
    figure = (
        query_figure(runs, metric, settings) if per_query
        else connection_figure(runs, metric, settings)
    )
    add_figure_title(figure, runs, metric)
    output.mkdir(parents=True, exist_ok=True)
    for suffix in ("png", "pdf"):
        figure.savefig(output / f"{metric}.{suffix}", dpi=180)
    plt.close(figure)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    """Parses a result/index path and the figures to produce."""
    parser = argparse.ArgumentParser(description="Plot TPC-H benchmark results")
    parser.add_argument("source", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--config", type=Path, help="TOML with a [plot] table")
    parser.add_argument(
        "--queries", nargs="+", type=int, default=argparse.SUPPRESS,
        help="Query IDs to show in per-query figures only",
    )
    parser.add_argument(
        "--connections", "--query-connections", dest="connections",
        nargs="+", type=int,
        default=argparse.SUPPRESS,
        help="Connection counts to show in every metric",
    )
    parser.add_argument(
        "--metrics", nargs="+", choices=METRICS, default=argparse.SUPPRESS
    )
    parser.add_argument(
        "--group-by", nargs="*", choices=GROUP_FIELDS,
        default=argparse.SUPPRESS,
        help="Dimensions producing separate series (default: all three)",
    )
    parser.add_argument(
        "--devices", nargs="+", choices=("cpu", "gpu"),
        default=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--cache-modes", nargs="+", choices=("cold", "hot"),
        default=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--seeds", nargs="+", type=parse_order, default=argparse.SUPPRESS,
        help="Query orders: ASC, DESC, ASC-DESC, or integer shuffle seeds",
    )
    return parser.parse_args(argv)


def main() -> None:
    """Plots requested measurements without silently dropping missing data."""
    args = parse_arguments()
    settings = load_plot_settings(args)
    source = args.source.expanduser().resolve()
    runs = filter_dimensions(load_runs(source), settings)
    output = args.output or source.parent / "plots"
    # Reject missing selections or references before writing partial figures.
    for metric in settings.metrics:
        selected = prepare_runs(runs, metric, settings)
        make_series(selected, metric, settings)
    for metric in settings.metrics:
        render_metric(runs, metric, output, settings)
    print(f"Figures: {output}")


if __name__ == "__main__":
    main()
