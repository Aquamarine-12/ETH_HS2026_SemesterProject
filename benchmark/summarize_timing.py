"""Summarizes repeated timing profiles without requiring DuckDB to be installed.

Normalize DuckDB's legacy and 2.0 phase names while retaining missing values.
Report medians and ranges for each query, backend, and connection count.
Save full phase columns to CSV and print ordinary-run throughput separately.
Reported frontend time is diagnostic, especially for Sirius extension work.
"""

import argparse
from collections import defaultdict
import csv
from dataclasses import dataclass
import json
from pathlib import Path
from statistics import median
from typing import Any, TypeAlias

PhaseTimes: TypeAlias = dict[str, float | None]
SummaryRow: TypeAlias = dict[str, str | int | float | None]
GroupKey: TypeAlias = tuple[str, int, int]
PHASE_NAMES = {
    "parser": None,
    "planner": "planner",
    "optimizer": "all_optimizers",
    "physical_planner": "physical_planner",
}


@dataclass
class Sample:
    """Contains one completed query's client timings and frontend phases."""

    wall_s: float
    execute_s: float
    fetch_s: float
    phases: PhaseTimes

    def reported_frontend(self) -> float | None:
        """Sums available parent phases without double-counting subphases."""
        values = [value for value in self.phases.values() if value is not None]
        return sum(values) if values else None


def phase_times(profile: dict[str, Any]) -> PhaseTimes:
    """Normalizes metric names; an absent phase remains unavailable."""
    result: PhaseTimes = {}
    for name, legacy_name in PHASE_NAMES.items():
        group = profile.get(name)
        if isinstance(group, dict):
            result[name] = group.get("total_time")
        else:
            result[name] = profile.get(legacy_name) if legacy_name else None
    return result


def load_samples(directory: Path) -> dict[GroupKey, list[Sample]]:
    """Groups raw profile records by backend, N, and query number."""
    groups: dict[GroupKey, list[Sample]] = defaultdict(list)
    for path in sorted(directory.glob("*.profiles.json")):
        document = json.loads(path.read_text(encoding="utf-8"))
        for row in document["records"]:
            key = document["device"], document["connections"], row["query_id"]
            groups[key].append(
                Sample(row["wall_s"], row["execute_s"], row["fetch_s"],
                       phase_times(row["profile"]))
            )
    return groups


def load_plain_samples(directory: Path) -> dict[GroupKey, list[float]]:
    """Loads complete query latencies from the matching ordinary-run files."""
    groups: dict[GroupKey, list[float]] = defaultdict(list)
    for path in sorted(directory.glob("*-plain.json")):
        result = json.loads(path.read_text(encoding="utf-8"))
        for stream in result["streams"]:
            for query_id, latency in stream["query_latencies_s"].items():
                key = result["device"], result["connections"], int(query_id)
                groups[key].append(latency)
    return groups


def median_available(values: list[float | None]) -> float | None:
    """Returns a median only when every sample supplies the measurement."""
    if any(value is None for value in values):
        return None
    return median(value for value in values if value is not None)


def summarize_query(
    key: GroupKey, samples: list[Sample], plain: list[float]
) -> SummaryRow:
    """Builds medians and observed ranges for one query group."""
    wall = [sample.wall_s for sample in samples]
    frontend = [sample.reported_frontend() for sample in samples]
    shares = [value / sample.wall_s if value is not None else None
              for sample, value in zip(samples, frontend)]
    plain_median = median(plain) if plain else None
    row: SummaryRow = {
        "device": key[0], "connections": key[1], "query_id": key[2],
        "samples": len(samples), "wall_s_median": median(wall),
        "wall_s_min": min(wall), "wall_s_max": max(wall),
        "execute_s_median": median(sample.execute_s for sample in samples),
        "fetch_s_median": median(sample.fetch_s for sample in samples),
        "plain_samples": len(plain), "plain_wall_s_median": plain_median,
        "profile_to_plain_latency_ratio": (
            median(wall) / plain_median if plain_median is not None else None
        ),
        "reported_frontend_s_median": median_available(frontend),
        "reported_frontend_share_median": median_available(shares),
        "phase_fields_available_min": min(
            sum(value is not None for value in sample.phases.values())
            for sample in samples
        ),
    }
    for name in PHASE_NAMES:
        row[f"{name}_s_median"] = median_available(
            [sample.phases[name] for sample in samples]
        )
    return row


def number(value: str | int | float | None, factor: float = 1.0) -> str:
    """Formats numeric report cells while retaining unavailable measurements."""
    return "n/a" if value is None else f"{float(value) * factor:.2f}"


def print_queries(rows: list[SummaryRow]) -> None:
    """Prints compact per-query tables; the CSV retains all phase columns."""
    previous = None
    for row in rows:
        group = row["device"], row["connections"]
        if group != previous:
            print(f"\n{str(group[0]).upper()}, N={group[1]} (times in ms)")
            print(" Q Samples    Wall   Plain    P/U  Execute   Fetch  "
                  "Frontend  Share% Fields")
            previous = group
        print(
            f"{row['query_id']:2} {row['samples']:8} "
            f"{number(row['wall_s_median'], 1000):>7} "
            f"{number(row['plain_wall_s_median'], 1000):>7} "
            f"{number(row['profile_to_plain_latency_ratio']):>6} "
            f"{number(row['execute_s_median'], 1000):>8} "
            f"{number(row['fetch_s_median'], 1000):>7} "
            f"{number(row['reported_frontend_s_median'], 1000):>9} "
            f"{number(row['reported_frontend_share_median'], 100):>7} "
            f"{row['phase_fields_available_min']}/4"
        )


def print_plain_runs(directory: Path) -> None:
    """Prints ordinary-run QPS medians and ranges independently of profiling."""
    groups: dict[tuple[str, int], list[float]] = defaultdict(list)
    for path in sorted(directory.glob("*-plain.json")):
        result = json.loads(path.read_text(encoding="utf-8"))
        groups[result["device"], result["connections"]].append(result["qps"])
    print("\nUnprofiled QPS: median [min, max]")
    for (device, connections), values in sorted(groups.items()):
        print(f"{device}, N={connections}: {median(values):.3f} "
              f"[{min(values):.3f}, {max(values):.3f}]")


def main() -> None:
    """Writes a CSV report for a directory containing diagnostic profiles."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory", type=Path)
    args = parser.parse_args()
    plain = load_plain_samples(args.directory)
    rows = [summarize_query(key, samples, plain.get(key, []))
            for key, samples in sorted(load_samples(args.directory).items())]
    output = args.directory / "timing-summary.csv"
    with output.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print_queries(rows)
    print_plain_runs(args.directory)
    print("\nFrontend = recorded DuckDB phases, not all Sirius planning.")
    print("P/U = profiled/plain latency ratio, including pacing changes.")
    print("Fetch includes conversion and possibly unfinished execution.")
    print("Profile retrieval changes stream pacing; use plain runs for QPS.")
    print(f"Full phase medians and latency ranges (seconds): {output}")


if __name__ == "__main__":
    main()
